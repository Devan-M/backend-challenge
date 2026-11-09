import { createHash, randomUUID } from 'node:crypto';
import {
  BadRequestException,
  ConflictException,
  Inject,
  InternalServerErrorException,
  Injectable,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { MikroORM, EntityManager } from '@mikro-orm/core';
import { Money } from '../domain/money.js';
import {
  EventContext,
  IntegrationEvent,
  WalletBalanceChanged,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from '../domain/integration-event.js';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../domain/wager-transaction.js';
import { LedgerDirection } from '../domain/wallet-ledger-entry.js';
import { Wallet } from '../domain/wallet.js';
import { MetricsService } from './metrics.service.js';

/** Validated domain input shared by HTTP and SQS callers. */
interface SubmitInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
}

/** Minimal wallet columns needed to rehydrate the aggregate under a database lock. */
interface WalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance: string;
  version: number;
}

/** Persisted transaction columns used for idempotent replay and reference checks. */
interface TransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  payload_hash: string;
  status: WagerTransactionStatus;
  response_balance: string | null;
  failure_code: string | null;
  wallet_id: string;
  player_id: string;
  round_id: string;
  currency: string;
  amount: string;
  kind: WagerTransactionKind;
  reference_transaction_id: string | null;
  pending_attempts: number;
}

/** Stable result saved and replayed for a given idempotency key. */
interface SubmitResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: { amount: string; currency: string };
  idempotentReplay: boolean;
  failureCode?: string;
}

/** Durable SQS identity passed into the same use case as HTTP submissions. */
export interface InboxContext {
  consumerName: string;
  messageId: string;
  payloadHash: string;
}

/** Signals a non-retryable database failure that was persisted as FAILED. */
export class PermanentInfrastructureError extends InternalServerErrorException {
  constructor(transactionId: string) {
    super({ code: 'PERMANENT_INFRASTRUCTURE', retryable: false, transactionId });
    this.name = 'PermanentInfrastructureError';
  }
}

/** Excludes OPENING, which is created only by the wallet-opening transaction. */
const transactionKinds: Set<string> = new Set(Object.values(WagerTransactionKind).filter((kind) => kind !== WagerTransactionKind.Opening));

/** Coordinates domain validation and atomic PostgreSQL writes for all wagering entry points. */
@Injectable()
export class WageringService {
  private readonly logger = new Logger(WageringService.name);

  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    private readonly metrics: MetricsService,
  ) {}

  /** Opens wallet, optional OPENING transaction, ledger, and event in one SQL transaction. */
  async createWallet(body: unknown): Promise<Record<string, unknown>> {
    if (!isRecord(body) || typeof body.playerId !== 'string' || !isRecord(body.initialBalance)) {
      throw new BadRequestException({ code: 'INVALID_PAYLOAD', message: 'playerId and initialBalance are required' });
    }
    const playerId = body.playerId;
    if (!isUuid(playerId)) throw new BadRequestException({ code: 'INVALID_PAYLOAD', message: 'playerId must be a UUID' });
    let initialBalance: Money;
    try {
      initialBalance = Money.from({ amount: readString(body.initialBalance, 'amount'), currency: readString(body.initialBalance, 'currency') });
    } catch (error) {
      throw new BadRequestException({ code: 'INVALID_PAYLOAD', message: errorMessage(error) });
    }

    const now = new Date();
    const wallet = Wallet.open({ id: randomUUID(), playerId, initialBalance, now });
    try {
      // Keep wallet creation and its opening audit record indivisible.
      await this.orm.em.transactional(async (em) => {
        await this.execute(em,
          'insert into wallets (id, player_id, currency, balance, version, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?)',
          [wallet.id, wallet.playerId, wallet.currency, wallet.balance.toString(), wallet.version, now, now],
        );
        if (initialBalance.isPositive()) {
          const transactionId = randomUUID();
          const ledgerId = randomUUID();
          const openingKey = `opening:${wallet.id}`;
          const payloadHash = hashCanonicalPayload({ walletId: wallet.id, amount: initialBalance.toString(), currency: initialBalance.currency });
          await this.execute(em,
            `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
              wallet_id, player_id, round_id, game_id, kind, amount, currency, status, created_at, processed_at, response_balance)
             values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPENING', ?, ?, 'PROCESSED', ?, ?, ?)`,
            [transactionId, '__internal__', openingKey, openingKey, payloadHash, wallet.id, playerId, '__internal__', '__internal__',
              initialBalance.toString(), initialBalance.currency, now, now, initialBalance.toString()],
          );
          await this.execute(em,
            `insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
             values (?, ?, ?, 'CREDIT', ?, '0.00', ?, ?)`,
            [ledgerId, wallet.id, transactionId, initialBalance.toString(), initialBalance.toString(), now],
          );
          await this.enqueue(em, WalletBalanceChanged.from({
            aggregateId: wallet.id,
            correlationId: transactionId,
            causationId: transactionId,
            occurredAt: now,
            data: {
              walletId: wallet.id, transactionId, direction: LedgerDirection.Credit,
              money: initialBalance.toJSON(), balanceBefore: Money.zero(wallet.currency).toJSON(),
              balanceAfter: initialBalance.toJSON(), walletVersion: wallet.version,
            },
          }));
        }
      });
    } catch (error) {
      if (isUniqueViolation(error) && await this.walletExists(playerId, initialBalance.currency)) {
        throw new ConflictException({ code: 'WALLET_ALREADY_EXISTS', message: 'A wallet already exists for this player and currency' });
      }
      if (isDatabaseUnavailable(error)) {
        throw new ServiceUnavailableException({ code: 'TRANSIENT_INFRASTRUCTURE', retryable: true });
      }
      throw error;
    }

    return { id: wallet.id, playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  }

  /** Applies a wager, or returns its persisted result when the idempotency key already exists. */
  async submit(idempotencyKey: string, body: unknown, inbox?: InboxContext): Promise<SubmitResult> {
    if (!idempotencyKey.trim()) throw new BadRequestException({ code: 'INVALID_PAYLOAD', message: 'Idempotency-Key is required' });
    const input = this.parseInput(body);
    // Hash only normalized business fields; headers and transport metadata are excluded.
    const payloadHash = hashCanonicalPayload({
      providerId: input.providerId,
      externalTransactionId: input.externalTransactionId,
      playerId: input.playerId,
      walletId: input.walletId,
      roundId: input.roundId,
      gameId: input.gameId,
      kind: input.kind,
      money: input.money.toJSON(),
      ...(input.referenceExternalTransactionId ? { referenceExternalTransactionId: input.referenceExternalTransactionId } : {}),
    });

    try {
      // Inbox, wallet lock, balance, ledger, status, and outbox all share this transaction.
      const result = await this.orm.em.transactional(async (em) => {
        if (inbox) {
          await this.execute(em,
            `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at)
             values (?, ?, ?, ?) on conflict (consumer_name, message_id) do nothing`,
            [inbox.consumerName, inbox.messageId, inbox.payloadHash, new Date()],
          );
          const inboxRow = await this.one<{ payload_hash: string; processed_at: Date | null }>(em,
            'select payload_hash, processed_at from inbox_messages where consumer_name = ? and message_id = ?',
            [inbox.consumerName, inbox.messageId]);
          if (!inboxRow || inboxRow.payload_hash !== inbox.payloadHash) {
            throw new ConflictException({ code: 'INBOX_MESSAGE_CONFLICT', message: 'Message ID was reused with a different payload' });
          }
          if (inboxRow.processed_at) {
            const existing = await this.one<TransactionRow>(em,
              'select * from wager_transactions where idempotency_key = ?', [idempotencyKey]);
            if (!existing) throw new ConflictException({ code: 'INBOX_STATE_INVALID', message: 'Processed inbox has no transaction' });
            return this.replay(existing, payloadHash);
          }
        }
        const existing = await this.one<TransactionRow>(em,
          'select * from wager_transactions where idempotency_key = ?', [idempotencyKey]);
        if (existing && existing.status !== WagerTransactionStatus.PendingReference) {
          if (inbox) await this.markInboxProcessed(em, inbox);
          return this.replay(existing, payloadHash);
        }
        if (existing && existing.payload_hash !== payloadHash) return this.replay(existing, payloadHash);

        // Wallet ID is the concurrency boundary; separate wallets remain independently writable.
        const walletRow = await this.one<WalletRow>(em,
          'select id, player_id, currency, balance::text, version from wallets where id = ? for update', [input.walletId]);
        if (!walletRow) throw new NotFoundException({ code: 'WALLET_NOT_FOUND', message: 'Wallet does not exist' });
        if (walletRow.player_id !== input.playerId) {
          throw new ConflictException({ code: 'WALLET_MISMATCH', message: 'Wallet does not belong to player' });
        }

        const now = new Date();
        const transactionId = existing?.id ?? randomUUID();
        if (!existing) {
          await this.execute(em,
            `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
              wallet_id, player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id, status, created_at)
             values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)`,
            [transactionId, input.providerId, input.externalTransactionId, idempotencyKey, payloadHash, input.walletId,
              input.playerId, input.roundId, input.gameId, input.kind, input.money.toString(), input.money.currency,
              input.referenceExternalTransactionId ?? null, now],
          );
        }

        let wallet = Wallet.rehydrate({
          id: walletRow.id, playerId: walletRow.player_id, currency: walletRow.currency,
          balance: Money.from({ amount: walletRow.balance, currency: walletRow.currency }),
          version: walletRow.version, createdAt: now, updatedAt: now,
        });
        if (wallet.currency !== input.money.currency) {
          return this.reject(em, transactionId, input, wallet.balance, 'CURRENCY_MISMATCH', now, inbox);
        }

        let reference: TransactionRow | undefined;
        if (input.referenceExternalTransactionId) {
          reference = await this.one<TransactionRow>(em,
            'select * from wager_transactions where provider_id = ? and external_transaction_id = ?',
            [input.providerId, input.referenceExternalTransactionId]);
          if (!reference || reference.status === WagerTransactionStatus.Pending
            || reference.status === WagerTransactionStatus.PendingReference) {
            const attempts = (existing?.pending_attempts ?? 0) + 1;
            if (attempts >= 12) {
              return this.reject(em, transactionId, input, wallet.balance, 'REFERENCE_NOT_FOUND', now, inbox);
            }
            const retryDelaySeconds = Math.min(60, 5 * (2 ** Math.min(attempts - 1, 4)));
            await this.execute(em,
              `update wager_transactions set status = 'PENDING_REFERENCE', pending_attempts = ?, response_balance = ?,
               next_reference_attempt_at = ? where id = ?`,
              [attempts, wallet.balance.toString(), new Date(now.getTime() + retryDelaySeconds * 1_000), transactionId],
            );
            if (!existing) {
              await this.enqueue(em, WagerTransactionPendingReference.from({
                aggregateId: wallet.id, correlationId: transactionId, causationId: transactionId, occurredAt: now,
                data: { transactionId, providerId: input.providerId, referenceExternalTransactionId: input.referenceExternalTransactionId },
              }));
            }
            if (inbox) await this.markInboxProcessed(em, inbox);
            return this.result(transactionId, WagerTransactionStatus.PendingReference, wallet.balance, false);
          }
          const referenceProblem = await this.validateReference(em, input, reference);
          if (referenceProblem) return this.reject(em, transactionId, input, wallet.balance, referenceProblem, now, inbox);
        }

        // A rollback reverses its reference's economic direction; LOSS has no direction.
        const direction = input.kind === WagerTransactionKind.Rollback
          ? (reference?.kind === WagerTransactionKind.Bet ? LedgerDirection.Credit : LedgerDirection.Debit)
          : input.kind === WagerTransactionKind.Bet ? LedgerDirection.Debit
            : input.kind === WagerTransactionKind.Loss ? undefined : LedgerDirection.Credit;

        let movement: ReturnType<Wallet['credit']> | undefined;
        if (direction === LedgerDirection.Debit) {
          if (input.money.isLessThan(wallet.balance) || input.money.equals(wallet.balance)) {
            movement = wallet.debit(input.money, now);
          } else {
            const code = input.kind === WagerTransactionKind.Bet ? 'INSUFFICIENT_FUNDS' : 'REVERSAL_WOULD_OVERDRAW';
            return this.reject(em, transactionId, input, wallet.balance, code, now, inbox);
          }
        } else if (direction === LedgerDirection.Credit) {
          movement = wallet.credit(input.money, now);
        }

        const balanceAfter = movement?.balanceAfter ?? wallet.balance;
        if (movement) {
          await this.execute(em,
            'update wallets set balance = ?, version = ?, updated_at = ? where id = ?',
            [balanceAfter.toString(), movement.version, now, wallet.id],
          );
          await this.execute(em,
            `insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
             values (?, ?, ?, ?, ?, ?, ?, ?)`,
            [randomUUID(), wallet.id, transactionId, direction, input.money.toString(),
              movement.balanceBefore.toString(), balanceAfter.toString(), now],
          );
        }

        // Financial state and integration events commit together; publishers run afterward.
        await this.execute(em,
          `update wager_transactions set status = 'PROCESSED', reference_transaction_id = ?, processed_at = ?, response_balance = ? where id = ?`,
          [reference?.id ?? null, now, balanceAfter.toString(), transactionId],
        );
        const eventContext: EventContext = { correlationId: transactionId, causationId: transactionId, occurredAt: now };
        await this.enqueue(em, WagerTransactionProcessed.from({
          aggregateId: wallet.id, ...eventContext,
          data: { transactionId, providerId: input.providerId, kind: input.kind, status: WagerTransactionStatus.Processed },
        }));
        if (movement && direction) {
          await this.enqueue(em, WalletBalanceChanged.from({
            aggregateId: wallet.id, ...eventContext,
            data: {
              walletId: wallet.id, transactionId, direction, money: input.money.toJSON(),
              balanceBefore: movement.balanceBefore.toJSON(), balanceAfter: balanceAfter.toJSON(),
              walletVersion: movement.version,
            },
          }));
        }
        if (inbox) await this.markInboxProcessed(em, inbox);
        return this.result(transactionId, WagerTransactionStatus.Processed, balanceAfter, false);
      });
      this.logger.log(JSON.stringify({
        event: 'wager_transaction_result', correlationId: result.transactionId, transactionId: result.transactionId,
        messageId: inbox?.messageId, walletId: input.walletId, providerId: input.providerId, status: result.status,
      }));
      return result;
    } catch (error) {
      if (isLockConflict(error)) this.metrics.lockConflicts.inc();
      let existing: TransactionRow | undefined;
      try {
        existing = await this.findByIdempotencyKey(idempotencyKey);
      } catch {
        if (isDatabaseUnavailable(error)) {
          throw new ServiceUnavailableException({ code: 'TRANSIENT_INFRASTRUCTURE', retryable: true });
        }
        throw error;
      }
      if (existing) return this.replay(existing, payloadHash);
      if (isUniqueViolation(error)) {
        throw new ConflictException({ code: 'TRANSACTION_CONFLICT', message: 'Provider transaction identifier already exists' });
      }
      if (isDatabaseUnavailable(error)) {
        throw new ServiceUnavailableException({ code: 'TRANSIENT_INFRASTRUCTURE', retryable: true });
      }
      if (isPermanentDatabaseFailure(error)) {
        const transactionId = await this.persistFailedTransaction(idempotencyKey, payloadHash, input);
        if (transactionId) {
          this.logger.error(JSON.stringify({
            event: 'wager_transaction_failed',
            correlationId: transactionId,
            messageId: inbox?.messageId,
            transactionId,
            walletId: input.walletId,
            providerId: input.providerId,
            failureCode: 'PERMANENT_INFRASTRUCTURE',
          }));
          throw new PermanentInfrastructureError(transactionId);
        }
      }
      throw error;
    }
  }

  /** Persists a permanent failure after the failed financial transaction has rolled back. */
  private async persistFailedTransaction(
    idempotencyKey: string,
    payloadHash: string,
    input: SubmitInput,
  ): Promise<string | undefined> {
    return this.orm.em.transactional(async (em) => {
      const existing = await this.one<TransactionRow>(em,
        'select * from wager_transactions where idempotency_key = ?', [idempotencyKey]);
      if (existing) return existing.id;

      const wallet = await this.one<WalletRow>(em,
        'select id, player_id, currency, balance::text, version from wallets where id = ? for update', [input.walletId]);
      if (!wallet || wallet.player_id !== input.playerId || wallet.currency !== input.money.currency) return undefined;

      const now = new Date();
      const failed = WagerTransaction.create({
        id: randomUUID(),
        providerId: input.providerId,
        externalTransactionId: input.externalTransactionId,
        idempotencyKey,
        payloadHash,
        walletId: input.walletId,
        playerId: input.playerId,
        roundId: input.roundId,
        gameId: input.gameId,
        kind: input.kind,
        money: input.money,
        ...(input.referenceExternalTransactionId
          ? { referenceExternalTransactionId: input.referenceExternalTransactionId }
          : {}),
        createdAt: now,
      });
      failed.fail('PERMANENT_INFRASTRUCTURE');
      await this.execute(em,
        `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
          wallet_id, player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id,
          status, failure_code, created_at, processed_at, response_balance)
         values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [failed.id, failed.providerId, failed.externalTransactionId, failed.idempotencyKey, failed.payloadHash,
          failed.walletId, failed.playerId, failed.roundId, failed.gameId, failed.kind, failed.money.toString(),
          failed.money.currency, failed.referenceExternalTransactionId ?? null, failed.status, failed.failureCode,
          now, now, wallet.balance],
      );
      return failed.id;
    });
  }

  /** Reads the materialized wallet balance without changing aggregate state. */
  async getWallet(walletId: string): Promise<Record<string, unknown>> {
    const row = await this.one<WalletRow>(this.orm.em,
      'select id, player_id, currency, balance::text, version from wallets where id = ?', [walletId]);
    if (!row) throw new NotFoundException({ code: 'WALLET_NOT_FOUND', message: 'Wallet does not exist' });
    return { id: row.id, playerId: row.player_id, balance: Money.from({ amount: row.balance, currency: row.currency }).toJSON(), version: row.version };
  }

  /** Claims due references with a short lease so multiple schedulers do not burn attempts together. */
  async processDuePendingReferences(): Promise<void> {
    interface PendingRow {
      idempotency_key: string;
      provider_id: string;
      external_transaction_id: string;
      player_id: string;
      wallet_id: string;
      round_id: string;
      game_id: string;
      kind: WagerTransactionKind;
      amount: string;
      currency: string;
      reference_external_transaction_id: string | null;
    }
    // SKIP LOCKED distributes due rows across service instances before the business use case runs.
    const pending = await this.orm.em.transactional(async (em) => this.execute<PendingRow>(em,
      `with due as (
         select id from wager_transactions where status = 'PENDING_REFERENCE'
           and (next_reference_attempt_at is null or next_reference_attempt_at <= now())
         order by next_reference_attempt_at asc for update skip locked limit 20
       )
       update wager_transactions wager set next_reference_attempt_at = now() + interval '60 seconds'
       from due where wager.id = due.id
       returning wager.idempotency_key, wager.provider_id, wager.external_transaction_id,
         wager.player_id, wager.wallet_id, wager.round_id, wager.game_id,
         wager.kind, wager.amount::text, wager.currency,
         wager.reference_external_transaction_id`,
    ));
    for (const transaction of pending) {
      const result = await this.submit(transaction.idempotency_key, {
        providerId: transaction.provider_id,
        externalTransactionId: transaction.external_transaction_id,
        playerId: transaction.player_id,
        walletId: transaction.wallet_id,
        roundId: transaction.round_id,
        gameId: transaction.game_id,
        kind: transaction.kind,
        money: { amount: transaction.amount, currency: transaction.currency },
        ...(transaction.reference_external_transaction_id
          ? { referenceExternalTransactionId: transaction.reference_external_transaction_id }
          : {}),
      });
          this.metrics.transactionStatuses.inc({ status: result.status });
    }
  }

  /** Returns a transaction by its internal ID, including its terminal failure code. */
  async getTransaction(transactionId: string): Promise<Record<string, unknown>> {
    const row = await this.one<TransactionRow>(this.orm.em, 'select * from wager_transactions where id = ?', [transactionId]);
    if (!row) throw new NotFoundException({ code: 'TRANSACTION_NOT_FOUND', message: 'Transaction does not exist' });
    return this.transactionResponse(row);
  }

  /** Resolves a transaction by provider-owned identity rather than internal UUID. */
  async getProviderTransaction(providerId: string, externalTransactionId: string): Promise<Record<string, unknown>> {
    const row = await this.one<TransactionRow>(this.orm.em,
      'select * from wager_transactions where provider_id = ? and external_transaction_id = ?', [providerId, externalTransactionId]);
    if (!row) throw new NotFoundException({ code: 'TRANSACTION_NOT_FOUND', message: 'Transaction does not exist' });
    return this.transactionResponse(row);
  }

  /** Pages immutable ledger entries with a stable opaque (timestamp, ID) cursor. */
  async getLedger(walletId: string, cursor: string | undefined, requestedLimit: number): Promise<Record<string, unknown>> {
    const limit = Number.isInteger(requestedLimit) ? Math.min(Math.max(requestedLimit, 1), 100) : 50;
    const decoded = cursor ? decodeCursor(cursor) : undefined;
    const rows: Record<string, unknown>[] = decoded
      ? await this.orm.em.getConnection().execute<Record<string, unknown>[]>(
        `select l.id, l.transaction_id, l.direction, l.amount::text, l.balance_before::text, l.balance_after::text,
         l.created_at, w.currency from wallet_ledger_entries l join wallets w on w.id = l.wallet_id
         where l.wallet_id = ? and (l.created_at, l.id) < (?, ?) order by l.created_at desc, l.id desc limit ?`,
        [walletId, decoded.createdAt, decoded.id, limit + 1],
      )
      : await this.orm.em.getConnection().execute<Record<string, unknown>[]>(
        `select l.id, l.transaction_id, l.direction, l.amount::text, l.balance_before::text, l.balance_after::text,
         l.created_at, w.currency from wallet_ledger_entries l join wallets w on w.id = l.wallet_id
         where l.wallet_id = ? order by l.created_at desc, l.id desc limit ?`, [walletId, limit + 1]);
    // Fetch one extra row to determine whether the caller should receive a next cursor.
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const last = page.at(-1);
    const nextCursor = hasMore && last ? encodeCursor(last.created_at, last.id) : undefined;
    return {
      entries: page.map((row) => ({
        id: row.id, transactionId: row.transaction_id, direction: row.direction,
        money: { amount: row.amount, currency: row.currency },
        balanceBefore: { amount: row.balance_before, currency: row.currency },
        balanceAfter: { amount: row.balance_after, currency: row.currency },
        createdAt: row.created_at,
      })),
      ...(nextCursor ? { nextCursor } : {}),
    };
  }

  /** Reconstructs balance from the ledger and reports discrepancies without repairing them. */
  async reconcile(walletId: string): Promise<Record<string, unknown>> {
    const wallet = await this.one<WalletRow>(this.orm.em,
      'select id, player_id, currency, balance::text, version from wallets where id = ?', [walletId]);
    if (!wallet) throw new NotFoundException({ code: 'WALLET_NOT_FOUND', message: 'Wallet does not exist' });
    const totals = await this.one<{ calculated_balance: string; checked_entries: string }>(this.orm.em,
      `select coalesce(sum(case when direction = 'CREDIT' then amount else -amount end), 0)::text as calculated_balance,
       count(*)::text as checked_entries from wallet_ledger_entries where wallet_id = ?`, [walletId]);
    // PostgreSQL sums exact NUMERIC values; signed Money represents a difference in either direction.
    const storedBalance = Money.from({ amount: wallet.balance, currency: wallet.currency });
    const calculatedString = totals?.calculated_balance ?? '0.00';
    const calculatedBalance = calculatedString.startsWith('-')
      ? Money.from({ amount: calculatedString.slice(1), currency: wallet.currency }).negate()
      : Money.from({ amount: calculatedString.includes('.') ? calculatedString : `${calculatedString}.00`, currency: wallet.currency });
    const difference = storedBalance.isLessThan(calculatedBalance)
      ? Money.from({ amount: calculatedBalance.subtract(storedBalance).toString(), currency: wallet.currency }).negate()
      : storedBalance.subtract(calculatedBalance);
    const consistent = storedBalance.equals(calculatedBalance);
    if (!consistent) {
      this.metrics.reconciliationDivergences.inc();
      this.logger.warn(JSON.stringify({
        event: 'wallet_reconciliation_divergence',
        correlationId: randomUUID(),
        walletId,
        checkedEntries: Number(totals?.checked_entries ?? '0'),
      }));
    }
    return {
      walletId,
      storedBalance: storedBalance.toJSON(),
      calculatedBalance: calculatedBalance.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: Number(totals?.checked_entries ?? '0'),
    };
  }

  /** Parses untrusted HTTP/SQS payloads before constructing domain values. */
  private parseInput(body: unknown): SubmitInput {
    if (!isRecord(body)) throw new BadRequestException({ code: 'INVALID_PAYLOAD', message: 'Request body must be an object' });
    try {
      const kindText = readString(body, 'kind');
      if (!transactionKinds.has(kindText as WagerTransactionKind)) throw new Error('Unsupported transaction kind');
      const moneyRecord = body.money;
      if (!isRecord(moneyRecord)) throw new Error('money must be an object');
      const reference = body.referenceExternalTransactionId;
      if (reference !== undefined && typeof reference !== 'string') throw new Error('referenceExternalTransactionId must be a string');
      if ((kindText === WagerTransactionKind.Refund || kindText === WagerTransactionKind.Rollback)
        && (typeof reference !== 'string' || reference.length === 0)) {
        throw new Error(`${kindText} requires referenceExternalTransactionId`);
      }
      return {
        providerId: readString(body, 'providerId'),
        externalTransactionId: readString(body, 'externalTransactionId'),
        playerId: readUuid(body, 'playerId'),
        walletId: readUuid(body, 'walletId'),
        roundId: readString(body, 'roundId'),
        gameId: readString(body, 'gameId'),
        kind: kindText as WagerTransactionKind,
        money: Money.from({ amount: readString(moneyRecord, 'amount'), currency: readString(moneyRecord, 'currency') }),
        ...(typeof reference === 'string' ? { referenceExternalTransactionId: reference } : {}),
      };
    } catch (error) {
      throw new BadRequestException({ code: 'INVALID_PAYLOAD', message: errorMessage(error) });
    }
  }

  /** Enforces same-provider, wallet, player, round, kind, amount, and single-reversal rules. */
  private async validateReference(
    em: EntityManager,
    input: SubmitInput,
    reference: TransactionRow,
  ): Promise<'REFERENCE_INVALID' | 'WALLET_MISMATCH' | 'CURRENCY_MISMATCH' | 'REFERENCE_AMOUNT_MISMATCH' | 'REFERENCE_ALREADY_REVERSED' | undefined> {
    if (reference.status !== WagerTransactionStatus.Processed) return 'REFERENCE_INVALID';
    if (reference.wallet_id !== input.walletId || reference.player_id !== input.playerId || reference.round_id !== input.roundId) {
      return 'WALLET_MISMATCH';
    }
    if (reference.currency !== input.money.currency) return 'CURRENCY_MISMATCH';
    const validKind = input.kind === WagerTransactionKind.Refund
      ? reference.kind === WagerTransactionKind.Bet
      : input.kind === WagerTransactionKind.Rollback
        ? [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund].includes(reference.kind)
        : input.kind === WagerTransactionKind.Win && reference.kind === WagerTransactionKind.Bet;
    if (!validKind) return 'REFERENCE_INVALID';
    if ((input.kind === WagerTransactionKind.Refund || input.kind === WagerTransactionKind.Rollback)
      && input.money.toString() !== reference.amount) {
      return 'REFERENCE_AMOUNT_MISMATCH';
    }
    const reversed = await this.one<{ id: string }>(em,
      `select id from wager_transactions where provider_id = ? and reference_external_transaction_id = ? and kind = ? and status = 'PROCESSED'`,
      [input.providerId, input.referenceExternalTransactionId, input.kind]);
    if (reversed) return 'REFERENCE_ALREADY_REVERSED';
    return undefined;
  }

  /** Commits a business rejection and its integration event without touching wallet balance. */
  private async reject(
    em: EntityManager,
    transactionId: string,
    input: SubmitInput,
    balance: Money,
    code: string,
    now: Date,
    inbox?: InboxContext,
  ): Promise<SubmitResult> {
    await this.execute(em,
      `update wager_transactions set status = 'REJECTED', failure_code = ?, processed_at = ?, response_balance = ? where id = ?`,
      [code, now, balance.toString(), transactionId],
    );
    await this.enqueue(em, WagerTransactionRejected.from({
      aggregateId: input.walletId, correlationId: transactionId, causationId: transactionId, occurredAt: now,
      data: { transactionId, providerId: input.providerId, kind: input.kind, failureCode: code },
    }));
    if (inbox) await this.markInboxProcessed(em, inbox);
    return { ...this.result(transactionId, WagerTransactionStatus.Rejected, balance, false), failureCode: code };
  }

  /** Marks the inbox row complete in the same commit as the associated result. */
  private async markInboxProcessed(em: EntityManager, inbox: InboxContext): Promise<void> {
    await this.execute(em,
      'update inbox_messages set processed_at = ? where consumer_name = ? and message_id = ?',
      [new Date(), inbox.consumerName, inbox.messageId],
    );
  }

  /** Serializes a versioned event envelope into the transactional outbox. */
  private async enqueue(em: EntityManager, event: IntegrationEvent<unknown>): Promise<void> {
    await this.execute(em,
      'insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at) values (?, ?, ?, ?::jsonb, ?)',
      [event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt],
    );
  }

  /** Builds the HTTP/SQS result shape from an exact Money balance. */
  private result(
    transactionId: string,
    status: WagerTransactionStatus,
    balance: Money,
    idempotentReplay: boolean,
  ): SubmitResult {
    return { transactionId, status, balance: balance.toJSON(), idempotentReplay };
  }

  /** Returns the original terminal/pending response; a changed payload is a conflict, not a replay. */
  private replay(row: TransactionRow, payloadHash: string): SubmitResult {
    if (row.payload_hash !== payloadHash) {
      throw new ConflictException({ code: 'IDEMPOTENCY_KEY_REUSED', message: 'Idempotency-Key was already used with a different payload' });
    }
    const balance = Money.from({ amount: row.response_balance ?? '0.00', currency: row.currency });
    return {
      ...this.result(row.id, row.status, balance, true),
      ...(row.failure_code ? { failureCode: row.failure_code } : {}),
    };
  }

  /** Maps persistence column names into the public transaction query contract. */
  private transactionResponse(row: TransactionRow): Record<string, unknown> {
    return {
      transactionId: row.id,
      status: row.status,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      kind: row.kind,
      money: { amount: row.amount, currency: row.currency },
      ...(row.failure_code ? { failureCode: row.failure_code } : {}),
      ...(row.reference_transaction_id ? { referenceTransactionId: row.reference_transaction_id } : {}),
    };
  }

  /** Executes a raw query and returns its first row, preserving an optional transaction context. */
  private async one<T>(em: EntityManager, sql: string, params: unknown[]): Promise<T | undefined> {
    const rows = await this.execute<T>(em, sql, params);
    return rows[0];
  }

  /** Passes MikroORM's active transaction object to the low-level connection API. */
  private execute<T>(em: EntityManager, sql: string, params: any[] = []): Promise<T[]> {
    return em.getConnection().execute<T>(sql, params, 'all', em.getTransactionContext());
  }

  /** Reads the idempotency record after a raced insert or transaction failure. */
  private async findByIdempotencyKey(key: string): Promise<TransactionRow | undefined> {
    return this.one<TransactionRow>(this.orm.em, 'select * from wager_transactions where idempotency_key = ?', [key]);
  }

  /** Distinguishes a duplicate-wallet conflict from unrelated database uniqueness failures. */
  private async walletExists(playerId: string, currency: string): Promise<boolean> {
    return Boolean(await this.one<{ id: string }>(this.orm.em,
      'select id from wallets where player_id = ? and currency = ?', [playerId, currency]));
  }
}

/** Ensures untrusted JSON is an object before accessing domain fields. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Reads a required non-empty string and keeps validation errors field-specific. */
function readString(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  if (typeof value !== 'string' || value.length === 0) throw new Error(`${key} must be a non-empty string`);
  return value;
}

/** Extracts a safe validation detail without assuming every thrown value is an Error. */
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Invalid request';
}

/** Computes the documented SHA-256 digest over canonical business JSON. */
export function hashCanonicalPayload(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

/** Sorts object keys recursively while preserving array order for stable idempotency hashes. */
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

/** Recognizes PostgreSQL duplicate-key errors, including wrapped driver causes. */
function isUniqueViolation(error: unknown): boolean {
  return errorCode(error) === '23505';
}

/** Detects SQLSTATEs for serialization, deadlock, and lock-timeout metrics. */
function isLockConflict(error: unknown): boolean {
  const code = errorCode(error);
  return code === '40P01' || code === '55P03' || code === '40001';
}

/** Distinguishes retryable connection/service outages from terminal SQL failures. */
function isDatabaseUnavailable(error: unknown): boolean {
  const code = errorCode(error);
  return (code?.startsWith('08') ?? false)
    || ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', '57P01', '57P02', '57P03'].includes(code ?? '');
}

/** Walks wrapped exceptions until the underlying PostgreSQL or socket code is found. */
function errorCode(error: unknown): string | undefined {
  if (!isRecord(error)) return undefined;
  if (typeof error.code === 'string') return error.code;
  return isRecord(error.cause) ? errorCode(error.cause) : undefined;
}

/** Encodes the ledger timestamp/ID ordering key as an opaque base64url cursor. */
function encodeCursor(createdAt: unknown, id: unknown): string {
  return Buffer.from(JSON.stringify({ createdAt: new Date(String(createdAt)).toISOString(), id: String(id) })).toString('base64url');
}

/** Validates and decodes an opaque cursor; malformed values become a stable 400 response. */
function decodeCursor(cursor: string): { createdAt: Date; id: string } {
  try {
    const value: unknown = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (!isRecord(value) || typeof value.createdAt !== 'string' || typeof value.id !== 'string') throw new Error();
    const createdAt = new Date(value.createdAt);
    if (Number.isNaN(createdAt.getTime())) throw new Error();
    return { createdAt, id: value.id };
  } catch {
    throw new BadRequestException({ code: 'INVALID_CURSOR', message: 'Cursor is invalid' });
  }
}

/** Reads a required UUID before it reaches PostgreSQL's native UUID parser. */
function readUuid(record: Record<string, unknown>, key: string): string {
  const value = readString(record, key);
  if (!isUuid(value)) throw new Error(`${key} must be a UUID`);
  return value;
}

/** Accepts supported UUID versions/variants for player and wallet identifiers. */
function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

/** Classifies non-unique SQL/data failures eligible for a terminal FAILED audit record. */
function isPermanentDatabaseFailure(error: unknown): boolean {
  const code = errorCode(error);
  return code === 'P0001'
    || (code?.startsWith('22') ?? false)
    || ((code?.startsWith('23') ?? false) && code !== '23505');
}