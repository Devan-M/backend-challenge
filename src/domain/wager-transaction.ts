import { Money } from './money.js';
import { LedgerDirection } from './wallet-ledger-entry.js';

/** Provider operation types; OPENING is reserved for wallet creation internals. */
export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

/** PENDING states may advance; PROCESSED, REJECTED, and FAILED are terminal. */
export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

/** Stable machine-readable reasons returned to providers or operations. */
export type FailureCode =
  | 'INSUFFICIENT_FUNDS'
  | 'REVERSAL_WOULD_OVERDRAW'
  | 'REFERENCE_NOT_FOUND'
  | 'REFERENCE_INVALID'
  | 'REFERENCE_AMOUNT_MISMATCH'
  | 'REFERENCE_ALREADY_REVERSED'
  | 'WALLET_NOT_FOUND'
  | 'WALLET_MISMATCH'
  | 'CURRENCY_MISMATCH'
  | 'PERMANENT_INFRASTRUCTURE';

/** Creation contract; the static factory validates public versus internal kinds. */
export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt?: Date;
}

/** Persisted snapshot used to rebuild a transaction without replaying transitions. */
export interface WagerTransactionState extends CreateWagerTransactionProps {
  referenceTransactionId?: string;
  status: WagerTransactionStatus;
  failureCode?: FailureCode;
  processedAt?: Date;
}

/** Signals an invalid transition, such as changing an already terminal transaction. */
export class InvalidTransactionStateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidTransactionStateError';
  }
}

/** Domain entity for provider idempotency, references, lifecycle, and ledger direction. */
export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
  ) {}

  /** Creates a public wager in PENDING and enforces reference/amount preconditions. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening) {
      throw new InvalidTransactionStateError('OPENING transactions are internal');
    }
    if ((props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback)
      && !props.referenceExternalTransactionId) {
      throw new InvalidTransactionStateError(`${props.kind} requires a reference`);
    }
    if (!props.money.isPositive()) throw new InvalidTransactionStateError('Transaction amount must be positive');

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.createdAt ?? new Date(),
      WagerTransactionStatus.Pending,
    );
  }

  /** Rehydrates persisted state exactly; it intentionally does not revalidate history. */
  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt ?? new Date(),
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
    );
  }

  /** Current lifecycle state; only non-terminal states may transition. */
  get status(): WagerTransactionStatus { return this._status; }
  /** Internal ID of the resolved row, distinct from the provider's external reference. */
  get referenceTransactionId(): string | undefined { return this._referenceTransactionId; }
  /** Stable machine-readable reason for a rejected or failed operation. */
  get failureCode(): FailureCode | undefined { return this._failureCode; }
  /** Time the operation entered a terminal status, when applicable. */
  get processedAt(): Date | undefined { return this._processedAt; }

  /** Applies a terminal success and records its internal reference and completion time. */
  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertNonTerminal();
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._status = WagerTransactionStatus.Processed;
  }

  /** Defers reversals whose referenced provider transaction is not ready yet. */
  markPendingReference(): void {
    this.assertNonTerminal();
    this._status = WagerTransactionStatus.PendingReference;
  }

  /** Records a terminal business-rule rejection and its stable reason code. */
  reject(code: FailureCode): void {
    this.assertNonTerminal();
    this._failureCode = code;
    this._status = WagerTransactionStatus.Rejected;
  }

  /** Records a terminal infrastructure failure for audit and prevents retries changing it. */
  fail(code: FailureCode): void {
    this.assertNonTerminal();
    this._failureCode = code;
    this._status = WagerTransactionStatus.Failed;
  }

  /** Identifies statuses that must never transition again. */
  isTerminal(): boolean {
    return this._status === WagerTransactionStatus.Processed
      || this._status === WagerTransactionStatus.Rejected
      || this._status === WagerTransactionStatus.Failed;
  }

  /** LOSS records a result but has no wallet movement or ledger row. */
  /** LOSS records an outcome but intentionally has no wallet movement. */
  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  /** REFUND and ROLLBACK require a provider reference. */
  /** Reversals need a provider transaction ID to resolve their target. */
  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  /** Compares canonical business payload digests for idempotent replay. */
  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /** Chooses the ledger direction, inverting the referenced operation for ROLLBACK. */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback:
        if (!reference || reference.kind === WagerTransactionKind.Loss
          || reference.kind === WagerTransactionKind.Rollback) {
          throw new InvalidTransactionStateError('ROLLBACK requires a reversible transaction reference');
        }
        return reference.ledgerDirectionFor() === LedgerDirection.Credit
          ? LedgerDirection.Debit
          : LedgerDirection.Credit;
      default:
        throw new InvalidTransactionStateError(`${this.kind} does not produce a ledger entry`);
    }
  }

  /** Centralizes the terminal-state rule shared by every state transition. */
  private assertNonTerminal(): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(`Cannot transition terminal transaction ${this._status}`);
    }
  }
}