// End-to-end scenarios require the Compose PostgreSQL, LocalStack, and API services to be running.
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { DeleteMessageCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';

const apiUrl = process.env.API_URL ?? 'http://localhost:3000';
const apiUrls = [apiUrl, process.env.API_SECONDARY_URL, process.env.API_TERTIARY_URL].filter(
  (url): url is string => Boolean(url),
);
const queueUrl = process.env.WAGER_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
const deadLetterQueueUrl = process.env.WAGER_DLQ_URL ?? 'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
const eventsQueueUrl = process.env.EVENTS_QUEUE_URL ?? 'http://localhost:4566/000000000000/wagering-events.fifo';
let database: MikroORM;

interface WagerResponse {
  transactionId: string;
  status: string;
  idempotentReplay: boolean;
  balance: { amount: string; currency: string };
  failureCode?: string;
}

const sqs = new SQSClient({
  region: process.env.AWS_REGION ?? 'us-east-1',
  endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
  },
});

/** Recovers PostgreSQL SQLSTATE from wrapped MikroORM driver exceptions. */
function readDatabaseCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const record = error as { code?: unknown; cause?: unknown };
  if (typeof record.code === 'string') return record.code;
  return readDatabaseCode(record.cause);
}

/** Restarts every API replica while leaving PostgreSQL and SQS data intact. */
function restartApplicationCluster(): void {
  runDockerCompose(['restart', 'app', 'app-2', 'app-3']);
}

/** Runs Compose with the current test environment and fails immediately on a nonzero exit. */
function runDockerCompose(args: string[]): void {
  const result = Bun.spawnSync({
    cmd: ['docker', 'compose', '--profile', 'multi', ...args],
    env: Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined)),
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (result.exitCode !== 0) {
    throw new Error(`docker compose ${args.join(' ')} failed: ${result.stderr.toString()}`);
  }
}

/** Waits until every configured replica has passed database and SQS readiness checks. */
async function waitForApiInstances(): Promise<boolean> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const readiness = await Promise.all(apiUrls.map(async (targetApiUrl) => {
      try {
        return (await fetch(`${targetApiUrl}/health/ready`)).ok;
      } catch {
        return false;
      }
    }));
    if (readiness.every(Boolean)) return true;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  return false;
}

/** Sums a bounded-cardinality metric across replicas because any one may consume a message. */
async function readMetricTotal(metricName: string, source: string): Promise<number> {
  const values = await Promise.all(apiUrls.map(async (targetApiUrl) => {
    const text = await fetch(`${targetApiUrl}/metrics`).then((response) => response.text());
    const value = text.match(new RegExp(`^${metricName}\\{source="${source}"\\}\\s+([\\d.]+)`, 'm'))?.[1];
    return Number(value ?? 0);
  }));
  return values.reduce((total, value) => total + value, 0);
}

// Use a separate ORM connection to inject database failures and inspect persisted invariants.
beforeAll(async () => {
  database = await MikroORM.init({
    driver: PostgreSqlDriver,
    host: process.env.DATABASE_HOST ?? 'localhost',
    port: Number(process.env.DATABASE_PORT ?? 5432),
    user: process.env.DATABASE_USER ?? 'wagering',
    password: process.env.DATABASE_PASSWORD ?? 'wagering',
    dbName: process.env.DATABASE_NAME ?? 'wagering',
    entities: [],
    discovery: { warnWhenNoEntities: false },
  });
});

// Close test-owned clients so Bun can exit cleanly after the container suite.
afterAll(async () => {
  await database.close(true);
  sqs.destroy();
});

describe('container-backed wagering API', () => {
  // Two new IDs contend for one balance; one succeeds and one is rejected.
  test('serializes competing bets and preserves idempotent replay results', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }),
    });
    expect(walletResponse.status).toBe(201);
    const wallet = await walletResponse.json() as { id: string };

    const basePayload = {
      providerId: 'integration-provider',
      playerId,
      walletId: wallet.id,
      roundId: randomUUID(),
      gameId: 'integration-game',
      kind: 'BET',
      money: { amount: '80.00', currency: 'BRL' },
    };
    const requests = [randomUUID(), randomUUID()].map((externalTransactionId) => ({
      ...basePayload,
      externalTransactionId,
    }));
    const keys = requests.map((request) => `${request.providerId}:${request.externalTransactionId}`);
    const submit = async (payload: Record<string, unknown>, key: string, targetApiUrl = apiUrl) => {
      const response = await fetch(`${targetApiUrl}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': key },
        body: JSON.stringify(payload),
      });
      return { statusCode: response.status, body: await response.json() as WagerResponse };
    };

    const competing = await Promise.all(requests.map((request, index) =>
      submit(request, keys[index]!, apiUrls[index % apiUrls.length]!),
    ));
    expect(competing.filter((result) => result.body.status === 'PROCESSED')).toHaveLength(1);
    expect(competing.filter((result) => result.body.status === 'REJECTED')).toHaveLength(1);
    expect(competing.filter((result) => result.statusCode === 422)).toHaveLength(1);

    const winnerIndex = competing.findIndex((result) => result.body.status === 'PROCESSED');
    const winnerPayload = requests[winnerIndex]!;
    const winnerKey = keys[winnerIndex]!;
    const replays = await Promise.all(Array.from({ length: 50 }, (_, index) =>
      submit(winnerPayload, winnerKey, apiUrls[index % apiUrls.length]!),
    ));
    expect(replays.filter((result) => result.statusCode !== 200 || result.body.idempotentReplay !== true).slice(0, 5)).toEqual([]);
    expect(replays.every((result) => result.body.balance.amount === '20.00')).toBe(true);

    const conflictingPayload = { ...winnerPayload, money: { amount: '79.00', currency: 'BRL' } };
    expect((await submit(conflictingPayload, winnerKey)).statusCode).toBe(409);

    const currentWallet = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
      balance: { amount: string };
    };
    expect(currentWallet.balance.amount).toBe('20.00');

    const ledger = await fetch(`${apiUrl}/wallets/${wallet.id}/ledger?limit=100`)
      .then((response) => response.json()) as { entries: Array<{ direction: string }> };
    expect(ledger.entries.filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
    expect(ledger.entries).toHaveLength(2);

    const reconciliation = await fetch(`${apiUrl}/wallets/${wallet.id}/reconciliation`, { method: 'POST' })
      .then((response) => response.json()) as { consistent: boolean; calculatedBalance: { amount: string } };
    expect(reconciliation.consistent).toBe(true);
    expect(reconciliation.calculatedBalance.amount).toBe('20.00');
  });

  // Independent aggregates should make progress concurrently across app processes.
  test('processes distinct wallet wagers in parallel across instances', async () => {
    const wallets = await Promise.all(Array.from({ length: 30 }, async (_, index) => {
      const targetApiUrl = apiUrls[index % apiUrls.length]!;
      const playerId = randomUUID();
      const response = await fetch(`${targetApiUrl}/wallets`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ playerId, initialBalance: { amount: '1.00', currency: 'BRL' } }),
      });
      expect(response.status).toBe(201);
      return { ...(await response.json() as { id: string }), playerId, targetApiUrl };
    }));

    const results = await Promise.all(wallets.map(async ({ id: walletId, playerId, targetApiUrl }) => {
      const externalTransactionId = randomUUID();
      const response = await fetch(`${targetApiUrl}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': `parallel-wallet-provider:${externalTransactionId}`,
        },
        body: JSON.stringify({
          providerId: 'parallel-wallet-provider',
          externalTransactionId,
          playerId,
          walletId,
          roundId: randomUUID(),
          gameId: 'integration-game',
          kind: 'BET',
          money: { amount: '0.50', currency: 'BRL' },
        }),
      });
      return { statusCode: response.status, body: await response.json() as WagerResponse, walletId };
    }));
    expect(results.every((result) => result.statusCode === 200 && result.body.status === 'PROCESSED')).toBe(true);

    const finalBalances = await Promise.all(results.map(async ({ walletId }) => {
      const response = await fetch(`${apiUrl}/wallets/${walletId}`);
      return (await response.json() as { balance: { amount: string } }).balance.amount;
    }));
    expect(finalBalances.every((balance) => balance === '0.50')).toBe(true);
  }, 30_000);

  // All 50 requests are first deliveries of the same key, not replays after a warm-up call.
  test('deduplicates 50 simultaneous first submissions of the same wager', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }),
    });
    expect(walletResponse.status).toBe(201);
    const wallet = await walletResponse.json() as { id: string };
    const externalTransactionId = randomUUID();
    const payload = {
      providerId: 'integration-provider',
      externalTransactionId,
      playerId,
      walletId: wallet.id,
      roundId: randomUUID(),
      gameId: 'integration-game',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
    };
    const idempotencyKey = `${payload.providerId}:${externalTransactionId}`;

    const responses = await Promise.all(Array.from({ length: 50 }, async (_, index) => {
      const targetApiUrl = apiUrls[index % apiUrls.length]!;
      const response = await fetch(`${targetApiUrl}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
        body: JSON.stringify(payload),
      });
      return {
        statusCode: response.status,
        body: await response.json() as {
          transactionId: string;
          status: string;
          idempotentReplay: boolean;
          balance: { amount: string };
        },
      };
    }));

    expect(responses.every((response) => response.statusCode === 200 && response.body.status === 'PROCESSED')).toBe(true);
    expect(new Set(responses.map((response) => response.body.transactionId)).size).toBe(1);
    expect(responses.filter((response) => !response.body.idempotentReplay)).toHaveLength(1);
    expect(responses.every((response) => response.body.balance.amount === '90.00')).toBe(true);

    const currentWallet = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
      balance: { amount: string };
    };
    expect(currentWallet.balance.amount).toBe('90.00');
    const ledger = await fetch(`${apiUrl}/wallets/${wallet.id}/ledger?limit=100`)
      .then((response) => response.json()) as { entries: Array<{ direction: string }> };
    expect(ledger.entries.filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
    expect(ledger.entries).toHaveLength(2);
  }, 20_000);

  // Attempts to violate schema rules directly, bypassing all application validation.
  test('enforces wallet uniqueness, non-negative balance, and ledger immutability in PostgreSQL', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }),
    });
    const wallet = await walletResponse.json() as { id: string };
    const duplicateWallet = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '0.00', currency: 'BRL' } }),
    });
    expect(duplicateWallet.status).toBe(409);

    let negativeBalanceError: unknown;
    try {
      await database.em.getConnection().execute('update wallets set balance = ? where id = ?', ['-0.01', wallet.id]);
    } catch (error) {
      negativeBalanceError = error;
    }
    expect(readDatabaseCode(negativeBalanceError)).toBe('23514');

    const ledgerRows: Array<{ id: string }> = await database.em.getConnection().execute(
      'select id from wallet_ledger_entries where wallet_id = ?', [wallet.id],
    );
    let immutableLedgerError: unknown;
    try {
      await database.em.getConnection().execute(
        'update wallet_ledger_entries set amount = ? where id = ?', ['9.00', ledgerRows[0]!.id],
      );
    } catch (error) {
      immutableLedgerError = error;
    }
    expect(immutableLedgerError).toBeDefined();
    expect((await fetch(`${apiUrl}/wallets/${wallet.id}`).then((result) => result.json()) as {
      balance: { amount: string };
    }).balance.amount).toBe('10.00');
  });

  // Injects a transient outbox failure and proves every earlier financial write rolled back.
  test('rolls back wallet, transaction, and ledger when the outbox insert fails', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }),
    });
    const wallet = await walletResponse.json() as { id: string };
    const externalTransactionId = randomUUID();
    const idempotencyKey = `atomicity-probe:${externalTransactionId}`;
    const payload = {
      providerId: 'atomicity-probe',
      externalTransactionId,
      playerId,
      walletId: wallet.id,
      roundId: randomUUID(),
      gameId: 'integration-game',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
    };

    await database.em.getConnection().execute(`
      create function reject_atomicity_probe_outbox() returns trigger language plpgsql as $$
      begin
        if new.event_type = 'WagerTransactionProcessed' and new.payload #>> '{data,providerId}' = 'atomicity-probe' then
          raise exception using errcode = '08006', message = 'forced transient outbox failure';
        end if;
        return new;
      end;
      $$;
      create trigger reject_atomicity_probe_outbox before insert on outbox_messages
      for each row execute function reject_atomicity_probe_outbox();
    `);

    let response: Response;
    try {
      response = await fetch(`${apiUrl}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
        body: JSON.stringify(payload),
      });
      expect(response.status).toBe(503);

      const state = await database.em.getConnection().execute<Array<{
        balance: string;
        transaction_count: string;
        ledger_count: string;
      }>>(
        `select w.balance::text as balance,
         (select count(*) from wager_transactions where idempotency_key = ?)::text as transaction_count,
         (select count(*) from wallet_ledger_entries where wallet_id = w.id)::text as ledger_count
         from wallets w where w.id = ?`,
        [idempotencyKey, wallet.id],
      );
      expect(state[0]).toEqual({ balance: '100.00', transaction_count: '0', ledger_count: '1' });
    } finally {
      await database.em.getConnection().execute('drop trigger if exists reject_atomicity_probe_outbox on outbox_messages');
      await database.em.getConnection().execute('drop function if exists reject_atomicity_probe_outbox()');
    }

    const retry = await fetch(`${apiUrl}/wagering/transactions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
      body: JSON.stringify(payload),
    });
    expect(retry.status).toBe(200);
    const walletAfter = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((result) => result.json()) as {
      balance: { amount: string };
    };
    expect(walletAfter.balance.amount).toBe('90.00');
  });

  // A terminal database error is stored as FAILED and replayed without a financial effect.
  test('persists permanent infrastructure failures as auditable FAILED transactions', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }),
    });
    const wallet = await walletResponse.json() as { id: string };
    const externalTransactionId = randomUUID();
    const idempotencyKey = `permanent-infra-probe:${externalTransactionId}`;
    const payload = {
      providerId: 'permanent-infra-probe',
      externalTransactionId,
      playerId,
      walletId: wallet.id,
      roundId: randomUUID(),
      gameId: 'integration-game',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
    };
    await database.em.getConnection().execute(`
      create function reject_permanent_probe_outbox() returns trigger language plpgsql as $$
      begin
        if new.event_type = 'WagerTransactionProcessed' and new.payload #>> '{data,providerId}' = 'permanent-infra-probe' then
          raise exception 'forced permanent outbox failure';
        end if;
        return new;
      end;
      $$;
      create trigger reject_permanent_probe_outbox before insert on outbox_messages
      for each row execute function reject_permanent_probe_outbox();
    `);

    let response: Response;
    try {
      response = await fetch(`${apiUrl}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
        body: JSON.stringify(payload),
      });
      const failure = await response.json() as { code: string; transactionId: string; retryable: boolean };
      expect(response.status).toBe(500);
      expect(failure.code).toBe('PERMANENT_INFRASTRUCTURE');
      expect(failure.retryable).toBe(false);

      const failedRows: Array<{ status: string; failure_code: string; response_balance: string }> =
        await database.em.getConnection().execute(
          `select status, failure_code, response_balance::text from wager_transactions where idempotency_key = ?`,
          [idempotencyKey],
        );
      expect(failedRows[0]).toEqual({
        status: 'FAILED', failure_code: 'PERMANENT_INFRASTRUCTURE', response_balance: '100.00',
      });
      const ledger: Array<{ id: string }> = await database.em.getConnection().execute(
        'select id from wallet_ledger_entries where wallet_id = ?', [wallet.id],
      );
      expect(ledger).toHaveLength(1);
    } finally {
      await database.em.getConnection().execute('drop trigger if exists reject_permanent_probe_outbox on outbox_messages');
      await database.em.getConnection().execute('drop function if exists reject_permanent_probe_outbox()');
    }

    const replay = await fetch(`${apiUrl}/wagering/transactions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': idempotencyKey },
      body: JSON.stringify(payload),
    });
    const replayBody = await replay.json() as WagerResponse;
    expect(replay.status).toBe(500);
    expect(replayBody.status).toBe('FAILED');
    expect(replayBody.failureCode).toBe('PERMANENT_INFRASTRUCTURE');
    const walletAfter = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((result) => result.json()) as {
      balance: { amount: string };
    };
    expect(walletAfter.balance.amount).toBe('100.00');
  });

  // The consumer must audit permanent infrastructure errors and move the original to DLQ.
  test('audits permanent SQS infrastructure failures and routes them to DLQ', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '50.00', currency: 'BRL' } }),
    });
    const wallet = await walletResponse.json() as { id: string };
    const providerId = 'permanent-sqs-probe';
    const externalTransactionId = randomUUID();
    const messageId = randomUUID();
    const data = {
      providerId,
      externalTransactionId,
      idempotencyKey: `${providerId}:${externalTransactionId}`,
      playerId,
      walletId: wallet.id,
      roundId: randomUUID(),
      gameId: 'integration-game',
      kind: 'BET',
      money: { amount: '5.00', currency: 'BRL' },
    };
    const messageBody = JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data });
    await database.em.getConnection().execute(`
      create function reject_permanent_sqs_outbox() returns trigger language plpgsql as $$
      begin
        if new.event_type = 'WagerTransactionProcessed' and new.payload #>> '{data,providerId}' = '${providerId}' then
          raise exception 'forced permanent SQS failure';
        end if;
        return new;
      end;
      $$;
      create trigger reject_permanent_sqs_outbox before insert on outbox_messages
      for each row execute function reject_permanent_sqs_outbox();
    `);

    try {
      await sqs.send(new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: messageBody,
        MessageGroupId: wallet.id,
        MessageDeduplicationId: randomUUID(),
      }));

      const transactionDeadline = Date.now() + 15_000;
      let failedStatus: { status: string; failureCode?: string } | undefined;
      while (Date.now() < transactionDeadline) {
        const response = await fetch(`${apiUrl}/providers/${providerId}/wagering/transactions/${externalTransactionId}`);
        if (response.ok) {
          failedStatus = await response.json() as { status: string; failureCode?: string };
          if (failedStatus.status === 'FAILED') break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(failedStatus?.status).toBe('FAILED');
      expect(failedStatus?.failureCode).toBe('PERMANENT_INFRASTRUCTURE');

      const dlqDeadline = Date.now() + 15_000;
      let failureInDlq = false;
      while (Date.now() < dlqDeadline && !failureInDlq) {
        const batch = await sqs.send(new ReceiveMessageCommand({
          QueueUrl: deadLetterQueueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 1,
        }));
        for (const message of batch.Messages ?? []) {
          if (!message.Body || !message.ReceiptHandle) continue;
          try {
            const deadLetter = JSON.parse(message.Body) as { originalBody?: string; failureCode?: string };
            if (deadLetter.originalBody === messageBody && deadLetter.failureCode === 'PERMANENT_INFRASTRUCTURE') {
              failureInDlq = true;
            }
          } finally {
            await sqs.send(new DeleteMessageCommand({ QueueUrl: deadLetterQueueUrl, ReceiptHandle: message.ReceiptHandle }));
          }
        }
      }
      expect(failureInDlq).toBe(true);
      const walletAfter = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
        balance: { amount: string };
      };
      expect(walletAfter.balance.amount).toBe('50.00');
    } finally {
      await database.em.getConnection().execute('drop trigger if exists reject_permanent_sqs_outbox on outbox_messages');
      await database.em.getConnection().execute('drop function if exists reject_permanent_sqs_outbox()');
    }
  }, 30_000);

  // Corrupts the materialized balance directly and verifies detection without auto-repair.
  test('reports reconciliation divergence without silently correcting the wallet', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '10.00', currency: 'BRL' } }),
    });
    const wallet = await walletResponse.json() as { id: string };
    await database.em.getConnection().execute(
      'update wallets set balance = ? where id = ?', ['9.00', wallet.id],
    );

    const metricsBefore = await fetch(`${apiUrl}/metrics`).then((response) => response.text());
    const countBefore = Number(metricsBefore.match(/^wallet_reconciliation_divergences_total\s+([\d.]+)/m)?.[1] ?? 0);
    const reconciliation = await fetch(`${apiUrl}/wallets/${wallet.id}/reconciliation`, { method: 'POST' });
    const result = await reconciliation.json() as {
      consistent: boolean;
      difference: { amount: string };
      calculatedBalance: { amount: string };
    };
    expect(result.consistent).toBe(false);
    expect(result.difference.amount).toBe('-1.00');
    expect(result.calculatedBalance.amount).toBe('10.00');

    const walletAfter = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
      balance: { amount: string };
    };
    expect(walletAfter.balance.amount).toBe('9.00');
    const metricsAfter = await fetch(`${apiUrl}/metrics`).then((response) => response.text());
    const countAfter = Number(metricsAfter.match(/^wallet_reconciliation_divergences_total\s+([\d.]+)/m)?.[1] ?? 0);
    expect(countAfter).toBe(countBefore + 1);
  });

      // Exercises BET, value-changing WIN, no-effect LOSS, single REFUND, and ROLLBACK.
      test('applies one refund and reverses it with a rollback', async () => {
        const playerId = randomUUID();
        const walletResponse = await fetch(`${apiUrl}/wallets`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }),
        });
        const wallet = await walletResponse.json() as { id: string };
        const context = { providerId: 'integration-provider', playerId, walletId: wallet.id, roundId: randomUUID(), gameId: 'integration-game' };
        const submit = async (payload: Record<string, unknown>) => {
          const key = `${String(payload.providerId)}:${String(payload.externalTransactionId)}`;
          const response = await fetch(`${apiUrl}/wagering/transactions`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'idempotency-key': key },
            body: JSON.stringify(payload),
          });
          return { statusCode: response.status, body: await response.json() as WagerResponse };
        };

        const bet = { ...context, externalTransactionId: randomUUID(), kind: 'BET', money: { amount: '25.00', currency: 'BRL' } };
        expect((await submit(bet)).body.status).toBe('PROCESSED');
        const win = {
          ...context, externalTransactionId: randomUUID(), kind: 'WIN',
          referenceExternalTransactionId: bet.externalTransactionId, money: { amount: '50.00', currency: 'BRL' },
        };
        expect((await submit(win)).body.status).toBe('PROCESSED');
        const loss = { ...context, externalTransactionId: randomUUID(), kind: 'LOSS', money: { amount: '25.00', currency: 'BRL' } };
        expect((await submit(loss)).body.status).toBe('PROCESSED');
        const refund = {
          ...context, externalTransactionId: randomUUID(), kind: 'REFUND',
          referenceExternalTransactionId: bet.externalTransactionId, money: bet.money,
        };
        expect((await submit(refund)).body.status).toBe('PROCESSED');

        const duplicateRefund = {
          ...refund, externalTransactionId: randomUUID(),
        };
        const rejected = await submit(duplicateRefund);
        expect(rejected.statusCode).toBe(422);
        expect(rejected.body.failureCode).toBe('REFERENCE_ALREADY_REVERSED');

        const rollback = {
          ...context, externalTransactionId: randomUUID(), kind: 'ROLLBACK',
          referenceExternalTransactionId: refund.externalTransactionId, money: refund.money,
        };
        expect((await submit(rollback)).body.status).toBe('PROCESSED');
        const currentWallet = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
          balance: { amount: string };
        };
        expect(currentWallet.balance.amount).toBe('125.00');
      });

      // Persists a missing-reference reversal, then lets the scheduler apply it after BET arrives.
      test('retries a reversal that arrives before its referenced bet', async () => {
        const playerId = randomUUID();
        const walletResponse = await fetch(`${apiUrl}/wallets`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }),
        });
        const wallet = await walletResponse.json() as { id: string };
        const context = { providerId: 'integration-provider', playerId, walletId: wallet.id, roundId: randomUUID(), gameId: 'integration-game' };
        const betId = randomUUID();
        const rollback = {
          ...context,
          externalTransactionId: randomUUID(),
          kind: 'ROLLBACK',
          referenceExternalTransactionId: betId,
          money: { amount: '25.00', currency: 'BRL' },
        };
        const rollbackKey = `${rollback.providerId}:${rollback.externalTransactionId}`;
        const pendingResponse = await fetch(`${apiUrl}/wagering/transactions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'idempotency-key': rollbackKey },
          body: JSON.stringify(rollback),
        });
        expect(pendingResponse.status).toBe(202);
        const pending = await pendingResponse.json() as { transactionId: string; status: string };
        expect(pending.status).toBe('PENDING_REFERENCE');

        const bet = {
          ...context, externalTransactionId: betId, kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
        };
        const betResponse = await fetch(`${apiUrl}/wagering/transactions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'idempotency-key': `${bet.providerId}:${bet.externalTransactionId}` },
          body: JSON.stringify(bet),
        });
        expect(betResponse.status).toBe(200);

        const deadline = Date.now() + 15_000;
        let status: string | undefined;
        while (Date.now() < deadline) {
          const response = await fetch(`${apiUrl}/wagering/transactions/${pending.transactionId}`);
          if (response.ok) {
            status = (await response.json() as { status: string }).status;
            if (status === 'PROCESSED') break;
          }
          await new Promise((resolve) => setTimeout(resolve, 200));
        }
        expect(status).toBe('PROCESSED');
        const currentWallet = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
          balance: { amount: string };
        };
        expect(currentWallet.balance.amount).toBe('100.00');
      }, 20_000);

  // Competing publishers drain events from many aggregates without losing their event IDs.
  test('publishes concurrent outbox rows and delivers every event once', async () => {
    const wallets = await Promise.all(Array.from({ length: 30 }, async (_, index) => {
      const targetApiUrl = apiUrls[index % apiUrls.length]!;
      const response = await fetch(`${targetApiUrl}/wallets`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ playerId: randomUUID(), initialBalance: { amount: '1.00', currency: 'BRL' } }),
      });
      expect(response.status).toBe(201);
      return (await response.json() as { id: string }).id;
    }));
    const placeholders = wallets.map(() => '?').join(',');
    const deadline = Date.now() + 20_000;
    let expectedEventIds = new Set<string>();
    while (Date.now() < deadline) {
      const rows: Array<{ id: string; published_at: Date | null }> = await database.em.getConnection().execute(
        `select id, published_at from outbox_messages
         where event_type = 'WalletBalanceChanged' and aggregate_id in (${placeholders})`,
        wallets,
      );
      if (rows.length === wallets.length && rows.every((row) => row.published_at !== null)) {
        expectedEventIds = new Set(rows.map((row) => row.id));
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    expect(expectedEventIds.size).toBe(wallets.length);

    const observedEventIds = new Set<string>();
    const eventDeadline = Date.now() + 45_000;
    while (Date.now() < eventDeadline && observedEventIds.size < expectedEventIds.size) {
      const batch = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: eventsQueueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
      }));
      for (const message of batch.Messages ?? []) {
        if (!message.Body || !message.ReceiptHandle) continue;
        try {
          const event = JSON.parse(message.Body) as { eventId?: string };
          if (event.eventId && expectedEventIds.has(event.eventId)) observedEventIds.add(event.eventId);
        } finally {
          await sqs.send(new DeleteMessageCommand({ QueueUrl: eventsQueueUrl, ReceiptHandle: message.ReceiptHandle }));
        }
      }
    }
    expect(observedEventIds).toEqual(expectedEventIds);
  }, 60_000);

  // A malformed envelope is terminal and must be copied to the configured dead-letter queue.
  test('moves permanently invalid queue messages to the DLQ', async () => {
    const messageId = randomUUID();
    const invalidBody = JSON.stringify({ messageId, type: 'UnsupportedMessageType', data: {} });
    await sqs.send(new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: invalidBody,
      MessageGroupId: randomUUID(),
      MessageDeduplicationId: randomUUID(),
    }));

    const deadline = Date.now() + 15_000;
    let deadLetterObserved = false;
    while (Date.now() < deadline && !deadLetterObserved) {
      const batch = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: deadLetterQueueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
      }));
      for (const message of batch.Messages ?? []) {
        if (!message.Body || !message.ReceiptHandle) continue;
        try {
          const deadLetter = JSON.parse(message.Body) as { originalBody?: string; failureCode?: string };
          if (deadLetter.originalBody === invalidBody && deadLetter.failureCode === 'INVALID_ENVELOPE') {
            deadLetterObserved = true;
          }
        } finally {
          await sqs.send(new DeleteMessageCommand({ QueueUrl: deadLetterQueueUrl, ReceiptHandle: message.ReceiptHandle }));
        }
      }
    }
    expect(deadLetterObserved).toBe(true);
  }, 20_000);

  if (process.env.API_SECONDARY_URL && process.env.API_TERTIARY_URL) {
    // Commits output during a broker outage, restarts publishers, and checks eventual delivery.
    test('retries committed outbox events after SQS and publishers restart', async () => {
      const retriesBefore = await readMetricTotal('wager_retries_total', 'outbox');
      runDockerCompose(['stop', '--timeout', '5', 'localstack']);
      try {
        const walletIds = await Promise.all(Array.from({ length: 30 }, async (_, index) => {
          const targetApiUrl = apiUrls[index % apiUrls.length]!;
          const response = await fetch(`${targetApiUrl}/wallets`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ playerId: randomUUID(), initialBalance: { amount: '1.00', currency: 'BRL' } }),
          });
          expect(response.status).toBe(201);
          return (await response.json() as { id: string }).id;
        }));

        const retryDeadline = Date.now() + 20_000;
        let retryObserved = false;
        while (Date.now() < retryDeadline) {
          if (await readMetricTotal('wager_retries_total', 'outbox') > retriesBefore) {
            retryObserved = true;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
        expect(retryObserved).toBe(true);

        restartApplicationCluster();
        runDockerCompose(['up', '-d', 'localstack']);
        runDockerCompose(['run', '--rm', 'queue-init']);
        expect(await waitForApiInstances()).toBe(true);

        const placeholders = walletIds.map(() => '?').join(',');
        const deadline = Date.now() + 45_000;
        let expectedEventIds = new Set<string>();
        while (Date.now() < deadline) {
          const rows: Array<{ id: string; published_at: Date | null }> = await database.em.getConnection().execute(
            `select id, published_at from outbox_messages
             where event_type = 'WalletBalanceChanged' and aggregate_id in (${placeholders})`,
            walletIds,
          );
          if (rows.length === walletIds.length && rows.every((row) => row.published_at !== null)) {
            expectedEventIds = new Set(rows.map((row) => row.id));
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
        expect(expectedEventIds.size).toBe(walletIds.length);

        const deliveredEventIds = new Set<string>();
        const deliveryDeadline = Date.now() + 45_000;
        while (Date.now() < deliveryDeadline && deliveredEventIds.size < expectedEventIds.size) {
          const batch = await sqs.send(new ReceiveMessageCommand({
            QueueUrl: eventsQueueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 1,
          }));
          for (const message of batch.Messages ?? []) {
            if (!message.Body || !message.ReceiptHandle) continue;
            try {
              const event = JSON.parse(message.Body) as { eventId?: string };
              if (event.eventId && expectedEventIds.has(event.eventId)) deliveredEventIds.add(event.eventId);
            } finally {
              await sqs.send(new DeleteMessageCommand({ QueueUrl: eventsQueueUrl, ReceiptHandle: message.ReceiptHandle }));
            }
          }
        }
        expect(deliveredEventIds).toEqual(expectedEventIds);
      } finally {
        runDockerCompose(['up', '-d', 'localstack']);
        runDockerCompose(['run', '--rm', 'queue-init']);
        await waitForApiInstances();
      }
    }, 120_000);

    // Stops PostgreSQL to force visibility-based retry, then restores it and checks one debit.
    test('retries transient database failures and processes the same queue message after recovery', async () => {
      const playerId = randomUUID();
      const walletResponse = await fetch(`${apiUrl}/wallets`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ playerId, initialBalance: { amount: '50.00', currency: 'BRL' } }),
      });
      const wallet = await walletResponse.json() as { id: string };
      const externalTransactionId = randomUUID();
      const messageId = randomUUID();
      const data = {
        providerId: 'transient-provider',
        externalTransactionId,
        idempotencyKey: `transient-provider:${externalTransactionId}`,
        playerId,
        walletId: wallet.id,
        roundId: randomUUID(),
        gameId: 'integration-game',
        kind: 'BET',
        money: { amount: '5.00', currency: 'BRL' },
      };
      const retriesBefore = await readMetricTotal('wager_retries_total', 'sqs');

      runDockerCompose(['stop', '--timeout', '5', 'postgres']);
      try {
        await sqs.send(new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
          MessageGroupId: wallet.id,
          MessageDeduplicationId: randomUUID(),
        }));

        const retryDeadline = Date.now() + 20_000;
        let retryObserved = false;
        while (Date.now() < retryDeadline) {
          if (await readMetricTotal('wager_retries_total', 'sqs') > retriesBefore) {
            retryObserved = true;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
        expect(retryObserved).toBe(true);
      } finally {
        runDockerCompose(['up', '-d', 'postgres']);
      }

      expect(await waitForApiInstances()).toBe(true);
      const deadline = Date.now() + 35_000;
      let transactionStatus: string | undefined;
      while (Date.now() < deadline) {
        const response = await fetch(`${apiUrl}/providers/transient-provider/wagering/transactions/${externalTransactionId}`);
        if (response.ok) {
          transactionStatus = (await response.json() as { status: string }).status;
          if (transactionStatus === 'PROCESSED') break;
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
      expect(transactionStatus).toBe('PROCESSED');
      const walletAfter = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
        balance: { amount: string };
      };
      expect(walletAfter.balance.amount).toBe('45.00');
    }, 90_000);

    // Uses a test-only delay to kill all consumers after commit but before DeleteMessage.
    test('deduplicates a message after SIGKILL between commit and ack', async () => {
      const previousVisibility = process.env.WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS;
      const previousAckDelay = process.env.SQS_BEFORE_ACK_DELAY_MS;
      try {
        process.env.WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS = '5';
        process.env.SQS_BEFORE_ACK_DELAY_MS = '10000';
        runDockerCompose(['up', '-d', '--force-recreate', 'app', 'app-2', 'app-3']);
        expect(await waitForApiInstances()).toBe(true);

        const playerId = randomUUID();
        const walletResponse = await fetch(`${apiUrl}/wallets`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ playerId, initialBalance: { amount: '50.00', currency: 'BRL' } }),
        });
        const wallet = await walletResponse.json() as { id: string };
        const externalTransactionId = randomUUID();
        const messageId = randomUUID();
        const data = {
          providerId: 'commit-before-ack-provider',
          externalTransactionId,
          idempotencyKey: `commit-before-ack-provider:${externalTransactionId}`,
          playerId,
          walletId: wallet.id,
          roundId: randomUUID(),
          gameId: 'integration-game',
          kind: 'BET',
          money: { amount: '5.00', currency: 'BRL' },
        };
        await sqs.send(new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
          MessageGroupId: wallet.id,
          MessageDeduplicationId: randomUUID(),
        }));

        const commitDeadline = Date.now() + 60_000;
        let committed = false;
        while (Date.now() < commitDeadline) {
          const response = await fetch(
            `${apiUrl}/providers/commit-before-ack-provider/wagering/transactions/${externalTransactionId}`,
          );
          if (response.ok && (await response.json() as { status: string }).status === 'PROCESSED') {
            committed = true;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        expect(committed).toBe(true);

        runDockerCompose(['kill', '--signal', 'SIGKILL', 'app', 'app-2', 'app-3']);
        delete process.env.WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS;
        delete process.env.SQS_BEFORE_ACK_DELAY_MS;
        runDockerCompose(['up', '-d', '--force-recreate', 'app', 'app-2', 'app-3']);
        expect(await waitForApiInstances()).toBe(true);

        const duplicatesBefore = await readMetricTotal('wager_duplicates_total', 'sqs');
        const redeliveryDeadline = Date.now() + 25_000;
        let redeliveryObserved = false;
        while (Date.now() < redeliveryDeadline) {
          if (await readMetricTotal('wager_duplicates_total', 'sqs') > duplicatesBefore) {
            redeliveryObserved = true;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
        expect(redeliveryObserved).toBe(true);
        const walletAfter = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
          balance: { amount: string };
        };
        expect(walletAfter.balance.amount).toBe('45.00');
        const ledger = await fetch(`${apiUrl}/wallets/${wallet.id}/ledger?limit=100`)
          .then((response) => response.json()) as { entries: Array<{ direction: string }> };
        expect(ledger.entries.filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
        expect(ledger.entries).toHaveLength(2);
      } finally {
        if (previousVisibility === undefined) delete process.env.WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS;
        else process.env.WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS = previousVisibility;
        if (previousAckDelay === undefined) delete process.env.SQS_BEFORE_ACK_DELAY_MS;
        else process.env.SQS_BEFORE_ACK_DELAY_MS = previousAckDelay;
        runDockerCompose(['up', '-d', '--force-recreate', 'app', 'app-2', 'app-3']);
        await waitForApiInstances();
      }
    }, 120_000);

    // Verifies shutdown visibility release plus durable reference and queue recovery on restart.
    test('returns unstarted messages on shutdown and recovers queued and pending work after restart', async () => {
      const pendingPlayerId = randomUUID();
      const pendingWalletResponse = await fetch(`${apiUrl}/wallets`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ playerId: pendingPlayerId, initialBalance: { amount: '100.00', currency: 'BRL' } }),
      });
      const pendingWallet = await pendingWalletResponse.json() as { id: string };
      const roundId = randomUUID();
      const futureBetId = randomUUID();
      const rollbackId = randomUUID();
      const rollback = {
        providerId: 'restart-provider',
        externalTransactionId: rollbackId,
        playerId: pendingPlayerId,
        walletId: pendingWallet.id,
        roundId,
        gameId: 'integration-game',
        kind: 'ROLLBACK',
        referenceExternalTransactionId: futureBetId,
        money: { amount: '25.00', currency: 'BRL' },
      };
      const pendingResponse = await fetch(`${apiUrl}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': `restart-provider:${rollbackId}` },
        body: JSON.stringify(rollback),
      });
      expect(pendingResponse.status).toBe(202);
      const pending = await pendingResponse.json() as { transactionId: string };

      const queuedWork = await Promise.all(Array.from({ length: 18 }, async (_, index) => {
        const playerId = randomUUID();
        const targetApiUrl = apiUrls[index % apiUrls.length]!;
        const response = await fetch(`${targetApiUrl}/wallets`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ playerId, initialBalance: { amount: '50.00', currency: 'BRL' } }),
        });
        const wallet = await response.json() as { id: string };
        const externalTransactionId = randomUUID();
        const data = {
          providerId: 'restart-provider',
          externalTransactionId,
          idempotencyKey: `restart-provider:${externalTransactionId}`,
          playerId,
          walletId: wallet.id,
          roundId: randomUUID(),
          gameId: 'integration-game',
          kind: 'BET',
          money: { amount: '5.00', currency: 'BRL' },
        };
        await sqs.send(new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify({ messageId: randomUUID(), type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
          MessageGroupId: wallet.id,
          MessageDeduplicationId: randomUUID(),
        }));
        return { walletId: wallet.id, externalTransactionId };
      }));

      restartApplicationCluster();
      expect(await waitForApiInstances()).toBe(true);

      const referenceBet = {
        providerId: 'restart-provider',
        externalTransactionId: futureBetId,
        playerId: pendingPlayerId,
        walletId: pendingWallet.id,
        roundId,
        gameId: 'integration-game',
        kind: 'BET',
        money: { amount: '25.00', currency: 'BRL' },
      };
      const referenceResponse = await fetch(`${apiUrl}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': `restart-provider:${futureBetId}` },
        body: JSON.stringify(referenceBet),
      });
      expect(referenceResponse.status).toBe(200);

      const deadline = Date.now() + 45_000;
      let allRecovered = false;
      while (Date.now() < deadline) {
        const [pendingResponseAfterRestart, ...queuedResponses] = await Promise.all([
          fetch(`${apiUrl}/wagering/transactions/${pending.transactionId}`),
          ...queuedWork.map((work) => fetch(
            `${apiUrl}/providers/restart-provider/wagering/transactions/${work.externalTransactionId}`,
          )),
        ]);
        const pendingStatus = pendingResponseAfterRestart?.ok
          ? (await pendingResponseAfterRestart.json() as { status: string }).status
          : undefined;
        const queuedStatuses = await Promise.all(queuedResponses.map(async (response) => {
          if (!response.ok) return undefined;
          return (await response.json() as { status: string }).status;
        }));
        if (pendingStatus === 'PROCESSED' && queuedStatuses.every((status) => status === 'PROCESSED')) {
          allRecovered = true;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
      expect(allRecovered).toBe(true);

      const balances = await Promise.all(queuedWork.map(async (work) => {
        const wallet = await fetch(`${apiUrl}/wallets/${work.walletId}`).then((response) => response.json()) as {
          balance: { amount: string };
        };
        return wallet.balance.amount;
      }));
      expect(balances.every((balance) => balance === '45.00')).toBe(true);
      const pendingWalletAfter = await fetch(`${apiUrl}/wallets/${pendingWallet.id}`).then((response) => response.json()) as {
        balance: { amount: string };
      };
      expect(pendingWalletAfter.balance.amount).toBe('100.00');
    }, 90_000);
  }

  // Sends the real FIFO request, repeats its inbox identity, and observes its outbox event.
  test('processes a queue request through the persistent inbox and shared use case', async () => {
    const playerId = randomUUID();
    const walletResponse = await fetch(`${apiUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: '50.00', currency: 'BRL' } }),
    });
    expect(walletResponse.status).toBe(201);
    const wallet = await walletResponse.json() as { id: string };
    const externalTransactionId = randomUUID();
    const messageId = randomUUID();
    const data = {
      providerId: 'integration-provider',
      externalTransactionId,
      idempotencyKey: `integration-provider:${externalTransactionId}`,
      playerId,
      walletId: wallet.id,
      roundId: randomUUID(),
      gameId: 'integration-game',
      kind: 'BET',
      money: { amount: '5.00', currency: 'BRL' },
    };
    await sqs.send(new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
      MessageGroupId: wallet.id,
      MessageDeduplicationId: randomUUID(),
    }));

    const deadline = Date.now() + 60_000;
    let status: unknown;
    let transactionId: string | undefined;
    while (Date.now() < deadline) {
      const response = await fetch(`${apiUrl}/providers/integration-provider/wagering/transactions/${externalTransactionId}`);
      if (response.ok) {
        const transaction = await response.json() as { status: string; transactionId: string };
        status = transaction.status;
        transactionId = transaction.transactionId;
        if (status === 'PROCESSED') break;
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    expect(status).toBe('PROCESSED');
    const currentWallet = await fetch(`${apiUrl}/wallets/${wallet.id}`).then((response) => response.json()) as {
      balance: { amount: string };
    };
    expect(currentWallet.balance.amount).toBe('45.00');

    const duplicateMetric = async () => {
      const values = await Promise.all(apiUrls.map(async (targetApiUrl) => {
        const text = await fetch(`${targetApiUrl}/metrics`).then((response) => response.text());
        const value = text.match(/^wager_duplicates_total\{source="sqs"\}\s+([\d.]+)/m)?.[1];
        return Number(value ?? 0);
      }));
      return values.reduce((total, value) => total + value, 0);
    };
    const duplicatesBefore = await duplicateMetric();
    await sqs.send(new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
      MessageGroupId: wallet.id,
      MessageDeduplicationId: randomUUID(),
    }));
    const duplicateDeadline = Date.now() + 10_000;
    let duplicateObserved = false;
    while (Date.now() < duplicateDeadline) {
      if (await duplicateMetric() > duplicatesBefore) {
        duplicateObserved = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    expect(duplicateObserved).toBe(true);

    const ledger = await fetch(`${apiUrl}/wallets/${wallet.id}/ledger?limit=100`)
      .then((response) => response.json()) as { entries: unknown[] };
    expect(ledger.entries).toHaveLength(2);

    const eventDeadline = Date.now() + 30_000;
    let processedEventObserved = false;
    while (Date.now() < eventDeadline && !processedEventObserved) {
      const batch = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: eventsQueueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
      }));
      for (const eventMessage of batch.Messages ?? []) {
        if (!eventMessage.Body || !eventMessage.ReceiptHandle) continue;
        try {
          const event = JSON.parse(eventMessage.Body) as {
            eventType?: string;
            data?: { transactionId?: string };
          };
          if (event.eventType === 'WagerTransactionProcessed' && event.data?.transactionId === transactionId) {
            processedEventObserved = true;
          }
        } finally {
          await sqs.send(new DeleteMessageCommand({ QueueUrl: eventsQueueUrl, ReceiptHandle: eventMessage.ReceiptHandle }));
        }
      }
    }
    expect(processedEventObserved).toBe(true);
  }, 75_000);
});
