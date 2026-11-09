// Unit coverage for legal creation rules, terminal transitions, and reversal direction.
import { describe, expect, test } from 'bun:test';
import { Money } from './money.js';
import {
  InvalidTransactionStateError,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from './wager-transaction.js';
import { LedgerDirection } from './wallet-ledger-entry.js';

const create = (kind = WagerTransactionKind.Bet, referenceExternalTransactionId?: string) => WagerTransaction.create({
  id: 't1', providerId: 'provider', externalTransactionId: 'external', idempotencyKey: 'key',
  payloadHash: 'hash', walletId: 'w1', playerId: 'p1', roundId: 'r1', gameId: 'g1', kind,
  money: Money.from({ amount: '5.00', currency: 'BRL' }),
  ...(referenceExternalTransactionId ? { referenceExternalTransactionId } : {}),
});

describe('WagerTransaction', () => {
  test('requires references for reversals and rejects public OPENING creation', () => {
    expect(() => create(WagerTransactionKind.Refund)).toThrow(InvalidTransactionStateError);
    expect(() => create(WagerTransactionKind.Opening)).toThrow('internal');
  });

  test('models pending references and terminal state transitions', () => {
    const transaction = create(WagerTransactionKind.Rollback, 'bet-1');
    transaction.markPendingReference();
    expect(transaction.status).toBe(WagerTransactionStatus.PendingReference);
    transaction.reject('REFERENCE_NOT_FOUND');
    expect(transaction.isTerminal()).toBe(true);
    expect(() => transaction.markProcessed(undefined, new Date())).toThrow(InvalidTransactionStateError);
  });

  test('computes reversal direction from the referenced transaction', () => {
    const bet = create();
    const win = create(WagerTransactionKind.Win);
    const rollback = create(WagerTransactionKind.Rollback, 'win-1');
    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(rollback.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
  });
});