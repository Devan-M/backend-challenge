// Unit coverage for aggregate versioning, overdraft protection, and ledger equations.
import { describe, expect, test } from 'bun:test';
import { Money } from './money.js';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry.js';
import { Wallet, WalletError } from './wallet.js';

const money = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Wallet and ledger', () => {
  test('tracks movements and increments version only when balance changes', () => {
    const wallet = Wallet.open({ id: 'w1', playerId: 'p1', initialBalance: money('10.00') });
    expect(wallet.version).toBe(1);

    const movement = wallet.debit(money('3.25'), new Date('2026-01-01T00:00:00.000Z'));
    expect(movement.balanceBefore.toString()).toBe('10.00');
    expect(movement.balanceAfter.toString()).toBe('6.75');
    expect(wallet.version).toBe(2);
  });

  test('rejects overdrafts, zero movements, and currency mismatches', () => {
    const wallet = Wallet.open({ id: 'w1', playerId: 'p1', initialBalance: money('1.00') });
    expect(() => wallet.debit(money('1.01'))).toThrow();
    expect(() => wallet.credit(money('0.00'))).toThrow(WalletError);
    expect(() => wallet.credit(Money.from({ amount: '1.00', currency: 'USD' }))).toThrow(WalletError);
    expect(wallet.balance.toString()).toBe('1.00');
  });

  test('ledger factory checks debit and credit balance arithmetic', () => {
    const entry = WalletLedgerEntry.create({
      id: 'l1', walletId: 'w1', transactionId: 't1', direction: LedgerDirection.Debit,
      money: money('2.00'), balanceBefore: money('5.00'), balanceAfter: money('3.00'), createdAt: new Date(),
    });
    expect(entry.isBalanced()).toBe(true);
    expect(() => WalletLedgerEntry.create({ ...entry, balanceAfter: money('4.00') })).toThrow();
  });
});