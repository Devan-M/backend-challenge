import { Money } from './money.js';

/** Direction determines the exact balance equation for one immutable ledger row. */
export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

/** Values reconstructed from storage; create() additionally checks arithmetic. */
export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

/** Raised when a new ledger entry is not a valid positive balance movement. */
export class LedgerEntryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'LedgerEntryError';
  }
}

/** Immutable audit record pairing one transaction with its wallet balance change. */
export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  /** Validates arithmetic before the entry can be persisted. */
  static create(state: LedgerEntryState): WalletLedgerEntry {
    const entry = new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
    if (!entry.isBalanced()) throw new LedgerEntryError('Ledger entry balance arithmetic is invalid');
    return entry;
  }

  /** Rehydrates trusted historical data without mutating or rewriting the ledger. */
  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  /** Checks balanceBefore +/- amount equals balanceAfter using exact Money operations. */
  isBalanced(): boolean {
    if (!this.money.isPositive()) return false;
    const expected = this.direction === LedgerDirection.Credit
      ? this.balanceBefore.add(this.money)
      : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }
}