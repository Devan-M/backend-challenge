import { Money } from './money.js';

/** Persistence snapshot accepted by the rehydration factory. */
export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

/** Result of a balance mutation, used to create its matching ledger entry. */
export interface WalletMovement {
  balanceBefore: Money;
  balanceAfter: Money;
  version: number;
}

/** Raised when a wallet movement violates its balance or currency invariant. */
export class WalletError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WalletError';
  }
}

/** Aggregate root for one player's balance in one currency. */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /** Opens a new wallet at version 1; the caller persists its opening ledger entry atomically. */
  static open(props: { id: string; playerId: string; initialBalance: Money; now?: Date }): Wallet {
    if (props.initialBalance.isNegative()) throw new WalletError('Initial balance cannot be negative');
    const now = props.now ?? new Date();
    return new Wallet(props.id, props.playerId, props.initialBalance.currency, props.initialBalance, 1, now, now);
  }

  /** Reconstructs persisted state without replaying domain transitions. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  /** Current immutable Money value held by the aggregate. */
  get balance(): Money {
    return this._balance;
  }

  /** Version increments only when debit or credit changes the balance. */
  get version(): number {
    return this._version;
  }

  /** Timestamp of the latest balance mutation. */
  get updatedAt(): Date {
    return this._updatedAt;
  }

  /** Debits only positive, same-currency amounts; Money rejects overdrafts before state changes. */
  debit(amount: Money, at: Date = new Date()): WalletMovement {
    this.assertSameCurrency(amount);
    if (!amount.isPositive()) throw new WalletError('Movement amount must be positive');

    const balanceBefore = this._balance;
    this._balance = this._balance.subtract(amount);
    this.recordChange(at);
    return { balanceBefore, balanceAfter: this._balance, version: this._version };
  }

  /** Credits only positive amounts and returns the before/after pair for the ledger. */
  credit(amount: Money, at: Date = new Date()): WalletMovement {
    this.assertSameCurrency(amount);
    if (!amount.isPositive()) throw new WalletError('Movement amount must be positive');

    const balanceBefore = this._balance;
    this._balance = this._balance.add(amount);
    this.recordChange(at);
    return { balanceBefore, balanceAfter: this._balance, version: this._version };
  }

  /** Version changes track balance mutations, not reads or transaction status changes. */
  private recordChange(at: Date): void {
    this._version += 1;
    this._updatedAt = at;
  }

  /** Keeps every wallet operation in the aggregate's configured currency. */
  private assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new WalletError(`Currency mismatch: wallet uses ${this.currency}, operation uses ${money.currency}`);
    }
  }
}