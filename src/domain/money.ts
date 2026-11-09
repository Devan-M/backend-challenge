/** JSON contract for money; decimal text prevents precision loss at API boundaries. */
export interface MoneyProps {
  amount: string;
  currency: string;
}

/** Raised when an amount, scale, or currency operation violates Money rules. */
export class MoneyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MoneyError';
  }
}

/** Immutable exact-precision amount stored as signed minor units. */
export class Money {
  private constructor(
    private readonly minorUnits: bigint,
    public readonly currency: string,
  ) {}

  /** Parses a non-negative, fixed-two-decimal API value without using floating point. */
  static from(props: MoneyProps): Money {
    const currency = props.currency.trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(currency)) {
      throw new MoneyError('Currency must be a three-letter ISO code');
    }
    if (!/^(0|[1-9]\d*)\.\d{2}$/.test(props.amount)) {
      throw new MoneyError('Amount must be a non-negative decimal with exactly two places');
    }

    const [whole, fraction] = props.amount.split('.');
    return new Money(BigInt(whole!) * 100n + BigInt(fraction!), currency);
  }

  /** Creates a currency-specific zero value for balances and ledger calculations. */
  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }

  /** Adds amounts only when both values use the same currency. */
  /** Returns a new exact sum, preserving the shared currency. */
  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits + other.minorUnits, this.currency);
  }

  /** Subtracts without allowing a negative result; signed values are created with negate(). */
  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    if (this.minorUnits < other.minorUnits) {
      throw new MoneyError('Money result cannot be negative');
    }
    return new Money(this.minorUnits - other.minorUnits, this.currency);
  }

  /** Returns the signed inverse used for reconciliation differences and rollback math. */
  negate(): Money {
    if (this.minorUnits === 0n) return this;
    return new Money(-this.minorUnits, this.currency);
  }

  /** Reports whether the amount has no minor units. */
  isZero(): boolean {
    return this.minorUnits === 0n;
  }

  /** Reports whether the signed amount is above zero. */
  isPositive(): boolean {
    return this.minorUnits > 0n;
  }

  /** Reports whether the signed amount is below zero. */
  isNegative(): boolean {
    return this.minorUnits < 0n;
  }

  /** Compares values after enforcing currency compatibility. */
  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits < other.minorUnits;
  }

  /** Compares exact minor units and rejects cross-currency comparisons. */
  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits === other.minorUnits;
  }

  /** Serializes using the API's fixed-scale decimal string contract. */
  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  /** Formats signed minor units without converting through a floating-point number. */
  toString(): string {
    const absoluteUnits = this.minorUnits < 0n ? -this.minorUnits : this.minorUnits;
    const whole = absoluteUnits / 100n;
    const fraction = (absoluteUnits % 100n).toString().padStart(2, '0');
    return `${this.minorUnits < 0n ? '-' : ''}${whole}.${fraction}`;
  }

  /** Prevents arithmetic or comparison from silently mixing currencies. */
  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new MoneyError(`Currency mismatch: ${this.currency} and ${other.currency}`);
    }
  }
}