// Unit coverage for exact decimal parsing, currency safety, immutability, and formatting.
import { describe, expect, test } from 'bun:test';
import { Money, MoneyError } from './money.js';

describe('Money', () => {
  test('uses exact minor units and serializes with a fixed scale', () => {
    const amount = Money.from({ amount: '999999999999999999.99', currency: 'brl' });

    expect(amount.add(Money.from({ amount: '0.01', currency: 'BRL' })).toString())
      .toBe('1000000000000000000.00');
    expect(amount.toJSON()).toEqual({ amount: '999999999999999999.99', currency: 'BRL' });
  });

  test.each(['', '1', '1.2', '01.00', '-1.00', '1.000', '1e2', 'NaN', 'Infinity'])(
    'rejects invalid amount %s',
    (amount) => {
      expect(() => Money.from({ amount, currency: 'BRL' })).toThrow(MoneyError);
    },
  );

  test('rejects mixed currencies and negative subtraction results', () => {
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    const usd = Money.from({ amount: '1.00', currency: 'USD' });

    expect(() => brl.add(usd)).toThrow('Currency mismatch');
    expect(() => brl.equals(usd)).toThrow('Currency mismatch');
    expect(() => Money.zero('BRL').subtract(brl)).toThrow('cannot be negative');
  });

  test('keeps negate immutable and supports zero checks', () => {
    const original = Money.from({ amount: '2.30', currency: 'BRL' });

    expect(original.negate().toString()).toBe('-2.30');
    expect(original.toString()).toBe('2.30');
    expect(Money.zero('BRL').isZero()).toBe(true);
  });
});