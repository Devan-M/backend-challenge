import { randomUUID } from 'node:crypto';

/** Correlation metadata shared by all integration events emitted by a use case. */
export interface EventContext {
  correlationId: string;
  causationId?: string;
  occurredAt?: Date;
}

/** Construction data for a versioned event envelope. */
export interface IntegrationEventProps<T> extends EventContext {
  eventId?: string;
  aggregateId: string;
  data: T;
}

/** Base envelope that supplies stable identity and JSON-safe event metadata. */
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId: string | undefined;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  /** Freezes a shallow event payload and fills stable defaults for event identity/time. */
  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId ?? randomUUID();
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt ?? new Date();
    this.data = Object.freeze({ ...props.data });
  }

  /** Produces the versioned payload stored atomically in the transactional outbox. */
  toJSON(): Record<string, unknown> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

/** Event emitted when any wager reaches PROCESSED, including LOSS. */
export class WagerTransactionProcessed extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;
  static from(props: IntegrationEventProps<Record<string, unknown>>): WagerTransactionProcessed {
    return new WagerTransactionProcessed(props);
  }
  private constructor(props: IntegrationEventProps<Record<string, unknown>>) { super(props); }
}

/** Event emitted when a business rule rejects a wager. */
export class WagerTransactionRejected extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;
  static from(props: IntegrationEventProps<Record<string, unknown>>): WagerTransactionRejected {
    return new WagerTransactionRejected(props);
  }
  private constructor(props: IntegrationEventProps<Record<string, unknown>>) { super(props); }
}

/** Event emitted when a requested reversal must wait for its reference. */
export class WagerTransactionPendingReference extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;
  static from(props: IntegrationEventProps<Record<string, unknown>>): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(props);
  }
  private constructor(props: IntegrationEventProps<Record<string, unknown>>) { super(props); }
}

/** Event emitted only when the materialized wallet balance changes. */
export class WalletBalanceChanged extends IntegrationEvent<Record<string, unknown>> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;
  static from(props: IntegrationEventProps<Record<string, unknown>>): WalletBalanceChanged {
    return new WalletBalanceChanged(props);
  }
  private constructor(props: IntegrationEventProps<Record<string, unknown>>) { super(props); }
}