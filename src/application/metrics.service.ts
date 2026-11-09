import { Injectable } from '@nestjs/common';
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';

/** Owns per-process Prometheus instruments used by HTTP handlers and workers. */
@Injectable()
export class MetricsService {
  private readonly registry = new Registry();
  readonly transactionStatuses: Counter<'status'>;
  readonly duplicates: Counter<string>;
  readonly retries: Counter<string>;
  readonly deadLetters: Counter<string>;
  readonly deadLetterQueueMessages: Gauge<string>;
  readonly lockConflicts: Counter<string>;
  readonly reconciliationDivergences: Counter<string>;
  readonly outboxLagSeconds: Gauge<string>;
  readonly processingDuration: Histogram<string>;

  /** Registers bounded-cardinality application metrics plus standard process metrics. */
  constructor() {
    collectDefaultMetrics({ register: this.registry });
    this.transactionStatuses = new Counter({
      name: 'wager_transactions_total', help: 'Wager transaction results by status', labelNames: ['status'], registers: [this.registry],
    });
    this.duplicates = new Counter({
      name: 'wager_duplicates_total', help: 'Idempotent replays and inbox duplicates', labelNames: ['source'], registers: [this.registry],
    });
    this.retries = new Counter({
      name: 'wager_retries_total', help: 'Transient processing retries', labelNames: ['source'], registers: [this.registry],
    });
    this.deadLetters = new Counter({
      name: 'wager_dead_letters_total', help: 'Messages sent to a dead-letter queue', labelNames: ['source'], registers: [this.registry],
    });
    this.deadLetterQueueMessages = new Gauge({
      name: 'wager_dlq_messages', help: 'Visible and in-flight messages currently in the dead-letter queue', registers: [this.registry],
    });
    this.lockConflicts = new Counter({
      name: 'wager_lock_conflicts_total', help: 'Database lock conflicts', registers: [this.registry],
    });
    this.reconciliationDivergences = new Counter({
      name: 'wallet_reconciliation_divergences_total',
      help: 'Wallet reconciliation mismatches',
      registers: [this.registry],
    });
    this.outboxLagSeconds = new Gauge({
      name: 'wager_outbox_lag_seconds', help: 'Age of the oldest pending outbox event', registers: [this.registry],
    });
    this.processingDuration = new Histogram({
      name: 'wager_processing_duration_seconds', help: 'Wager processing duration',
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5], registers: [this.registry],
    });
  }

  /** Renders all registered counters, gauges, and histograms in Prometheus text format. */
  metrics(): Promise<string> {
    return this.registry.metrics();
  }

  /** Content type advertised by prom-client for the generated exposition format. */
  contentType(): string {
    return this.registry.contentType;
  }
}