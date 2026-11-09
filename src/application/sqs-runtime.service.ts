import { ChangeMessageVisibilityCommand, DeleteMessageCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { EntityManager, MikroORM } from '@mikro-orm/core';
import { InboxContext, hashCanonicalPayload, PermanentInfrastructureError, WageringService } from './wagering.service.js';
import { MetricsService } from './metrics.service.js';

/** Database projection claimed by one outbox publisher lease. */
interface OutboxRow {
  id: string;
  aggregate_id: string;
  payload: unknown;
  occurred_at: Date;
  attempts: number;
}

/** Transport envelope accepted by the wager request queue. */
interface QueueEnvelope {
  messageId?: unknown;
  type?: unknown;
  data?: unknown;
}

/** Owns SQS polling, persistent inbox use, DLQ routing, and outbox publication. */
@Injectable()
export class SqsRuntimeService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SqsRuntimeService.name);
  private readonly client = new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    ...(process.env.SQS_ENDPOINT ? { endpoint: process.env.SQS_ENDPOINT } : {}),
    ...(process.env.SQS_ENDPOINT ? {
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
    } : {}),
  });
  private readonly inputQueueUrl = process.env.WAGER_QUEUE_URL;
  private readonly deadLetterQueueUrl = process.env.WAGER_DLQ_URL;
  private readonly eventsQueueUrl = process.env.EVENTS_QUEUE_URL;
  private readonly messageVisibilityTimeout = positiveInteger(process.env.WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS, 45);
  private readonly beforeAckDelayMs = nonNegativeInteger(process.env.SQS_BEFORE_ACK_DELAY_MS, 0);
  private readonly receiveAbortController = new AbortController();
  private readonly receivedMessageHandles = new Set<string>();
  private readonly processingMessageHandles = new Set<string>();
  private stopping = false;
  private consumerTask: Promise<void> | undefined;
  private outboxInterval: ReturnType<typeof setInterval> | undefined;
  private outboxTask: Promise<void> | undefined;
  private pendingInterval: ReturnType<typeof setInterval> | undefined;
  private pendingTask: Promise<void> | undefined;
  private queueMetricsInterval: ReturnType<typeof setInterval> | undefined;
  private queueMetricsTask: Promise<void> | undefined;

  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    private readonly wagering: WageringService,
    private readonly metrics: MetricsService,
  ) {}

  /** Starts independent reference, DLQ-metric, input-queue, and outbox loops. */
  onModuleInit(): void {
    this.pendingInterval = setInterval(() => this.schedulePendingReferences(), 1_000);
    this.pendingInterval.unref();
    this.schedulePendingReferences();
    this.queueMetricsInterval = setInterval(() => this.refreshDeadLetterMetric(), 5_000);
    this.queueMetricsInterval.unref();
    this.refreshDeadLetterMetric();
    if (this.inputQueueUrl) this.consumerTask = this.consume();
    if (this.eventsQueueUrl) {
      this.outboxInterval = setInterval(() => this.scheduleOutbox(), 1_000);
      this.outboxInterval.unref();
      this.scheduleOutbox();
    }
  }

  /** Stops polling, returns unstarted receipts, and waits for active work before closing SQS. */
  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    this.receiveAbortController.abort();
    if (this.outboxInterval) clearInterval(this.outboxInterval);
    if (this.pendingInterval) clearInterval(this.pendingInterval);
    if (this.queueMetricsInterval) clearInterval(this.queueMetricsInterval);
    await this.returnUnprocessedMessages();
    await this.consumerTask;
    await this.outboxTask;
    await this.pendingTask;
    await this.queueMetricsTask;
    this.client.destroy();
  }

  /** Readiness requires both the input queue and the integration-event queue to exist. */
  async isReady(): Promise<boolean> {
    if (!this.inputQueueUrl || !this.eventsQueueUrl) return false;
    try {
      await Promise.all([this.inputQueueUrl, this.eventsQueueUrl].map((QueueUrl) => this.client.send(
        new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }),
      )));
      return true;
    } catch {
      return false;
    }
  }

  /** Long-polls input messages and handles each receipt serially within this process. */
  private async consume(): Promise<void> {
    while (!this.stopping && this.inputQueueUrl) {
      try {
        const response = await this.client.send(new ReceiveMessageCommand({
          QueueUrl: this.inputQueueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 20,
          VisibilityTimeout: this.messageVisibilityTimeout,
          MessageSystemAttributeNames: ['ApproximateReceiveCount'],
        }), { abortSignal: this.receiveAbortController.signal });
        const messages = response.Messages ?? [];
        // Register every receipt before processing so shutdown can release unstarted messages.
        for (const message of messages) {
          if (message.ReceiptHandle) this.receivedMessageHandles.add(message.ReceiptHandle);
        }
        for (const message of messages) {
          const receiptHandle = message.ReceiptHandle;
          if (!receiptHandle) continue;
          if (this.stopping) {
            await this.returnMessageVisibility(receiptHandle);
            this.receivedMessageHandles.delete(receiptHandle);
            continue;
          }
          this.processingMessageHandles.add(receiptHandle);
          try {
            await this.processMessage(message);
          } finally {
            this.processingMessageHandles.delete(receiptHandle);
            this.receivedMessageHandles.delete(receiptHandle);
          }
        }
      } catch (error) {
        if (this.stopping) break;
        this.log('queue_receive_failed', { error: errorName(error) });
        await this.retryDelay(1);
      }
    }
  }

  /** Validates the envelope, delegates to the shared use case, then acknowledges only after commit. */
  private async processMessage(message: {
    Body?: string | undefined;
    MessageId?: string | undefined;
    ReceiptHandle?: string | undefined;
    Attributes?: Record<string, string> | undefined;
  }): Promise<void> {
    if (!message.ReceiptHandle) return;
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
    try {
      const envelope: unknown = JSON.parse(message.Body ?? '');
      if (!isRecord(envelope) || envelope.type !== 'WagerTransactionRequested' || !isRecord(envelope.data)) {
        throw new PermanentMessageError('Invalid wager request envelope', 'INVALID_ENVELOPE');
      }
      const data = envelope.data;
      const providerId = readText(data.providerId);
      const externalTransactionId = readText(data.externalTransactionId);
      const walletId = readText(data.walletId);
      const messageId = typeof envelope.messageId === 'string' ? envelope.messageId : message.MessageId;
      const idempotencyKey = typeof data.idempotencyKey === 'string'
        ? data.idempotencyKey
        : `${providerId}:${externalTransactionId}`;
      if (!messageId) throw new PermanentMessageError('Message ID is missing');
      // The inbox key and hash make a broker redelivery safe across all app instances.
      const inbox: InboxContext = {
        consumerName: 'wager-transactions-v1',
        messageId,
        payloadHash: hashCanonicalPayload(data),
      };
      const endTimer = this.metrics.processingDuration.startTimer();
      const result = await this.wagering.submit(idempotencyKey, data, inbox);
      endTimer();
      this.metrics.transactionStatuses.inc({ status: result.status });
      if (result.idempotentReplay) this.metrics.duplicates.inc({ source: 'sqs' });
      if (this.beforeAckDelayMs > 0) await this.retryDelay(this.beforeAckDelayMs / 1_000);
      await this.client.send(new DeleteMessageCommand({ QueueUrl: this.inputQueueUrl, ReceiptHandle: message.ReceiptHandle }));
      this.log('queue_message_acknowledged', {
        messageId, correlationId: result.transactionId, transactionId: result.transactionId,
        walletId, providerId, status: result.status,
      });
    } catch (error) {
      // Business/permanent failures are terminal; transient failures retain the message for redelivery.
      if (error instanceof PermanentMessageError || error instanceof PermanentInfrastructureError || isClientError(error)) {
        if (!this.deadLetterQueueUrl) {
          this.metrics.retries.inc({ source: 'sqs' });
          await this.client.send(new ChangeMessageVisibilityCommand({
            QueueUrl: this.inputQueueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: Math.min(900, 2 ** Math.min(receiveCount, 8)),
          }));
          this.log('queue_message_retry_scheduled', { messageId: message.MessageId, reason: 'dlq_not_configured' });
          return;
        }
        const failureCode = error instanceof PermanentMessageError
          ? error.failureCode
          : error instanceof PermanentInfrastructureError ? 'PERMANENT_INFRASTRUCTURE' : 'PERMANENT_CLIENT_ERROR';
        await this.sendToDeadLetter(message, failureCode);
        await this.client.send(new DeleteMessageCommand({ QueueUrl: this.inputQueueUrl, ReceiptHandle: message.ReceiptHandle }));
        this.metrics.deadLetters.inc({ source: 'sqs' });
        this.refreshDeadLetterMetric();
        this.log('queue_message_dead_lettered', { messageId: message.MessageId, failureCode });
        return;
      }
      this.metrics.retries.inc({ source: 'sqs' });
      const visibility = Math.min(900, 2 ** Math.min(receiveCount, 8));
      await this.client.send(new ChangeMessageVisibilityCommand({
        QueueUrl: this.inputQueueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: visibility,
      }));
      this.log('queue_message_retry_scheduled', { messageId: message.MessageId, receiveCount, error: errorName(error) });
    }
  }

  /** Copies terminal failures to the FIFO DLQ before deleting the original receipt. */
  private async sendToDeadLetter(message: { Body?: string | undefined; MessageId?: string | undefined }, reason: string): Promise<void> {
    if (!this.deadLetterQueueUrl) return;
    const groupId = message.MessageId ?? 'invalid-message';
    await this.client.send(new SendMessageCommand({
      QueueUrl: this.deadLetterQueueUrl,
      MessageBody: JSON.stringify({ originalBody: message.Body ?? '', failureCode: reason, failedAt: new Date().toISOString() }),
      MessageGroupId: groupId,
      MessageDeduplicationId: groupId,
    }));
  }

  /** Prevents overlapping publisher loops inside one instance. */
  private scheduleOutbox(): void {
    if (this.stopping || this.outboxTask) return;
    this.outboxTask = this.publishOutbox().catch((error: unknown) => {
      this.log('outbox_publish_batch_failed', { error: errorName(error) });
    }).finally(() => { this.outboxTask = undefined; });
  }

  /** Runs the due-reference scanner only once per process at a time. */
  private schedulePendingReferences(): void {
    if (this.stopping || this.pendingTask) return;
    this.pendingTask = this.wagering.processDuePendingReferences().catch((error: unknown) => {
      this.log('pending_reference_batch_failed', { error: errorName(error) });
    }).finally(() => { this.pendingTask = undefined; });
  }

  /** Refreshes the SQS approximate DLQ depth without adding payload or identity labels. */
  private refreshDeadLetterMetric(): void {
    if (this.stopping || !this.deadLetterQueueUrl || this.queueMetricsTask) return;
    this.queueMetricsTask = this.client.send(new GetQueueAttributesCommand({
      QueueUrl: this.deadLetterQueueUrl,
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    })).then((response) => {
      const visible = Number(response.Attributes?.ApproximateNumberOfMessages ?? 0);
      const inFlight = Number(response.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
      this.metrics.deadLetterQueueMessages.set(visible + inFlight);
    }).catch((error: unknown) => {
      this.log('dlq_metric_refresh_failed', { error: errorName(error) });
    }).finally(() => { this.queueMetricsTask = undefined; });
  }

  /** Claims rows with SKIP LOCKED, publishes after the claim commits, then records success/retry. */
  private async publishOutbox(): Promise<void> {
    if (!this.eventsQueueUrl) return;
    const rows = await this.orm.em.transactional(async (em) => {
      // The SQL update atomically claims a leased batch so parallel publishers cannot overlap.
      const result = await em.getConnection().execute<OutboxRow[]>(
        `with due as (
           select id from outbox_messages
           where published_at is null and (next_attempt_at is null or next_attempt_at <= now())
             and (locked_until is null or locked_until < now())
           order by occurred_at asc for update skip locked limit 10
         )
         update outbox_messages o set locked_until = now() + interval '45 seconds'
         from due where o.id = due.id
         returning o.id, o.aggregate_id, o.payload, o.occurred_at, o.attempts`,
      );
      return result;
    });

    for (const row of rows) {
      try {
        const payload = typeof row.payload === 'string' ? row.payload : JSON.stringify(row.payload);
        // A crash after send but before published_at may duplicate; eventId is the consumer dedupe key.
        await this.client.send(new SendMessageCommand({
          QueueUrl: this.eventsQueueUrl,
          MessageBody: payload,
          MessageGroupId: row.aggregate_id,
          MessageDeduplicationId: row.id,
        }));
        await this.orm.em.getConnection().execute(
          'update outbox_messages set published_at = ?, locked_until = null where id = ?', [new Date(), row.id]);
      } catch (error) {
        await this.orm.em.getConnection().execute(
          `update outbox_messages set attempts = attempts + 1,
           next_attempt_at = now() + (least(60, power(2, least(attempts, 6))) * interval '1 second'),
           locked_until = null where id = ?`, [row.id]);
        this.metrics.retries.inc({ source: 'outbox' });
        this.log('outbox_event_retry_scheduled', { eventId: row.id, error: errorName(error) });
      }
    }

    const oldest = await this.orm.em.getConnection().execute<{ lag_seconds: number }[]>(
      `select coalesce(extract(epoch from (now() - min(occurred_at))), 0)::float8 as lag_seconds
       from outbox_messages where published_at is null`);
    this.metrics.outboxLagSeconds.set(oldest[0]?.lag_seconds ?? 0);
  }

  /** Applies a short delay after transient receive failures to avoid a hot error loop. */
  private async retryDelay(seconds: number): Promise<void> {
    await new Promise<void>((resolve) => setTimeout(resolve, seconds * 1_000));
  }

  /** Releases receipts fetched in a batch but not yet handed to the use case. */
  private async returnUnprocessedMessages(): Promise<void> {
    const handles = [...this.receivedMessageHandles]
      .filter((receiptHandle) => !this.processingMessageHandles.has(receiptHandle));
    await Promise.all(handles.map((receiptHandle) => this.returnMessageVisibility(receiptHandle)));
  }

  /** Makes an unstarted message immediately visible for another instance to receive. */
  private async returnMessageVisibility(receiptHandle: string): Promise<void> {
    if (!this.inputQueueUrl) return;
    try {
      await this.client.send(new ChangeMessageVisibilityCommand({
        QueueUrl: this.inputQueueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: 0,
      }));
    } catch (error) {
      this.log('queue_visibility_return_failed', { error: errorName(error) });
    }
  }

  /** Emits structured logs with transport/transaction identifiers, never full wager payloads. */
  private log(event: string, fields: Record<string, unknown>): void {
    this.logger.log(JSON.stringify({ event, ...fields }));
  }
}

/** Marks malformed or otherwise permanently invalid message envelopes. */
class PermanentMessageError extends Error {
  constructor(message: string, readonly failureCode: string = 'INVALID_PAYLOAD') {
    super(message);
    this.name = 'PermanentMessageError';
  }
}

/** Narrows JSON input to a non-array object before reading envelope properties. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Extracts a required string field and classifies malformed values as permanent. */
function readText(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new PermanentMessageError('Required message field is missing', 'INVALID_PAYLOAD');
  }
  return value;
}

/** Converts unknown failures to a safe log label. */
function errorName(error: unknown): string {
  return error instanceof Error ? error.name : 'UnknownError';
}

/** Accepts positive integer settings and falls back when configuration is absent or invalid. */
function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/** Accepts zero or positive values for the test-only pre-ack delay. */
function nonNegativeInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

/** Treats non-5xx Nest client exceptions as permanent queue failures. */
function isClientError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'getStatus' in error
    && typeof error.getStatus === 'function' && error.getStatus() < 500;
}