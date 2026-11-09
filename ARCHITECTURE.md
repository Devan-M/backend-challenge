# Architecture Decisions

## Runtime and persistence

The service uses Bun 1.x, strict TypeScript, NestJS, MikroORM 6 and PostgreSQL. MikroORM owns connection management and SQL transactions; the financial write path uses parameterized SQL inside `EntityManager.transactional()` so PostgreSQL row locks and exact `NUMERIC` values are explicit. Money is represented in the domain as signed `bigint` minor units and persisted as `NUMERIC(38, 2)` text. No monetary calculation passes through JavaScript `number`.

Migrations are versioned and reversible. Database constraints enforce the wallet uniqueness key, non-negative materialized balance, positive ledger amounts, ledger arithmetic, one ledger row per transaction, provider transaction identity, idempotency key uniqueness, required reversal references, and one successful reversal per reference and reversal kind. A trigger rejects ledger updates and deletes.

## Concurrency and idempotency

`wallets` is the lock boundary. Each financial operation takes `SELECT ... FOR UPDATE` on its wallet, calculates with `Money`, then writes the wallet, transaction, ledger and outbox in one SQL transaction. Different wallets can proceed independently; a failed/insufficient debit commits as an auditable rejection without changing the wallet.

The `Idempotency-Key` is globally unique. A SHA-256 digest is calculated from recursively key-sorted business fields, excluding the header and transport metadata. Reuse with the same digest returns the stored terminal/pending result and original response balance; reuse with a different digest returns `409 IDEMPOTENCY_KEY_REUSED`. A provider/external transaction pair is separately unique.

`BET` debits, `WIN` and `REFUND` credit, `LOSS` has no ledger movement, and `ROLLBACK` reverses the referenced processed operation's ledger direction. Reversals require equal amounts and matching provider, player, wallet, currency and round. A wallet overdraw by `BET` is `INSUFFICIENT_FUNDS`; an overdraw by a reversal is `REVERSAL_WOULD_OVERDRAW`.

Missing or not-yet-processed references remain `PENDING_REFERENCE`. A database scheduler retries with exponential backoff starting at 5 seconds and capped at 60 seconds; after 12 attempts it rejects with `REFERENCE_NOT_FOUND`. The cap bounds retention to roughly eight minutes after the first attempt. Only `PROCESSED` reversals occupy the database's partial unique reversal index.

## Inbox, outbox and SQS

The HTTP and SQS paths call the same `WageringService.submit()` use case. SQS inbox identity is `(consumer_name, message_id)`; inbox receipt, financial effects, ledger and outbox are committed together. The consumer deletes a message only after the use case commits. Invalid client messages are sent to the configured DLQ; infrastructure errors receive exponential visibility delays and the queue's redrive policy moves exhausted messages to DLQ.

Outbox publishers claim due rows with `FOR UPDATE SKIP LOCKED` and a lease, then publish to the events FIFO queue. A crash after SQS accepts a message but before `published_at` is persisted can publish it again; consumers must deduplicate on the envelope's stable `eventId`. This is at-least-once delivery by design. Retry attempts and next-attempt times remain in PostgreSQL.

## API and operations

HTTP semantics are consistent: malformed input is `400`, idempotency/identity conflict is `409`, business rejection is `422`, missing reference is accepted as `202`, and known transient database failures are `503` with `retryable: true`. `GET /health/ready` checks PostgreSQL and both SQS queues; `/health/live` remains process-only. `/metrics` exposes Prometheus counters, histograms and outbox lag.

Authentication is intentionally not implemented because it is not scored by the challenge. A global no-op `AuthGuard` is the explicit replacement point. A production deployment should replace it with an OIDC guard backed by an external Identity Provider and validate provider identity against the authenticated principal. Health endpoints remain open.

## Current limitations

The implementation assumes ISO currency codes with two fractional digits, as required by the challenge's fixed two-decimal contract; currencies with other minor-unit scales are not supported. The LocalStack setup is for local development. Container-backed tests cover competing bets, 50 simultaneous first submissions of one idempotency key, parallel wagers on distinct wallets, three application processes, schema constraints, atomic rollback, reconciliation divergence, reversals, out-of-order recovery, inbox redelivery, transient database retry, permanent-infrastructure `FAILED` audit and DLQ, concurrent outbox publication during an SQS outage, graceful restart, and SIGKILL after commit but before ack. Remaining gaps are exhausting the SQS redrive policy end-to-end under a prolonged outage, a formal load test, and production OIDC/managed-secret/alerting configuration.