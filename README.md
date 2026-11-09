# Distributed Wagering Processor

A NestJS service for processing wagers and settlements from multiple providers. The project demonstrates precise financial processing, persistent idempotency, wallet-based concurrency, an immutable ledger, asynchronous integration via SQS, and recovery using the inbox/outbox pattern.

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Running Locally](#running-locally)
- [HTTP API](#http-api)
- [Financial Rules](#financial-rules)
- [Queues and Events](#queues-and-events)
- [Observability](#observability)
- [Testing](#testing)
- [Related Documentation](#related-documentation)
- [Limitations](#limitations)

## Overview

The service handles gaming operations associated with a specific round:

```text
BET -> WIN | LOSS | REFUND | ROLLBACK
```

Deliveries may be duplicated or arrive out of order. The application persists every operation and ensures that a repeated request does not re-apply the financial effect. PostgreSQL serves as the source of truth; while SQS FIFO organizes messages, it does not replace the guarantees provided by the database.

Monetary values ​​are represented as `bigint` (in the smallest currency unit) within the domain, and as `NUMERIC(38, 2)` in PostgreSQL. HTTP contracts use decimal values ​​represented as strings—never as the `number` type:

```json
{ "amount": "25.00", "currency": "BRL" }
```

## Architecture

- `src/domain`: Contains `Money`, `Wallet`, transactions, the ledger, and event envelopes; independent of NestJS or any ORM.
- `src/application`: Contains financial use cases, transactional persistence, idempotency logic, the inbox/outbox pattern, and background workers. - `src/http`: controllers, health checks, metrics, and HTTP error mapping.
- `src/migrations`: reversible PostgreSQL migrations and financial constraints.
- `test/integration`: scenarios using actual PostgreSQL and LocalStack instances running in containers.

A financial submission runs within a SQL transaction. The application locks the wallet row using `SELECT ... FOR UPDATE`, validates the balance and reference, records the transaction/ledger/outbox entries, and—in the case of SQS—marks the inbox before committing. The unit of concurrency is the wallet; different wallets remain independent.

The outbox is processed in batches using `FOR UPDATE SKIP LOCKED` and leases. Delivery follows an "at-least-once" guarantee; event consumers must perform deduplication based on the `eventId`. For details regarding design decisions, errors, and trade-offs, please refer to [ARCHITECTURE.md](ARCHITECTURE.md).

## Running Locally

### Requirements

- Bun 1.4.4 (or a version compatible with the project's lockfile).
- Docker Desktop with Docker Compose.

### Starting Services

From the repository root, using PowerShell:

```powershell
bun install
docker compose up --build -d
```

Docker Compose will start PostgreSQL and LocalStack, create the SQS queues, and launch the API. Upon the initial startup, the API container will automatically apply the migrations.

Local URLs:

| Service | URL |
|---|---|
| API | `http://localhost:3000` |
| Liveness | `http://localhost:3000/health/live` |
| Readiness | `http://localhost:3000/health/ready` |
| Prometheus Metrics | `http://localhost:3000/metrics` |
| SQS/LocalStack | `http://localhost:4566` |
| PostgreSQL | `localhost:5432` | To view the logs and the status of the containers:

```powershell
docker compose ps
docker compose logs -f app
```

To stop the services without deleting the local database:

```powershell
docker compose down
```

Data is stored in named volumes. Running `docker compose down -v` will also remove these volumes and delete the local data.

### Environment Variables

The [.env.example](.env.example) file serves as a version-controlled template, while `.env` is included in `.gitignore` to prevent local configurations from being committed. When running Bun directly on the host, use the template as a reference and define `DATABASE_*`, `SQS_ENDPOINT`, `WAGER_QUEUE_URL`, `WAGER_DLQ_URL`, and `EVENTS_QUEUE_URL` within your process environment.

In the current Docker Compose setup, `DATABASE_USER`, `DATABASE_PASSWORD`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` are defined directly in `docker-compose.yml` with values ​​intended **strictly for local development** (`wagering`). Creating a `.env` file will not change these credentials, as they are not currently interpolated by Compose. Do not use these values ​​in production; instead, configure secrets using a mechanism appropriate for your deployment environment and ensure they are not version-controlled.

The variables `WAGER_MESSAGE_VISIBILITY_TIMEOUT_SECONDS` and `SQS_BEFORE_ACK_DELAY_MS` allow you to configure the visibility timeout and a delay before acknowledgment (ack). This delay is intended for testing crash scenarios and should remain set to `0` outside of testing.

### Migrations

The container automatically applies pending migrations upon startup. You can also manually apply or roll back the most recent migration, provided the database is accessible and the connection variables are configured:

```powershell
bun run db:migrate
bun run db:rollback
```

## HTTP API

The examples below use `curl.exe`, which is available on Windows. Replace the example UUIDs with the values ​​returned by your own API calls. 

### Create wallet

```powershell
curl.exe -X POST http://localhost:3000/wallets ` 
-H "Content-Type: application/json" ` 
-d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance"