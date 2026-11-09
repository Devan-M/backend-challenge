# Code and Configuration Guide

JSON files cannot contain comments without becoming invalid, so this guide explains the configuration files and generated lockfile.

## Project Configuration

- `package.json`: Defines Bun scripts for the strict build, unit and integration tests, API startup, and reversible migrations; dependency groups are split between runtime and development tooling.
- `bun.lock`: Bun's generated, exact dependency graph. Regenerate it with `bun install` after dependency changes; do not hand-edit it.
- `tsconfig.json`: Enables strict production TypeScript, decorator metadata for NestJS, and emits application code from `src` to `dist`.
- `test/tsconfig.json`: Extends production strictness for integration tests, includes Bun test types, and disables output because test source runs directly under Bun.
- `mikro-orm.config.ts`: Supplies database connection defaults and the ordered, reversible migrations to the MikroORM CLI.
- `.env.example`: Lists local runtime environment variables with non-secret development defaults. Copy values into the shell or a local `.env`; do not store production credentials here.
- `.gitignore`: Excludes installed dependencies, build output, local environment secrets, and logs.
- `.dockerignore`: Keeps local dependencies, build artifacts, Git metadata, and secrets out of the image build context.

## Runtime Configuration

- `Dockerfile`: Installs the locked dependency graph, builds both strict TypeScript projects, then applies migrations before launching NestJS.
- `docker-compose.yml`: Defines PostgreSQL, LocalStack, queue initialization, the default app, and two optional replicas under the `multi` profile. The shared app anchor keeps all replicas on the same database and queues.
- `infra/init-queues.sh`: Creates the input FIFO with a five-receive redrive policy, its FIFO DLQ, and the integration-event FIFO; `set -eu` makes partial setup fail visibly.

## Application Layout

- `src/domain`: Exact money, wallet and ledger invariants, wager state transitions, and integration-event envelopes. These modules do not depend on NestJS or ORM decorators.
- `src/application`: Transactional use cases, metrics, and SQS inbox/outbox/reference workers.
- `src/http`: HTTP route adapters, health/metrics endpoints, authentication extension, and exception mapping.
- `src/migrations`: Versioned PostgreSQL schema changes; `up` and `down` define forward and reverse operations.
- `test/integration`: Container-backed HTTP, PostgreSQL, and LocalStack scenarios, including opt-in multi-process restart and crash-window cases.
