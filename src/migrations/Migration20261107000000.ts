import { Migration } from '@mikro-orm/migrations';

/** Initial PostgreSQL schema, including the financial invariants enforced below the application. */
export class Migration20261107000000 extends Migration {
  /** Creates wallets, transactions, immutable ledger, inbox, and outbox structures. */
  override async up(): Promise<void> {
    // Wallet uniqueness and non-negative balance are database guarantees across all instances.
    this.addSql(`create table "wallets" (
      "id" uuid primary key,
      "player_id" uuid not null,
      "currency" varchar(3) not null,
      "balance" numeric(38, 2) not null default 0 check ("balance" >= 0),
      "version" integer not null default 1 check ("version" > 0),
      "created_at" timestamptz not null,
      "updated_at" timestamptz not null,
      constraint "wallet_player_currency_unique" unique ("player_id", "currency")
    );`);

    // Idempotency, provider identity, references, and financial statuses are persisted here.
    this.addSql(`create table "wager_transactions" (
      "id" uuid primary key,
      "provider_id" text not null,
      "external_transaction_id" text not null,
      "idempotency_key" text not null unique,
      "payload_hash" char(64) not null,
      "wallet_id" uuid not null references "wallets" ("id"),
      "player_id" uuid not null,
      "round_id" text not null,
      "game_id" text not null,
      "kind" text not null check ("kind" in ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
      "amount" numeric(38, 2) not null check ("amount" > 0),
      "currency" varchar(3) not null,
      "reference_external_transaction_id" text,
      "reference_transaction_id" uuid references "wager_transactions" ("id"),
      "status" text not null check ("status" in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
      "failure_code" text,
      "created_at" timestamptz not null,
      "processed_at" timestamptz,
      "response_balance" numeric(38, 2),
      "pending_attempts" integer not null default 0,
      "next_reference_attempt_at" timestamptz,
      constraint "provider_external_transaction_unique" unique ("provider_id", "external_transaction_id"),
      constraint "reference_required_for_reversal" check (
        ("kind" in ('REFUND', 'ROLLBACK') and "reference_external_transaction_id" is not null)
        or ("kind" not in ('REFUND', 'ROLLBACK'))
      )
    );`);

    // Partial lookup indexes keep scheduled reference retry and duplicate reversal checks bounded.
    this.addSql(`create index "wager_pending_reference_due" on "wager_transactions" ("next_reference_attempt_at")
      where "status" = 'PENDING_REFERENCE';`);
    this.addSql(`create unique index "wager_reference_reversal_unique"
      on "wager_transactions" ("provider_id", "reference_external_transaction_id", "kind")
      where "kind" in ('REFUND', 'ROLLBACK');`);

    // Ledger arithmetic is checked by SQL; a trigger prevents updates or deletes after insertion.
    this.addSql(`create table "wallet_ledger_entries" (
      "id" uuid primary key,
      "wallet_id" uuid not null references "wallets" ("id"),
      "transaction_id" uuid not null unique references "wager_transactions" ("id"),
      "direction" text not null check ("direction" in ('DEBIT', 'CREDIT')),
      "amount" numeric(38, 2) not null check ("amount" > 0),
      "balance_before" numeric(38, 2) not null check ("balance_before" >= 0),
      "balance_after" numeric(38, 2) not null check ("balance_after" >= 0),
      "created_at" timestamptz not null,
      constraint "ledger_arithmetic_valid" check (
        ("direction" = 'CREDIT' and "balance_after" = "balance_before" + "amount")
        or ("direction" = 'DEBIT' and "balance_after" = "balance_before" - "amount")
      )
    );`);
    this.addSql(`create index "wallet_ledger_cursor" on "wallet_ledger_entries" ("wallet_id", "created_at" desc, "id" desc);`);
    this.addSql(`create function prevent_ledger_mutation() returns trigger language plpgsql as $$
      begin raise exception 'wallet ledger entries are immutable'; end;
      $$;`);
    this.addSql(`create trigger "wallet_ledger_immutable" before update or delete on "wallet_ledger_entries"
      for each row execute function prevent_ledger_mutation();`);

    // Inbox and outbox rows participate in the same transaction as financial writes.
    this.addSql(`create table "inbox_messages" (
      "consumer_name" text not null,
      "message_id" text not null,
      "payload_hash" char(64) not null,
      "received_at" timestamptz not null,
      "processed_at" timestamptz,
      primary key ("consumer_name", "message_id")
    );`);

    this.addSql(`create table "outbox_messages" (
      "id" uuid primary key,
      "aggregate_id" uuid not null,
      "event_type" text not null,
      "payload" jsonb not null,
      "occurred_at" timestamptz not null,
      "attempts" integer not null default 0,
      "next_attempt_at" timestamptz,
      "published_at" timestamptz,
      "locked_until" timestamptz
    );`);
    this.addSql(`create index "outbox_pending_due" on "outbox_messages" ("next_attempt_at", "occurred_at")
      where "published_at" is null;`);
  }

  /** Drops dependent objects in reverse dependency order for safe rollback. */
  override async down(): Promise<void> {
    this.addSql('drop table if exists "outbox_messages";');
    this.addSql('drop table if exists "inbox_messages";');
    this.addSql('drop trigger if exists "wallet_ledger_immutable" on "wallet_ledger_entries";');
    this.addSql('drop function if exists prevent_ledger_mutation();');
    this.addSql('drop table if exists "wallet_ledger_entries";');
    this.addSql('drop table if exists "wager_transactions";');
    this.addSql('drop table if exists "wallets";');
  }
}