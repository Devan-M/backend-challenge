import { Migration } from '@mikro-orm/migrations';

/** Narrows successful reversal uniqueness so rejected attempts do not block corrected retries. */
export class Migration20261108000000 extends Migration {
  /** Replaces the original index with a partial index over only PROCESSED reversals. */
  override async up(): Promise<void> {
    this.addSql('drop index if exists "wager_reference_reversal_unique";');
    this.addSql(`create unique index "wager_reference_reversal_unique"
      on "wager_transactions" ("provider_id", "reference_external_transaction_id", "kind")
      where "kind" in ('REFUND', 'ROLLBACK') and "status" = 'PROCESSED';`);
  }

  /** Restores the original index predicate when this migration is rolled back. */
  override async down(): Promise<void> {
    this.addSql('drop index if exists "wager_reference_reversal_unique";');
    this.addSql(`create unique index "wager_reference_reversal_unique"
      on "wager_transactions" ("provider_id", "reference_external_transaction_id", "kind")
      where "kind" in ('REFUND', 'ROLLBACK');`);
  }
}