// Shared CLI configuration for local migration commands (runtime configuration lives in AppModule).
import 'reflect-metadata';
import { defineConfig } from '@mikro-orm/postgresql';
import { Migration20261107000000 } from './src/migrations/Migration20261107000000.js';
import { Migration20261108000000 } from './src/migrations/Migration20261108000000.js';

export default defineConfig({
  host: process.env.DATABASE_HOST ?? 'localhost',
  port: Number(process.env.DATABASE_PORT ?? 5432),
  user: process.env.DATABASE_USER ?? 'wagering',
  password: process.env.DATABASE_PASSWORD ?? 'wagering',
  dbName: process.env.DATABASE_NAME ?? 'wagering',
  entities: [],
  // Register explicit versioned migrations so rollback order is deterministic.
  migrations: {
    migrationsList: [
      { name: 'Migration20261107000000', class: Migration20261107000000 },
      { name: 'Migration20261108000000', class: Migration20261108000000 },
    ],
    transactional: true,
    allOrNothing: true,
  },
  discovery: { warnWhenNoEntities: false },
});