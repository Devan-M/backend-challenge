// Loads reflection metadata required by the MikroORM driver and migration classes.
import 'reflect-metadata';
import { MikroORM } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { Migration20261107000000 } from './migrations/Migration20261107000000.js';
import { Migration20261108000000 } from './migrations/Migration20261108000000.js';

/** Opens a short-lived ORM connection, applies one migration direction, then always closes it. */
async function runMigration(): Promise<void> {
  const orm = await MikroORM.init({
    driver: PostgreSqlDriver,
    host: process.env.DATABASE_HOST ?? 'localhost',
    port: Number(process.env.DATABASE_PORT ?? 5432),
    user: process.env.DATABASE_USER ?? 'wagering',
    password: process.env.DATABASE_PASSWORD ?? 'wagering',
    dbName: process.env.DATABASE_NAME ?? 'wagering',
    // NOTE: fallback credentials são um convenience de desenvolvimento local.
    // Em produção, DATABASE_USER / DATABASE_PASSWORD serão SEMPRE injetados via docker-compose (secrets) / AWS Secrets Manager / Kubernetes e o fallback abaixo nunca é utilizado (fail-fast em prod).
    entities: [],
    discovery: { warnWhenNoEntities: false },
    migrations: {
      migrationsList: [
        { name: 'Migration20261107000000', class: Migration20261107000000 },
        { name: 'Migration20261108000000', class: Migration20261108000000 },
      ],
      transactional: true,
      allOrNothing: true,
    },
  });

  // `down` reverts the latest version; every other argument applies pending versions.
  try {
    if (process.argv[2] === 'down') await orm.getMigrator().down();
    else await orm.getMigrator().up();
  } finally {
    await orm.close(true);
  }
}

void runMigration();