import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { WageringService } from './application/wagering.service.js';
import { HealthController } from './http/health.controller.js';
import { MetricsController } from './http/metrics.controller.js';
import { WageringController } from './http/wagering.controller.js';
import { MetricsService } from './application/metrics.service.js';
import { SqsRuntimeService } from './application/sqs-runtime.service.js';
import { AuthGuard } from './http/auth.guard.js';

/** Root DI module: one PostgreSQL ORM context, HTTP adapters, and background workers. */
@Module({
  imports: [MikroOrmModule.forRoot({
    driver: PostgreSqlDriver,
    host: process.env.DATABASE_HOST ?? 'localhost',
    port: Number(process.env.DATABASE_PORT ?? 5432),
    // NOTE: fallback credentials são um convenience de desenvolvimento local.
    // Em produção, DATABASE_USER / DATABASE_PASSWORD serão SEMPRE injetados via docker-compose (secrets) / AWS Secrets Manager / Kubernetes e o fallback abaixo nunca é utilizado (fail-fast em prod).
    user: process.env.DATABASE_USER ?? 'wagering',
    password: process.env.DATABASE_PASSWORD ?? 'wagering',
    dbName: process.env.DATABASE_NAME ?? 'wagering',
    entities: [],
    discovery: { warnWhenNoEntities: false },
    // Compiled/runtime and source paths let the same migration set run in Docker and Bun.
    migrations: {
      path: `${process.cwd()}/dist/migrations`,
      pathTs: `${process.cwd()}/src/migrations`,
      transactional: true,
      allOrNothing: true,
    },
  })],
  controllers: [WageringController, HealthController, MetricsController],
  providers: [WageringService, MetricsService, SqsRuntimeService, AuthGuard],
})
export class AppModule {}