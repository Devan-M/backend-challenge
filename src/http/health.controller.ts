import { Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/core';
import { SqsRuntimeService } from '../application/sqs-runtime.service.js';

/** Exposes process liveness separately from PostgreSQL/SQS dependency readiness. */
@Controller('health')
export class HealthController {
  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    private readonly sqs: SqsRuntimeService,
  ) {}

  /** Liveness does not depend on downstream services. */
  @Get('live')
  live() {
    return { status: 'ok' };
  }

  /** Readiness is true only when the database and configured SQS queues respond. */
  @Get('ready')
  async ready() {
    // Probe each dependency independently so the response identifies the failing service.
    let postgresReady = false;
    let sqsReady = false;
    try {
      await this.orm.em.getConnection().execute('select 1');
      postgresReady = true;
    } catch {
      postgresReady = false;
    }
    sqsReady = await this.sqs.isReady();
    if (!postgresReady || !sqsReady) {
      throw new ServiceUnavailableException({
        status: 'not_ready',
        dependencies: { postgres: postgresReady ? 'ok' : 'unavailable', sqs: sqsReady ? 'ok' : 'unavailable' },
      });
    }
    return { status: 'ready', dependencies: { postgres: 'ok', sqs: 'ok' } };
  }
}