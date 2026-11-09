import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Param,
  Post,
  Query,
  Res,
} from '@nestjs/common';
import { WageringService } from '../application/wagering.service.js';
import { MetricsService } from '../application/metrics.service.js';

/** Minimal response surface needed to override Nest's default POST status. */
interface StatusResponse {
  status(code: number): unknown;
}

/** HTTP adapter that translates routes and transport metadata into application use cases. */
@Controller()
export class WageringController {
  constructor(private readonly wagering: WageringService, private readonly metrics: MetricsService) {}

  /** Creates a wallet and its optional opening ledger transaction. */
  @Post('wallets')
  createWallet(@Body() body: unknown) {
    return this.wagering.createWallet(body);
  }

  /** Returns the current materialized wallet balance. */
  @Get('wallets/:walletId')
  getWallet(@Param('walletId') walletId: string) {
    return this.wagering.getWallet(walletId);
  }

  /** Returns a stable cursor page of immutable wallet ledger entries. */
  @Get('wallets/:walletId/ledger')
  getLedger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor: string | undefined,
    @Query('limit') limit: string | undefined,
  ) {
    return this.wagering.getLedger(walletId, cursor, Number(limit ?? 50));
  }

  /** Reconstructs balance from ledger and reports, but never repairs, discrepancies. */
  @Post('wallets/:walletId/reconciliation')
  reconcile(@Param('walletId') walletId: string) {
    return this.wagering.reconcile(walletId);
  }

  /** Submits by required idempotency key and maps pending/rejected/failed states to HTTP status. */
  @Post('wagering/transactions')
  @HttpCode(200)
  async submit(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) response: StatusResponse,
  ) {
    if (!idempotencyKey) throw new BadRequestException({ code: 'INVALID_PAYLOAD', message: 'Idempotency-Key is required' });
    const endTimer = this.metrics.processingDuration.startTimer();
    const result = await this.wagering.submit(idempotencyKey, body);
    endTimer();
    this.metrics.transactionStatuses.inc({ status: result.status });
    if (result.idempotentReplay) this.metrics.duplicates.inc({ source: 'http' });
    if (result.status === 'PENDING_REFERENCE') response.status(202);
    else if (result.status === 'REJECTED') response.status(422);
    else if (result.status === 'FAILED') response.status(500);
    return result;
  }

  /** Looks up a transaction by its internal identifier. */
  @Get('wagering/transactions/:transactionId')
  getTransaction(@Param('transactionId') transactionId: string) {
    return this.wagering.getTransaction(transactionId);
  }

  /** Looks up a provider transaction by its external identifier. */
  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  getProviderTransaction(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ) {
    return this.wagering.getProviderTransaction(providerId, externalTransactionId);
  }
}