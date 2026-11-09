import { Controller, Get, Res } from '@nestjs/common';
import { MetricsService } from '../application/metrics.service.js';

interface MetricsResponse {
  setHeader(name: string, value: string): void;
  send(body: string): void;
}

/** Serves Prometheus text format from the application's private registry. */
@Controller()
export class MetricsController {
  constructor(private readonly metrics: MetricsService) {}

  /** Sets the registry's required content type before returning its collected samples. */
  @Get('metrics')
  async metricsEndpoint(@Res() response: MetricsResponse): Promise<void> {
    response.setHeader('Content-Type', this.metrics.contentType());
    response.send(await this.metrics.metrics());
  }
}