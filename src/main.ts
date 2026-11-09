// Enables metadata reflection before Nest/MikroORM decorators are loaded.
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { AuthGuard } from './http/auth.guard.js';
import { ApiExceptionFilter } from './http/api-exception.filter.js';

/** Creates the HTTP process and installs cross-cutting request policies once. */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  // AuthGuard is intentionally a no-op seam documented for external OIDC integration.
  app.useGlobalGuards(app.get(AuthGuard));
  // Converts raw database/socket failures into stable API status and error codes.
  app.useGlobalFilters(new ApiExceptionFilter());
  app.enableShutdownHooks();
  await app.listen(Number(process.env.PORT ?? 3000), '0.0.0.0');
}

// Bun executes the async bootstrap; errors are surfaced by Nest during startup.
void bootstrap();