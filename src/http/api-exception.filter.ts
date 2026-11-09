import { ArgumentsHost, Catch, ExceptionFilter, HttpException } from '@nestjs/common';

/** Narrow shape of the underlying HTTP response needed by this adapter. */
interface HttpResponse {
  status(code: number): HttpResponse;
  json(body: unknown): void;
}

/** Normalizes unexpected database and application errors into stable HTTP contracts. */
@Catch()
export class ApiExceptionFilter implements ExceptionFilter {
  /** Preserves typed Nest exceptions and hides internal details for unknown failures. */
  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<HttpResponse>();
    if (exception instanceof HttpException) {
      response.status(exception.getStatus()).json(exception.getResponse());
      return;
    }

    const code = databaseErrorCode(exception);
    if (code === '23505') {
      response.status(409).json({ code: 'RESOURCE_CONFLICT', message: 'A unique resource already exists' });
    } else if (code === '22P02') {
      response.status(400).json({ code: 'INVALID_PAYLOAD', message: 'A UUID field is malformed' });
    } else if (isTransientDatabaseCode(code)) {
      response.status(503).json({ code: 'TRANSIENT_INFRASTRUCTURE', retryable: true });
    } else {
      response.status(500).json({ code: 'INTERNAL_ERROR', retryable: false });
    }
  }
}

/** Walks wrapped driver errors because MikroORM may expose SQLSTATE on `cause`. */
function databaseErrorCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const record = error as { code?: unknown; cause?: unknown };
  if (typeof record.code === 'string') return record.code;
  return databaseErrorCode(record.cause);
}

/** Maps connection-class SQLSTATEs and common socket failures to retryable 503s. */
function isTransientDatabaseCode(code: string | undefined): boolean {
  return (code?.startsWith('08') ?? false)
    || ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', '57P01', '57P02', '57P03'].includes(code ?? '');
}
