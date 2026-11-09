import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';

/** Explicit replacement point for an external OIDC guard; intentionally allows requests for this challenge. */
@Injectable()
export class AuthGuard implements CanActivate {
  /** Production implementations should validate the provider principal and attach its identity. */
  canActivate(_context: ExecutionContext): boolean {
    return true;
  }
}