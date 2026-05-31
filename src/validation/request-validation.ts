import { CreateRequestInput } from '../types';
import { validateTargetUrlSync, type TargetSafetyOptions } from './url-safety';

const ALLOWED_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS']);

export class RequestValidationError extends Error {
  readonly statusCode = 400;

  constructor(message: string) {
    super(message);
    this.name = 'RequestValidationError';
  }
}

export interface RequestValidationOptions extends TargetSafetyOptions {}

export function validateCreateRequestInput(input: unknown, options: RequestValidationOptions = {}): string | null {
  if (!input || typeof input !== 'object') {
    return 'body must be a JSON object';
  }

  const candidate = input as Partial<CreateRequestInput>;
  if (typeof candidate.url !== 'string' || candidate.url.trim().length === 0) {
    return 'url is required';
  }

  let parsedUrl: URL;
  try {
    parsedUrl = new URL(candidate.url);
  } catch {
    return 'url must be valid';
  }

  if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
    return 'url must use http or https';
  }

  if (typeof candidate.method !== 'string' || candidate.method.trim().length === 0) {
    return 'method is required';
  }

  const normalizedMethod = candidate.method.toUpperCase();
  if (!ALLOWED_METHODS.has(normalizedMethod)) {
    return 'method must be one of GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS';
  }

  const maxRetriesError = validateOptionalInteger(candidate.maxRetries, 0, 100, 'maxRetries');
  if (maxRetriesError) {
    return maxRetriesError;
  }

  const backoffError = validateOptionalInteger(candidate.backoffMs, 1, 86_400_000, 'backoffMs');
  if (backoffError) {
    return backoffError;
  }

  const timeoutError = validateOptionalInteger(candidate.timeoutMs, 1, 300_000, 'timeoutMs');
  if (timeoutError) {
    return timeoutError;
  }

  const bodyError = validateSerializableBody(candidate.body);
  if (bodyError) {
    return bodyError;
  }

  return validateTargetUrlSync(parsedUrl, options);
}

function validateOptionalInteger(value: unknown, minimum: number, maximum: number, fieldName: string): string | null {
  if (value === undefined) {
    return null;
  }

  if (typeof value !== 'number' || !Number.isInteger(value)) {
    return `${fieldName} must be an integer`;
  }

  if (value < minimum || value > maximum) {
    return `${fieldName} must be between ${minimum} and ${maximum}`;
  }

  return null;
}

function validateSerializableBody(body: unknown): string | null {
  if (body === undefined) {
    return null;
  }

  try {
    JSON.stringify(body);
  } catch {
    return 'body must be JSON serializable';
  }

  return null;
}