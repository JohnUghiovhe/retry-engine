import { RequestValidationError, validateCreateRequestInput } from './validation/request-validation';
import { parseJsonOrText } from './utils/json-utils';
import { validateTargetUrl } from './validation/url-safety';
import { normalizePositiveInteger, runWithConcurrency } from './utils/worker-utils';
import { RetryStorage } from './persistence/retry-storage';
import {
  AttemptApiModel,
  AttemptOutcome,
  AttemptRow,
  CreateRequestInput,
  RequestApiModel,
  RequestDetail,
  RequestStatus,
  StoredRequestRow,
} from './types';

interface ExternalCallResult {
  outcome: AttemptOutcome;
  statusCode: number | null;
  responseBody: string | null;
  errorMessage: string | null;
}

export interface RetryEngineOptions {
  databasePath: string;
  workerIntervalMs?: number;
  defaultBackoffMs?: number;
  defaultTimeoutMs?: number;
  concurrencyLimit?: number;
  lockTimeoutMs?: number;
  allowPrivateTargets?: boolean;
  allowedHosts?: string[];
  logger?: (message: string) => void;
}

export class RetryEngine {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(private readonly storage: RetryStorage, private readonly options: RetryEngineOptions) {}

  async start(): Promise<void> {
    await this.storage.open();
    if (this.timer) {
      return;
    }

    await this.tick();
    this.timer = setInterval(() => {
      void this.tick();
    }, this.options.workerIntervalMs ?? 500);
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async submitRequest(input: CreateRequestInput): Promise<RequestApiModel> {
    const validationError = validateCreateRequestInput(input, {
      allowPrivateTargets: this.options.allowPrivateTargets,
      allowedHosts: this.options.allowedHosts,
    });
    if (validationError) {
      throw new RequestValidationError(validationError);
    }

    const runtimeTargetError = await validateTargetUrl(input.url, {
      allowPrivateTargets: this.options.allowPrivateTargets,
      allowedHosts: this.options.allowedHosts,
    });
    if (runtimeTargetError) {
      throw new RequestValidationError(runtimeTargetError);
    }

    const row = await this.storage.createRequest(input, {
      backoffMs: this.options.defaultBackoffMs ?? 1000,
      timeoutMs: this.options.defaultTimeoutMs ?? 5000,
    });

    return this.toApiRequest(row);
  }

  async getRequest(id: string): Promise<RequestDetail | null> {
    const request = await this.storage.getRequestById(id);
    if (!request) {
      return null;
    }

    const attempts = await this.storage.listAttempts(id);
    return {
      request: this.toApiRequest(request),
      attempts: attempts.map((attempt) => this.toApiAttempt(attempt)),
    };
  }

  async listRequests(status?: RequestStatus): Promise<RequestApiModel[]> {
    const rows = await this.storage.listRequests(status);
    return rows.map((row) => this.toApiRequest(row));
  }

  private async tick(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;
    try {
      const dueRequests = await this.storage.listDueRequests(Date.now());
      const concurrencyLimit = normalizePositiveInteger(this.options.concurrencyLimit, 3);
      await runWithConcurrency(dueRequests, concurrencyLimit, async (request) => {
        const claimed = await this.storage.claimRequest(request.id, Date.now(), this.options.lockTimeoutMs ?? 30_000);
        if (!claimed) {
          return;
        }

        try {
          await this.processRequest(request.id);
        } finally {
          await this.storage.releaseRequestLock(request.id);
        }
      });
    } finally {
      this.running = false;
    }
  }

  private async processRequest(requestId: string): Promise<void> {
    const request = await this.storage.getRequestById(requestId);
    if (!request || request.status === 'completed' || request.status === 'failed') {
      return;
    }

    const attemptNumber = request.attemptCount + 1;
    const startedAt = Date.now();
    const result = await this.performExternalCall(request);
    const finishedAt = Date.now();
    const shouldRetry = isRetryableOutcome(result.outcome) && attemptNumber <= request.maxRetries;
    const jitterFactor = shouldRetry ? 0.8 + Math.random() * 0.4 : 1;
    const waitMs = shouldRetry ? Math.round(request.backoffMs * Math.pow(2, attemptNumber - 1) * jitterFactor) : 0;

    await this.storage.recordAttempt({
      requestId: request.id,
      attemptNumber,
      startedAt,
      finishedAt,
      waitMs,
      jitterFactor,
      outcome: result.outcome,
      statusCode: result.statusCode,
      responseBody: result.responseBody,
      errorMessage: result.errorMessage,
    });

    const updatedAt = Date.now();
    if (result.outcome === 'success') {
      await this.storage.updateRequest(request.id, {
        status: 'completed',
        attemptCount: attemptNumber,
        nextRetryAt: null,
        lockedUntil: 0,
        lastError: null,
        result: result.responseBody,
        updatedAt,
      });
      this.log(`completed ${request.id} on attempt ${attemptNumber}`);
      return;
    }

    if (result.outcome === 'terminal_error') {
      await this.storage.updateRequest(request.id, {
        status: 'failed',
        attemptCount: attemptNumber,
        nextRetryAt: null,
        lockedUntil: 0,
        lastError: result.errorMessage,
        result: result.responseBody,
        updatedAt,
      });
      this.log(`failed permanently ${request.id} on attempt ${attemptNumber}: ${result.errorMessage ?? 'terminal error'}`);
      return;
    }

    if (!shouldRetry) {
      await this.storage.updateRequest(request.id, {
        status: 'failed',
        attemptCount: attemptNumber,
        nextRetryAt: null,
        lockedUntil: 0,
        lastError: result.errorMessage,
        result: result.responseBody,
        updatedAt,
      });
      this.log(`dead-lettered ${request.id} after ${attemptNumber} attempts: ${result.errorMessage ?? 'retry limit reached'}`);
      return;
    }

    const nextRetryAt = Date.now() + waitMs;
    await this.storage.updateRequest(request.id, {
      status: 'retrying',
      attemptCount: attemptNumber,
      nextRetryAt,
      lockedUntil: 0,
      lastError: result.errorMessage,
      result: result.responseBody,
      updatedAt,
    });

    this.log(
      `retrying ${request.id} after attempt ${attemptNumber}: wait ${waitMs}ms (base ${request.backoffMs}ms × jitter ${jitterFactor.toFixed(3)})`,
    );
  }

  private async performExternalCall(request: StoredRequestRow): Promise<ExternalCallResult> {
    const targetValidationError = await validateTargetUrl(request.url, {
      allowPrivateTargets: this.options.allowPrivateTargets,
      allowedHosts: this.options.allowedHosts,
    });
    if (targetValidationError) {
      return {
        outcome: 'terminal_error',
        statusCode: null,
        responseBody: null,
        errorMessage: targetValidationError,
      };
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), request.timeoutMs);
    try {
      const body = buildRequestBody(request.body, request.method);
      const response = await fetch(request.url, {
        method: request.method,
        body,
        headers: body ? { 'content-type': 'application/json' } : undefined,
        signal: controller.signal,
      });

      const responseBody = await response.text();
      if (response.ok) {
        return { outcome: 'success', statusCode: response.status, responseBody, errorMessage: null };
      }

      if (response.status >= 500) {
        return { outcome: 'retryable_error', statusCode: response.status, responseBody, errorMessage: `HTTP ${response.status}` };
      }

      return { outcome: 'terminal_error', statusCode: response.status, responseBody, errorMessage: `HTTP ${response.status}` };
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        return { outcome: 'timeout', statusCode: null, responseBody: null, errorMessage: 'timeout' };
      }

      return {
        outcome: 'network_error',
        statusCode: null,
        responseBody: null,
        errorMessage: error instanceof Error ? error.message : 'network error',
      };
    } finally {
      clearTimeout(timeout);
    }
  }

  private toApiRequest(row: StoredRequestRow): RequestApiModel {
    return {
      id: row.id,
      url: row.url,
      method: row.method,
      body: parseJsonOrText(row.body),
      status: row.status,
      attemptCount: row.attemptCount,
      maxRetries: row.maxRetries,
      backoffMs: row.backoffMs,
      timeoutMs: row.timeoutMs,
      nextRetryAt: row.nextRetryAt === null ? null : new Date(row.nextRetryAt).toISOString(),
      lastError: row.lastError,
      result: parseJsonOrText(row.result),
      createdAt: new Date(row.createdAt).toISOString(),
      updatedAt: new Date(row.updatedAt).toISOString(),
    };
  }

  private toApiAttempt(row: AttemptRow): AttemptApiModel {
    return {
      id: row.id,
      requestId: row.requestId,
      attemptNumber: row.attemptNumber,
      startedAt: new Date(row.startedAt).toISOString(),
      finishedAt: new Date(row.finishedAt).toISOString(),
      waitMs: row.waitMs,
      jitterFactor: row.jitterFactor,
      outcome: row.outcome,
      statusCode: row.statusCode,
      responseBody: parseJsonOrText(row.responseBody),
      errorMessage: row.errorMessage,
    };
  }

  private log(message: string): void {
    this.options.logger?.(`[retry-engine] ${message}`);
  }
}

function buildRequestBody(body: string | null, method: string): string | undefined {
  if (!body || method === 'GET' || method === 'HEAD') {
    return undefined;
  }

  return body;
}

function isRetryableOutcome(outcome: AttemptOutcome): boolean {
  return outcome === 'retryable_error' || outcome === 'timeout' || outcome === 'network_error';
}