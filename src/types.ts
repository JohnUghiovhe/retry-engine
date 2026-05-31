export type RequestStatus = 'pending' | 'retrying' | 'completed' | 'failed';

export type AttemptOutcome = 'success' | 'retryable_error' | 'terminal_error' | 'timeout' | 'network_error';

export interface CreateRequestInput {
  url: string;
  method: string;
  body?: unknown;
  maxRetries?: number;
  backoffMs?: number;
  timeoutMs?: number;
}

export interface StoredRequestRow {
  id: string;
  url: string;
  method: string;
  body: string | null;
  status: RequestStatus;
  attemptCount: number;
  maxRetries: number;
  backoffMs: number;
  timeoutMs: number;
  nextRetryAt: number | null;
  lastError: string | null;
  result: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface AttemptRow {
  id: number;
  requestId: string;
  attemptNumber: number;
  startedAt: number;
  finishedAt: number;
  waitMs: number;
  jitterFactor: number;
  outcome: AttemptOutcome;
  statusCode: number | null;
  responseBody: string | null;
  errorMessage: string | null;
}

export interface NewAttemptRow {
  requestId: string;
  attemptNumber: number;
  startedAt: number;
  finishedAt: number;
  waitMs: number;
  jitterFactor: number;
  outcome: AttemptOutcome;
  statusCode: number | null;
  responseBody: string | null;
  errorMessage: string | null;
}

export interface RequestApiModel {
  id: string;
  url: string;
  method: string;
  body: unknown;
  status: RequestStatus;
  attemptCount: number;
  maxRetries: number;
  backoffMs: number;
  timeoutMs: number;
  nextRetryAt: string | null;
  lastError: string | null;
  result: unknown;
  createdAt: string;
  updatedAt: string;
}

export interface AttemptApiModel {
  id: number;
  requestId: string;
  attemptNumber: number;
  startedAt: string;
  finishedAt: string;
  waitMs: number;
  jitterFactor: number;
  outcome: AttemptOutcome;
  statusCode: number | null;
  responseBody: unknown;
  errorMessage: string | null;
}

export interface RequestDetail {
  request: RequestApiModel;
  attempts: AttemptApiModel[];
}