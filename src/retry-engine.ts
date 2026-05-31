import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import sqlite3 = require('sqlite3');
import { open, type Database } from 'sqlite';
import {
  AttemptApiModel,
  AttemptOutcome,
  AttemptRow,
  CreateRequestInput,
  NewAttemptRow,
  RequestApiModel,
  RequestDetail,
  RequestStatus,
  StoredRequestRow,
} from './types';

type SqliteDb = Database<sqlite3.Database, sqlite3.Statement>;

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
  logger?: (message: string) => void;
}

export class RetryStorage {
  private dbPromise: Promise<SqliteDb> | null = null;

  constructor(private readonly databasePath: string) {}

  async open(): Promise<SqliteDb> {
    if (!this.dbPromise) {
      mkdirSync(dirname(resolve(this.databasePath)), { recursive: true });
      this.dbPromise = open({
        filename: this.databasePath,
        driver: sqlite3.Database,
      });
    }

    const db = await this.dbPromise;
    await db.exec('PRAGMA journal_mode = WAL;');
    await db.exec('PRAGMA foreign_keys = ON;');
    await this.ensureSchema(db);
    return db;
  }

  private async ensureSchema(db: SqliteDb): Promise<void> {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS requests (
        id TEXT PRIMARY KEY,
        url TEXT NOT NULL,
        method TEXT NOT NULL,
        body TEXT,
        status TEXT NOT NULL,
        attemptCount INTEGER NOT NULL DEFAULT 0,
        maxRetries INTEGER NOT NULL,
        backoffMs INTEGER NOT NULL,
        timeoutMs INTEGER NOT NULL,
        nextRetryAt INTEGER,
        lastError TEXT,
        result TEXT,
        createdAt INTEGER NOT NULL,
        updatedAt INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_requests_status_nextRetryAt
      ON requests (status, nextRetryAt);

      CREATE TABLE IF NOT EXISTS attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        requestId TEXT NOT NULL,
        attemptNumber INTEGER NOT NULL,
        startedAt INTEGER NOT NULL,
        finishedAt INTEGER NOT NULL,
        waitMs INTEGER NOT NULL,
        jitterFactor REAL NOT NULL,
        outcome TEXT NOT NULL,
        statusCode INTEGER,
        responseBody TEXT,
        errorMessage TEXT,
        FOREIGN KEY (requestId) REFERENCES requests(id) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_attempts_requestId
      ON attempts (requestId, attemptNumber);
    `);
  }

  async createRequest(input: CreateRequestInput, defaults: { backoffMs: number; timeoutMs: number }): Promise<StoredRequestRow> {
    const db = await this.open();
    const now = Date.now();
    const row: StoredRequestRow = {
      id: randomUUID(),
      url: input.url,
      method: input.method.toUpperCase(),
      body: input.body === undefined ? null : JSON.stringify(input.body),
      status: 'pending',
      attemptCount: 0,
      maxRetries: normalizePositiveInteger(input.maxRetries, 5),
      backoffMs: normalizePositiveInteger(input.backoffMs, defaults.backoffMs),
      timeoutMs: normalizePositiveInteger(input.timeoutMs, defaults.timeoutMs),
      nextRetryAt: null,
      lastError: null,
      result: null,
      createdAt: now,
      updatedAt: now,
    };

    await db.run(
      `INSERT INTO requests (
        id, url, method, body, status, attemptCount, maxRetries, backoffMs, timeoutMs,
        nextRetryAt, lastError, result, createdAt, updatedAt
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      row.id,
      row.url,
      row.method,
      row.body,
      row.status,
      row.attemptCount,
      row.maxRetries,
      row.backoffMs,
      row.timeoutMs,
      row.nextRetryAt,
      row.lastError,
      row.result,
      row.createdAt,
      row.updatedAt,
    );

    return row;
  }

  async getRequestById(id: string): Promise<StoredRequestRow | null> {
    const db = await this.open();
    return (await db.get<StoredRequestRow>('SELECT * FROM requests WHERE id = ?', id)) ?? null;
  }

  async listRequests(status?: RequestStatus): Promise<StoredRequestRow[]> {
    const db = await this.open();
    if (status) {
      return db.all<StoredRequestRow[]>('SELECT * FROM requests WHERE status = ? ORDER BY createdAt DESC', status);
    }

    return db.all<StoredRequestRow[]>('SELECT * FROM requests ORDER BY createdAt DESC');
  }

  async listDueRequests(now: number, limit = 25): Promise<StoredRequestRow[]> {
    const db = await this.open();
    return db.all<StoredRequestRow[]>(
      `SELECT * FROM requests
       WHERE status IN ('pending', 'retrying')
         AND (nextRetryAt IS NULL OR nextRetryAt <= ?)
       ORDER BY COALESCE(nextRetryAt, createdAt), createdAt
       LIMIT ?`,
      now,
      limit,
    );
  }

  async listAttempts(requestId: string): Promise<AttemptRow[]> {
    const db = await this.open();
    return db.all<AttemptRow[]>('SELECT * FROM attempts WHERE requestId = ? ORDER BY attemptNumber ASC', requestId);
  }

  async recordAttempt(attempt: NewAttemptRow): Promise<void> {
    const db = await this.open();
    await db.run(
      `INSERT INTO attempts (
        requestId, attemptNumber, startedAt, finishedAt, waitMs, jitterFactor,
        outcome, statusCode, responseBody, errorMessage
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      attempt.requestId,
      attempt.attemptNumber,
      attempt.startedAt,
      attempt.finishedAt,
      attempt.waitMs,
      attempt.jitterFactor,
      attempt.outcome,
      attempt.statusCode,
      attempt.responseBody,
      attempt.errorMessage,
    );
  }

  async updateRequest(id: string, patch: Partial<StoredRequestRow>): Promise<void> {
    const db = await this.open();
    const current = await this.getRequestById(id);
    if (!current) {
      return;
    }

    const next: StoredRequestRow = { ...current, ...patch, updatedAt: patch.updatedAt ?? Date.now() };
    await db.run(
      `UPDATE requests SET
        url = ?, method = ?, body = ?, status = ?, attemptCount = ?, maxRetries = ?, backoffMs = ?, timeoutMs = ?,
        nextRetryAt = ?, lastError = ?, result = ?, createdAt = ?, updatedAt = ?
       WHERE id = ?`,
      next.url,
      next.method,
      next.body,
      next.status,
      next.attemptCount,
      next.maxRetries,
      next.backoffMs,
      next.timeoutMs,
      next.nextRetryAt,
      next.lastError,
      next.result,
      next.createdAt,
      next.updatedAt,
      id,
    );
  }
}

export class RetryEngine {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private readonly processingIds = new Set<string>();

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
      for (const request of dueRequests) {
        if (this.processingIds.has(request.id)) {
          continue;
        }

        this.processingIds.add(request.id);
        try {
          await this.processRequest(request.id);
        } finally {
          this.processingIds.delete(request.id);
        }
      }
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
    const jitterFactor = 0.8 + Math.random() * 0.4;
    const waitMs = Math.round(request.backoffMs * Math.pow(2, attemptNumber - 1) * jitterFactor);

    await this.storage.recordAttempt({
      requestId: request.id,
      attemptNumber,
      startedAt,
      finishedAt,
      waitMs: result.outcome === 'success' ? 0 : waitMs,
      jitterFactor: result.outcome === 'success' ? 1 : jitterFactor,
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
        lastError: result.errorMessage,
        result: result.responseBody,
        updatedAt,
      });
      this.log(`failed permanently ${request.id} on attempt ${attemptNumber}: ${result.errorMessage ?? 'terminal error'}`);
      return;
    }

    if (attemptNumber > request.maxRetries) {
      await this.storage.updateRequest(request.id, {
        status: 'failed',
        attemptCount: attemptNumber,
        nextRetryAt: null,
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
      lastError: result.errorMessage,
      result: result.responseBody,
      updatedAt,
    });

    this.log(
      `retrying ${request.id} after attempt ${attemptNumber}: wait ${waitMs}ms (base ${request.backoffMs}ms × jitter ${jitterFactor.toFixed(3)})`,
    );
  }

  private async performExternalCall(request: StoredRequestRow): Promise<ExternalCallResult> {
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

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.floor(value);
}

function buildRequestBody(body: string | null, method: string): string | undefined {
  if (!body || method === 'GET' || method === 'HEAD') {
    return undefined;
  }

  return body;
}

function parseJsonOrText(value: string | null): unknown {
  if (value === null) {
    return null;
  }

  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}