import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import sqlite3 = require('sqlite3');
import { open, type Database } from 'sqlite';
import { normalizePositiveInteger } from '../utils/worker-utils';
import { AttemptRow, CreateRequestInput, NewAttemptRow, RequestStatus, StoredRequestRow } from '../types';
import { serializeBody } from '../utils/json-utils';

type SqliteDb = Database<sqlite3.Database, sqlite3.Statement>;

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
        lockedUntil INTEGER NOT NULL DEFAULT 0,
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

    const requestColumns = await db.all<{ name: string }[]>('PRAGMA table_info(requests)');
    const hasLockedUntil = requestColumns.some((column) => column.name === 'lockedUntil');
    if (!hasLockedUntil) {
      await db.exec('ALTER TABLE requests ADD COLUMN lockedUntil INTEGER NOT NULL DEFAULT 0;');
    }
  }

  async createRequest(input: CreateRequestInput, defaults: { backoffMs: number; timeoutMs: number }): Promise<StoredRequestRow> {
    const db = await this.open();
    const now = Date.now();
    const row: StoredRequestRow = {
      id: randomUUID(),
      url: input.url,
      method: input.method.toUpperCase(),
      body: serializeBody(input.body),
      status: 'pending',
      attemptCount: 0,
      maxRetries: normalizePositiveInteger(input.maxRetries, 5),
      backoffMs: normalizePositiveInteger(input.backoffMs, defaults.backoffMs),
      timeoutMs: normalizePositiveInteger(input.timeoutMs, defaults.timeoutMs),
      nextRetryAt: null,
      lockedUntil: 0,
      lastError: null,
      result: null,
      createdAt: now,
      updatedAt: now,
    };

    await db.run(
      `INSERT INTO requests (
        id, url, method, body, status, attemptCount, maxRetries, backoffMs, timeoutMs,
        nextRetryAt, lockedUntil, lastError, result, createdAt, updatedAt
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      row.lockedUntil,
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
         AND lockedUntil <= ?
       ORDER BY COALESCE(nextRetryAt, createdAt), createdAt
       LIMIT ?`,
      now,
      now,
      limit,
    );
  }

  async claimRequest(id: string, now: number, lockTimeoutMs: number): Promise<boolean> {
    const db = await this.open();
    const result = await db.run(
      `UPDATE requests
       SET lockedUntil = ?, updatedAt = ?
       WHERE id = ?
         AND status IN ('pending', 'retrying')
         AND (nextRetryAt IS NULL OR nextRetryAt <= ?)
         AND lockedUntil <= ?`,
      now + lockTimeoutMs,
      now,
      id,
      now,
      now,
    );

    return (result.changes ?? 0) > 0;
  }

  async releaseRequestLock(id: string): Promise<void> {
    await this.updateRequest(id, { lockedUntil: 0 });
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
        nextRetryAt = ?, lockedUntil = ?, lastError = ?, result = ?, createdAt = ?, updatedAt = ?
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
      next.lockedUntil,
      next.lastError,
      next.result,
      next.createdAt,
      next.updatedAt,
      id,
    );
  }
}