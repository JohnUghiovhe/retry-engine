import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { RetryEngine, RetryStorage } from './retry-engine';
import { CreateRequestInput, RequestStatus } from './types';

const port = normalizePositiveInteger(process.env.PORT ? Number(process.env.PORT) : undefined, 3000);
const databasePath = process.env.DATABASE_PATH ?? 'data/retry-engine.sqlite';

const storage = new RetryStorage(databasePath);
const engine = new RetryEngine(storage, {
  databasePath,
  workerIntervalMs: 500,
  defaultBackoffMs: 1000,
  defaultTimeoutMs: 5000,
  logger: (message) => console.log(message),
});

export async function startServer(): Promise<void> {
  await engine.start();

  const server = createServer((req, res) => {
    void handleRequest(req, res);
  });

  server.listen(port, () => {
    console.log(`retry engine listening on http://127.0.0.1:${port}`);
  });

  process.once('SIGINT', async () => {
    await engine.stop();
    server.close(() => process.exit(0));
  });

  process.once('SIGTERM', async () => {
    await engine.stop();
    server.close(() => process.exit(0));
  });
}

async function handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
  try {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`);

    if (req.method === 'POST' && url.pathname === '/request') {
      const payload = await readJsonBody(req);
      const validationError = validateCreateRequest(payload);
      if (validationError) {
        sendJson(res, 400, { error: validationError });
        return;
      }

      const created = await engine.submitRequest(payload as CreateRequestInput);
      sendJson(res, 202, { id: created.id, status: created.status });
      return;
    }

    if (req.method === 'GET' && url.pathname.startsWith('/requests/')) {
      const id = decodeURIComponent(url.pathname.replace('/requests/', ''));
      const detail = await engine.getRequest(id);
      if (!detail) {
        sendJson(res, 404, { error: 'request not found' });
        return;
      }

      sendJson(res, 200, detail);
      return;
    }

    if (req.method === 'GET' && url.pathname === '/requests') {
      const status = url.searchParams.get('status') ?? undefined;
      if (status && !isRequestStatus(status)) {
        sendJson(res, 400, { error: 'invalid status filter' });
        return;
      }

      const requests = await engine.listRequests(status as RequestStatus | undefined);
      sendJson(res, 200, { requests });
      return;
    }

    sendJson(res, 404, { error: 'not found' });
  } catch (error) {
    sendJson(res, 500, { error: error instanceof Error ? error.message : 'internal server error' });
  }
}

async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  if (chunks.length === 0) {
    return {};
  }

  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function validateCreateRequest(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') {
    return 'body must be a JSON object';
  }

  const candidate = payload as Partial<CreateRequestInput>;
  if (typeof candidate.url !== 'string' || candidate.url.trim().length === 0) {
    return 'url is required';
  }

  if (typeof candidate.method !== 'string' || candidate.method.trim().length === 0) {
    return 'method is required';
  }

  try {
    new URL(candidate.url);
  } catch {
    return 'url must be valid';
  }

  return null;
}

function isRequestStatus(value: string): value is RequestStatus {
  return value === 'pending' || value === 'retrying' || value === 'completed' || value === 'failed';
}

function sendJson(res: ServerResponse, statusCode: number, payload: unknown): void {
  const body = JSON.stringify(payload, null, 2);
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
}

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.floor(value);
}

if (require.main === module) {
  void startServer();
}