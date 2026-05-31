import 'dotenv/config';
import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { RetryEngine } from './retry-engine';
import { RetryStorage } from './persistence/retry-storage';
import { RequestValidationError } from './validation/request-validation';
import { PayloadTooLargeError, parseAllowedHosts, readJsonBody, sendJson } from './utils/http-utils';
import { normalizePositiveInteger } from './utils/worker-utils';
import { RequestStatus } from './types';

const port = normalizePositiveInteger(process.env.PORT ? Number(process.env.PORT) : undefined, 3000);
const databasePath = process.env.DATABASE_PATH ?? 'data/retry-engine.sqlite';
const maxRequestBodyBytes = normalizePositiveInteger(
  process.env.MAX_REQUEST_BODY_BYTES ? Number(process.env.MAX_REQUEST_BODY_BYTES) : undefined,
  1_048_576,
);
const allowPrivateTargets = process.env.ALLOW_PRIVATE_TARGETS === 'true';
const allowedHosts = parseAllowedHosts(process.env.ALLOWED_TARGET_HOSTS);

const storage = new RetryStorage(databasePath);
const engine = new RetryEngine(storage, {
  databasePath,
  workerIntervalMs: 500,
  defaultBackoffMs: 1000,
  defaultTimeoutMs: 5000,
  allowPrivateTargets,
  allowedHosts,
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
      const payload = await readJsonBody(req, maxRequestBodyBytes);
      const created = await engine.submitRequest(payload as Parameters<RetryEngine['submitRequest']>[0]);
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
    if (error instanceof RequestValidationError) {
      sendJson(res, error.statusCode, { error: error.message });
      return;
    }

    if (error instanceof SyntaxError) {
      sendJson(res, 400, { error: 'invalid JSON body' });
      return;
    }

    if (error instanceof PayloadTooLargeError) {
      sendJson(res, 413, { error: error.message });
      return;
    }

    sendJson(res, 500, { error: error instanceof Error ? error.message : 'internal server error' });
  }
}

function isRequestStatus(value: string): value is RequestStatus {
  return value === 'pending' || value === 'retrying' || value === 'completed' || value === 'failed';
}

if (require.main === module) {
  void startServer();
}