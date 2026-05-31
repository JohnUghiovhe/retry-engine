import 'dotenv/config';
import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { existsSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { RetryEngine } from './retry-engine';
import { RetryStorage } from './persistence/retry-storage';
import { RequestValidationError } from './validation/request-validation';
import { PayloadTooLargeError, readJsonBody, sendJson } from './utils/http-utils';
import { CreateRequestInput, RequestStatus } from './types';

const demoDbPath = resolve('data/demo.sqlite');
const retryPort = 3001;
const mockPort = 5101;

async function main(): Promise<void> {
  if (existsSync(demoDbPath)) {
    rmSync(demoDbPath);
  }

  const mockState = new Map<string, number>();
  const mockServer = createMockServer(mockState);
  await listen(mockServer, mockPort);
  console.log(`[demo] mock target listening on http://127.0.0.1:${mockPort}`);

  const storage = new RetryStorage(demoDbPath);
  const engine = new RetryEngine(storage, {
    databasePath: demoDbPath,
    workerIntervalMs: 500,
    defaultBackoffMs: 1000,
    defaultTimeoutMs: 5000,
    allowPrivateTargets: true,
    logger: (message) => console.log(message),
  });
  await engine.start();

  const apiServer = createServer((req, res) => {
    void handleApiRequest(req, res, engine);
  });
  await listen(apiServer, retryPort);
  console.log(`[demo] retry engine listening on http://127.0.0.1:${retryPort}`);

  const successResponse = await submitRequest(retryPort, {
    url: `http://127.0.0.1:${mockPort}/flaky-success`,
    method: 'POST',
    body: { scenario: 'success-after-3' },
    maxRetries: 5,
    backoffMs: 300,
  });

  console.log(`\n[demo] success-after-3 request id: ${successResponse.id}`);
  await pollUntilFinished(retryPort, successResponse.id);
  printDetail(await getRequestDetail(retryPort, successResponse.id));

  const clientErrorResponse = await submitRequest(retryPort, {
    url: `http://127.0.0.1:${mockPort}/client-error`,
    method: 'POST',
    body: { scenario: '4xx' },
    maxRetries: 5,
    backoffMs: 300,
  });

  console.log(`\n[demo] 4xx request id: ${clientErrorResponse.id}`);
  await pollUntilFinished(retryPort, clientErrorResponse.id);
  printDetail(await getRequestDetail(retryPort, clientErrorResponse.id));

  const deadLetterResponse = await submitRequest(retryPort, {
    url: `http://127.0.0.1:${mockPort}/always-fail`,
    method: 'POST',
    body: { scenario: 'dead-letter' },
    maxRetries: 2,
    backoffMs: 200,
  });

  console.log(`\n[demo] dead-letter request id: ${deadLetterResponse.id}`);
  await pollUntilFinished(retryPort, deadLetterResponse.id);
  printDetail(await getRequestDetail(retryPort, deadLetterResponse.id));

  console.log('\n[demo] done');
  apiServer.close();
  mockServer.close();
  await engine.stop();
}

function createMockServer(state: Map<string, number>): ReturnType<typeof createServer> {
  return createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`);
    const key = url.pathname;
    const attempts = (state.get(key) ?? 0) + 1;
    state.set(key, attempts);

    if (key === '/flaky-success') {
      if (attempts <= 3) {
        sendJson(res, 503, { ok: false, attempt: attempts, message: 'temporary failure' });
        return;
      }

      sendJson(res, 200, { ok: true, attempt: attempts, message: 'eventual success' });
      return;
    }

    if (key === '/client-error') {
      sendJson(res, 400, { ok: false, attempt: attempts, message: 'validation failed' });
      return;
    }

    if (key === '/always-fail') {
      sendJson(res, 503, { ok: false, attempt: attempts, message: 'persistent outage' });
      return;
    }

    sendJson(res, 404, { error: 'not found' });
  });
}

async function handleApiRequest(req: IncomingMessage, res: ServerResponse, engine: RetryEngine): Promise<void> {
  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`);

  if (req.method === 'POST' && url.pathname === '/request') {
    try {
      const payload = await readJsonBody(req, 1_048_576);
      const created = await engine.submitRequest(payload as CreateRequestInput);
      sendJson(res, 202, { id: created.id, status: created.status });
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
    return;
  }

  if (req.method === 'GET' && url.pathname.startsWith('/requests/')) {
    const id = decodeURIComponent(url.pathname.replace('/requests/', ''));
    const detail = await engine.getRequest(id);
    sendJson(res, detail ? 200 : 404, detail ?? { error: 'not found' });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/requests') {
    const status = url.searchParams.get('status') ?? undefined;
    sendJson(res, 200, { requests: await engine.listRequests(status as RequestStatus | undefined) });
    return;
  }

  sendJson(res, 404, { error: 'not found' });
}

async function submitRequest(port: number, payload: CreateRequestInput): Promise<{ id: string; status: string }> {
  const response = await fetch(`http://127.0.0.1:${port}/request`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });

  return response.json() as Promise<{ id: string; status: string }>;
}

async function getRequestDetail(port: number, id: string): Promise<any> {
  const response = await fetch(`http://127.0.0.1:${port}/requests/${id}`);
  return response.json();
}

async function pollUntilFinished(port: number, id: string): Promise<void> {
  while (true) {
    const detail = await getRequestDetail(port, id);
    const status = detail.request.status;
    if (status === 'completed' || status === 'failed') {
      return;
    }

    await delay(250);
  }
}

function printDetail(detail: any): void {
  console.log(JSON.stringify(detail, null, 2));
}

function listen(server: ReturnType<typeof createServer>, port: number): Promise<void> {
  return new Promise((resolve) => {
    server.listen(port, resolve);
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});