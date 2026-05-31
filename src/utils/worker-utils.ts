export function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.floor(value);
}

export async function runWithConcurrency<T>(items: T[], concurrencyLimit: number, handler: (item: T) => Promise<void>): Promise<void> {
  const queue = [...items];
  const workerCount = Math.max(1, Math.min(concurrencyLimit, queue.length));
  const workers = Array.from({ length: workerCount }, async () => {
    while (queue.length > 0) {
      const item = queue.shift();
      if (!item) {
        return;
      }

      await handler(item);
    }
  });

  await Promise.all(workers);
}