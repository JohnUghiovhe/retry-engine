export function parseJsonOrText(value: string | null): unknown {
  if (value === null) {
    return null;
  }

  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

export function serializeBody(body: unknown): string | null {
  if (body === undefined) {
    return null;
  }

  return JSON.stringify(body);
}