import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';

export interface TargetSafetyOptions {
  allowPrivateTargets?: boolean;
  allowedHosts?: string[];
}

export function validateTargetUrlSync(url: URL, options: TargetSafetyOptions = {}): string | null {
  if (options.allowPrivateTargets) {
    return null;
  }

  const hostname = url.hostname.toLowerCase();

  if (hostname === 'localhost' || hostname.endsWith('.localhost')) {
    return 'localhost targets are not allowed';
  }

  if (isIP(hostname) && isPrivateOrLinkLocalIp(hostname)) {
    return 'private or link-local IP targets are not allowed';
  }

  if (options.allowedHosts && options.allowedHosts.length > 0) {
    const normalizedAllowedHosts = new Set(options.allowedHosts.map((host) => host.toLowerCase()));
    if (!normalizedAllowedHosts.has(hostname)) {
      return 'url host is not allowlisted';
    }
  }

  return null;
}

export async function validateTargetUrl(url: string, options: TargetSafetyOptions = {}): Promise<string | null> {
  const parsedUrl = new URL(url);
  if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
    return 'url must use http or https';
  }

  if (options.allowPrivateTargets) {
    return null;
  }

  const syncValidation = validateTargetUrlSync(parsedUrl, options);
  if (syncValidation) {
    return syncValidation;
  }

  if (isIP(parsedUrl.hostname)) {
    return null;
  }

  try {
    const resolved = await lookup(parsedUrl.hostname, { all: true });
    for (const record of resolved) {
      if (isPrivateOrLinkLocalIp(record.address)) {
        return 'private or link-local IP targets are not allowed';
      }
    }
  } catch {
    return 'url host could not be resolved';
  }

  return null;
}

function isPrivateOrLinkLocalIp(address: string): boolean {
  if (isIP(address) === 4) {
    const [a, b] = address.split('.').map(Number);
    if (a === 10 || a === 127) return true;
    if (a === 169 && b === 254) return true;
    if (a === 192 && b === 168) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    return false;
  }

  if (isIP(address) === 6) {
    const lower = address.toLowerCase();
    return lower === '::1' || lower.startsWith('fc') || lower.startsWith('fd') || lower.startsWith('fe80:');
  }

  return false;
}