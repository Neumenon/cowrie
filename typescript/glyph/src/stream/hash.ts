/**
 * SHA-256 state hash helpers for GS1.
 * 
 * Uses the Web Crypto API for SHA-256.
 */

import { canonicalizeLoose } from '../loose';
import { GValue } from '../types';

/**
 * Compute SHA-256 of the given bytes.
 * Works in both Node.js and browsers.
 */
export async function sha256(data: Uint8Array): Promise<Uint8Array> {
  if (typeof crypto !== 'undefined' && crypto.subtle) {
    // Browser or Node 15+
    const hash = await crypto.subtle.digest('SHA-256', data);
    return new Uint8Array(hash);
  }
  
  // Node.js fallback
  const { createHash } = await import('crypto');
  const hash = createHash('sha256').update(data).digest();
  return new Uint8Array(hash);
}

/**
 * Compute SHA-256 synchronously (Node.js only).
 */
export function sha256Sync(data: Uint8Array): Uint8Array {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { createHash } = require('crypto');
  const hash = createHash('sha256').update(data).digest();
  return new Uint8Array(hash);
}

/**
 * Compute state hash using CanonicalizeLoose.
 * This is: sha256(CanonicalizeLoose(value))
 */
export async function stateHashLoose(value: GValue): Promise<Uint8Array> {
  const canonical = canonicalizeLoose(value);
  const encoder = new TextEncoder();
  return sha256(encoder.encode(canonical));
}

/**
 * Compute state hash synchronously (Node.js only).
 */
export function stateHashLooseSync(value: GValue): Uint8Array {
  const canonical = canonicalizeLoose(value);
  const encoder = new TextEncoder();
  return sha256Sync(encoder.encode(canonical));
}

/**
 * Compute state hash from raw bytes.
 */
export async function stateHashBytes(data: Uint8Array): Promise<Uint8Array> {
  return sha256(data);
}

/**
 * Verify that the current state hash matches the expected base.
 */
export function verifyBase(current: Uint8Array, expected: Uint8Array): boolean {
  if (current.length !== expected.length) return false;
  for (let i = 0; i < current.length; i++) {
    if (current[i] !== expected[i]) return false;
  }
  return true;
}

/**
 * Convert a 32-byte hash to lowercase hex string.
 */
export function hashToHex(h: Uint8Array): string {
  const hex = '0123456789abcdef';
  let result = '';
  for (let i = 0; i < h.length; i++) {
    result += hex[h[i] >> 4];
    result += hex[h[i] & 0x0f];
  }
  return result;
}

/**
 * Parse a 64-character hex string to a 32-byte hash.
 */
export function hexToHash(s: string): Uint8Array | null {
  // Strip optional prefix
  if (s.startsWith('sha256:')) {
    s = s.slice(7);
  }
  
  if (s.length !== 64) {
    return null;
  }
  
  const hash = new Uint8Array(32);
  for (let i = 0; i < 32; i++) {
    const hi = hexDigit(s.charCodeAt(i * 2));
    const lo = hexDigit(s.charCodeAt(i * 2 + 1));
    if (hi < 0 || lo < 0) {
      return null;
    }
    hash[i] = (hi << 4) | lo;
  }
  return hash;
}

function hexDigit(c: number): number {
  if (c >= 48 && c <= 57) return c - 48;      // 0-9
  if (c >= 97 && c <= 102) return c - 97 + 10; // a-f
  if (c >= 65 && c <= 70) return c - 65 + 10;  // A-F
  return -1;
}
