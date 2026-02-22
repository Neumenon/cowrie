/**
 * SHA-256 state hash helpers for GS1.
 *
 * Uses the Web Crypto API for SHA-256.
 */
import { GValue } from '../types';
/**
 * Compute SHA-256 of the given bytes.
 * Works in both Node.js and browsers.
 */
export declare function sha256(data: Uint8Array): Promise<Uint8Array>;
/**
 * Compute SHA-256 synchronously (Node.js only).
 */
export declare function sha256Sync(data: Uint8Array): Uint8Array;
/**
 * Compute state hash using CanonicalizeLoose.
 * This is: sha256(CanonicalizeLoose(value))
 */
export declare function stateHashLoose(value: GValue): Promise<Uint8Array>;
/**
 * Compute state hash synchronously (Node.js only).
 */
export declare function stateHashLooseSync(value: GValue): Uint8Array;
/**
 * Compute state hash from raw bytes.
 */
export declare function stateHashBytes(data: Uint8Array): Promise<Uint8Array>;
/**
 * Verify that the current state hash matches the expected base.
 */
export declare function verifyBase(current: Uint8Array, expected: Uint8Array): boolean;
/**
 * Convert a 32-byte hash to lowercase hex string.
 */
export declare function hashToHex(h: Uint8Array): string;
/**
 * Parse a 64-character hex string to a 32-byte hash.
 */
export declare function hexToHash(s: string): Uint8Array | null;
//# sourceMappingURL=hash.d.ts.map