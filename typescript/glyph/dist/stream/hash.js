"use strict";
/**
 * SHA-256 state hash helpers for GS1.
 *
 * Uses the Web Crypto API for SHA-256.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.sha256 = sha256;
exports.sha256Sync = sha256Sync;
exports.stateHashLoose = stateHashLoose;
exports.stateHashLooseSync = stateHashLooseSync;
exports.stateHashBytes = stateHashBytes;
exports.verifyBase = verifyBase;
exports.hashToHex = hashToHex;
exports.hexToHash = hexToHash;
const loose_1 = require("../loose");
/**
 * Compute SHA-256 of the given bytes.
 * Works in both Node.js and browsers.
 */
async function sha256(data) {
    if (typeof crypto !== 'undefined' && crypto.subtle) {
        // Browser or Node 15+
        const hash = await crypto.subtle.digest('SHA-256', data);
        return new Uint8Array(hash);
    }
    // Node.js fallback
    const { createHash } = await Promise.resolve().then(() => __importStar(require('crypto')));
    const hash = createHash('sha256').update(data).digest();
    return new Uint8Array(hash);
}
/**
 * Compute SHA-256 synchronously (Node.js only).
 */
function sha256Sync(data) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { createHash } = require('crypto');
    const hash = createHash('sha256').update(data).digest();
    return new Uint8Array(hash);
}
/**
 * Compute state hash using CanonicalizeLoose.
 * This is: sha256(CanonicalizeLoose(value))
 */
async function stateHashLoose(value) {
    const canonical = (0, loose_1.canonicalizeLoose)(value);
    const encoder = new TextEncoder();
    return sha256(encoder.encode(canonical));
}
/**
 * Compute state hash synchronously (Node.js only).
 */
function stateHashLooseSync(value) {
    const canonical = (0, loose_1.canonicalizeLoose)(value);
    const encoder = new TextEncoder();
    return sha256Sync(encoder.encode(canonical));
}
/**
 * Compute state hash from raw bytes.
 */
async function stateHashBytes(data) {
    return sha256(data);
}
/**
 * Verify that the current state hash matches the expected base.
 */
function verifyBase(current, expected) {
    if (current.length !== expected.length)
        return false;
    for (let i = 0; i < current.length; i++) {
        if (current[i] !== expected[i])
            return false;
    }
    return true;
}
/**
 * Convert a 32-byte hash to lowercase hex string.
 */
function hashToHex(h) {
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
function hexToHash(s) {
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
function hexDigit(c) {
    if (c >= 48 && c <= 57)
        return c - 48; // 0-9
    if (c >= 97 && c <= 102)
        return c - 97 + 10; // a-f
    if (c >= 65 && c <= 70)
        return c - 65 + 10; // A-F
    return -1;
}
//# sourceMappingURL=hash.js.map