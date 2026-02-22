/**
 * CRC-32 IEEE implementation for GS1.
 */

// CRC-32 IEEE lookup table
const CRC_TABLE = new Uint32Array(256);

// Initialize CRC table
(function initCRCTable() {
  const polynomial = 0xEDB88320;
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >>> 1) ^ polynomial;
      } else {
        crc = crc >>> 1;
      }
    }
    CRC_TABLE[i] = crc >>> 0;
  }
})();

/**
 * Compute CRC-32 IEEE of the given bytes.
 */
export function computeCRC(data: Uint8Array): number {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < data.length; i++) {
    crc = CRC_TABLE[(crc ^ data[i]) & 0xFF] ^ (crc >>> 8);
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

/**
 * Verify that the CRC matches.
 */
export function verifyCRC(data: Uint8Array, expected: number): boolean {
  return computeCRC(data) === expected;
}

/**
 * Convert CRC to 8-character lowercase hex string.
 */
export function crcToHex(crc: number): string {
  return crc.toString(16).padStart(8, '0');
}

/**
 * Parse CRC from hex string (with optional crc32: prefix).
 */
export function parseCRC(s: string): number | null {
  // Strip optional prefix
  if (s.startsWith('crc32:')) {
    s = s.slice(6);
  }
  if (s.length !== 8) {
    return null;
  }
  const n = parseInt(s, 16);
  if (isNaN(n)) {
    return null;
  }
  return n >>> 0;
}
