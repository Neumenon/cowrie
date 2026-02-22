"use strict";
/**
 * CRC-32 IEEE implementation for GS1.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.computeCRC = computeCRC;
exports.verifyCRC = verifyCRC;
exports.crcToHex = crcToHex;
exports.parseCRC = parseCRC;
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
            }
            else {
                crc = crc >>> 1;
            }
        }
        CRC_TABLE[i] = crc >>> 0;
    }
})();
/**
 * Compute CRC-32 IEEE of the given bytes.
 */
function computeCRC(data) {
    let crc = 0xFFFFFFFF;
    for (let i = 0; i < data.length; i++) {
        crc = CRC_TABLE[(crc ^ data[i]) & 0xFF] ^ (crc >>> 8);
    }
    return (crc ^ 0xFFFFFFFF) >>> 0;
}
/**
 * Verify that the CRC matches.
 */
function verifyCRC(data, expected) {
    return computeCRC(data) === expected;
}
/**
 * Convert CRC to 8-character lowercase hex string.
 */
function crcToHex(crc) {
    return crc.toString(16).padStart(8, '0');
}
/**
 * Parse CRC from hex string (with optional crc32: prefix).
 */
function parseCRC(s) {
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
//# sourceMappingURL=crc.js.map