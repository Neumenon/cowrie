/**
 * CRC-32 IEEE implementation for GS1.
 */
/**
 * Compute CRC-32 IEEE of the given bytes.
 */
export declare function computeCRC(data: Uint8Array): number;
/**
 * Verify that the CRC matches.
 */
export declare function verifyCRC(data: Uint8Array, expected: number): boolean;
/**
 * Convert CRC to 8-character lowercase hex string.
 */
export declare function crcToHex(crc: number): string;
/**
 * Parse CRC from hex string (with optional crc32: prefix).
 */
export declare function parseCRC(s: string): number | null;
//# sourceMappingURL=crc.d.ts.map