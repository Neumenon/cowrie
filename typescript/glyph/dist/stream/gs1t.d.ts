/**
 * GS1-T (Text framing) reader and writer.
 */
import { Frame } from './types';
export interface WriterOptions {
    /** Whether to compute and include CRC */
    withCRC?: boolean;
}
/**
 * Encode a frame as GS1-T bytes.
 *
 * Format:
 *   @frame{v=1 sid=N seq=N kind=K len=N [crc=X] [base=sha256:X] [final=true]}\n
 *   <payload bytes>\n
 */
export declare function encodeFrame(frame: Frame, options?: WriterOptions): Uint8Array;
/**
 * Encode multiple frames.
 */
export declare function encodeFrames(frames: Frame[], options?: WriterOptions): Uint8Array;
export interface ReaderOptions {
    /** Maximum payload size (default: 64 MiB) */
    maxPayload?: number;
    /** Whether to verify CRC (default: true) */
    verifyCRC?: boolean;
}
/**
 * GS1-T stream reader.
 * Incrementally reads frames from a byte buffer.
 */
export declare class Reader {
    private buffer;
    private offset;
    private readonly maxPayload;
    private readonly verifyCRC;
    constructor(options?: ReaderOptions);
    /**
     * Add data to the internal buffer.
     */
    push(data: Uint8Array): void;
    /**
     * Try to read the next frame.
     * Returns null if not enough data is available.
     * Throws ParseError or CRCMismatchError on errors.
     */
    next(): Frame | null;
    /**
     * Read all available frames.
     */
    readAll(): Frame[];
    private findNewline;
    private parseHeader;
    private tokenize;
}
/**
 * Decode frames from a complete byte buffer.
 */
export declare function decodeFrames(data: Uint8Array, options?: ReaderOptions): Frame[];
/**
 * Decode a single frame from bytes.
 */
export declare function decodeFrame(data: Uint8Array, options?: ReaderOptions): Frame | null;
/**
 * Create a doc frame.
 */
export declare function docFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame;
/**
 * Create a patch frame.
 */
export declare function patchFrame(sid: bigint, seq: bigint, payload: Uint8Array | string, base?: Uint8Array): Frame;
/**
 * Create a row frame.
 */
export declare function rowFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame;
/**
 * Create a UI event frame.
 */
export declare function uiFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame;
/**
 * Create an ack frame.
 */
export declare function ackFrame(sid: bigint, seq: bigint): Frame;
/**
 * Create an error frame.
 */
export declare function errFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame;
/**
 * Create a ping frame.
 */
export declare function pingFrame(sid: bigint, seq: bigint): Frame;
/**
 * Create a pong frame.
 */
export declare function pongFrame(sid: bigint, seq: bigint): Frame;
//# sourceMappingURL=gs1t.d.ts.map