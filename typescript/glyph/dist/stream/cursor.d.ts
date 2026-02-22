/**
 * StreamCursor - Per-SID state tracking for GS1 streams.
 */
import { Frame } from './types';
import { GValue } from '../types';
/**
 * State for a single stream ID.
 */
export interface SIDState {
    sid: bigint;
    lastSeq: bigint;
    lastAcked: bigint;
    stateHash: Uint8Array | null;
    state: GValue | null;
    final: boolean;
}
/**
 * StreamCursor tracks per-SID state for stream processing.
 */
export declare class StreamCursor {
    private cursors;
    /**
     * Get state for a SID, creating it if needed.
     */
    get(sid: bigint): SIDState;
    /**
     * Get state for a SID without creating it.
     */
    getReadOnly(sid: bigint): SIDState | undefined;
    /**
     * Delete state for a SID.
     */
    delete(sid: bigint): void;
    /**
     * Get all tracked SIDs.
     */
    allSIDs(): bigint[];
    /**
     * Process a frame and update cursor state.
     * Throws on sequence gaps, duplicates, or base mismatches.
     */
    processFrame(frame: Frame): void;
    /**
     * Set the current state and compute its hash.
     */
    setState(sid: bigint, value: GValue): void;
    /**
     * Set the state hash directly.
     */
    setStateHash(sid: bigint, hash: Uint8Array): void;
    /**
     * Mark a sequence as acknowledged.
     */
    ack(sid: bigint, seq: bigint): void;
    /**
     * Get sequences that have been seen but not acked.
     */
    pendingAcks(sid: bigint): bigint[];
    /**
     * Check if resync is needed (no state hash).
     */
    needsResync(sid: bigint): boolean;
}
/**
 * Frame handler callbacks.
 */
export interface FrameHandlerCallbacks {
    onDoc?: (sid: bigint, seq: bigint, payload: Uint8Array, state: SIDState) => void;
    onPatch?: (sid: bigint, seq: bigint, payload: Uint8Array, state: SIDState) => void;
    onRow?: (sid: bigint, seq: bigint, payload: Uint8Array, state: SIDState) => void;
    onUI?: (sid: bigint, seq: bigint, payload: Uint8Array, state: SIDState) => void;
    onAck?: (sid: bigint, seq: bigint, state: SIDState) => void;
    onErr?: (sid: bigint, seq: bigint, payload: Uint8Array, state: SIDState) => void;
    onFinal?: (sid: bigint, state: SIDState) => void;
    onSeqGap?: (sid: bigint, expected: bigint, got: bigint) => boolean;
    onBaseMismatch?: (sid: bigint, frame: Frame) => boolean;
}
/**
 * FrameHandler processes frames with state tracking and callbacks.
 */
export declare class FrameHandler {
    readonly cursor: StreamCursor;
    private callbacks;
    constructor(callbacks?: FrameHandlerCallbacks);
    /**
     * Handle a frame and call the appropriate callback.
     */
    handle(frame: Frame): void;
}
//# sourceMappingURL=cursor.d.ts.map