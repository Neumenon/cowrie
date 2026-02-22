/**
 * StreamCursor - Per-SID state tracking for GS1 streams.
 */

import { Frame, FrameKind, BaseMismatchError } from './types';
import { verifyBase, stateHashLooseSync } from './hash';
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
export class StreamCursor {
  private cursors = new Map<bigint, SIDState>();

  /**
   * Get state for a SID, creating it if needed.
   */
  get(sid: bigint): SIDState {
    let state = this.cursors.get(sid);
    if (!state) {
      state = {
        sid,
        lastSeq: 0n,
        lastAcked: 0n,
        stateHash: null,
        state: null,
        final: false,
      };
      this.cursors.set(sid, state);
    }
    return state;
  }

  /**
   * Get state for a SID without creating it.
   */
  getReadOnly(sid: bigint): SIDState | undefined {
    return this.cursors.get(sid);
  }

  /**
   * Delete state for a SID.
   */
  delete(sid: bigint): void {
    this.cursors.delete(sid);
  }

  /**
   * Get all tracked SIDs.
   */
  allSIDs(): bigint[] {
    return Array.from(this.cursors.keys());
  }

  /**
   * Process a frame and update cursor state.
   * Throws on sequence gaps, duplicates, or base mismatches.
   */
  processFrame(frame: Frame): void {
    const state = this.get(frame.sid);

    // Check sequence monotonicity
    if (frame.seq !== 0n && frame.seq <= state.lastSeq) {
      throw new Error(`sequence not monotonic: got ${frame.seq}, last was ${state.lastSeq}`);
    }

    // Check for gaps
    if (state.lastSeq > 0n && frame.seq !== state.lastSeq + 1n) {
      throw new Error(`sequence gap: expected ${state.lastSeq + 1n}, got ${frame.seq}`);
    }

    // For patches with base, verify state hash
    if (frame.kind === 'patch' && frame.base && state.stateHash) {
      if (!verifyBase(state.stateHash, frame.base)) {
        throw new BaseMismatchError();
      }
    }

    // Update sequence
    state.lastSeq = frame.seq;

    // Update final flag
    if (frame.final) {
      state.final = true;
    }
  }

  /**
   * Set the current state and compute its hash.
   */
  setState(sid: bigint, value: GValue): void {
    const state = this.get(sid);
    state.state = value;
    state.stateHash = stateHashLooseSync(value);
  }

  /**
   * Set the state hash directly.
   */
  setStateHash(sid: bigint, hash: Uint8Array): void {
    const state = this.get(sid);
    state.stateHash = hash;
  }

  /**
   * Mark a sequence as acknowledged.
   */
  ack(sid: bigint, seq: bigint): void {
    const state = this.get(sid);
    if (seq > state.lastAcked) {
      state.lastAcked = seq;
    }
  }

  /**
   * Get sequences that have been seen but not acked.
   */
  pendingAcks(sid: bigint): bigint[] {
    const state = this.getReadOnly(sid);
    if (!state || state.lastSeq <= state.lastAcked) {
      return [];
    }

    const pending: bigint[] = [];
    for (let seq = state.lastAcked + 1n; seq <= state.lastSeq; seq++) {
      pending.push(seq);
    }
    return pending;
  }

  /**
   * Check if resync is needed (no state hash).
   */
  needsResync(sid: bigint): boolean {
    const state = this.getReadOnly(sid);
    return !state || !state.stateHash;
  }
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
  onSeqGap?: (sid: bigint, expected: bigint, got: bigint) => boolean; // Return true to allow
  onBaseMismatch?: (sid: bigint, frame: Frame) => boolean; // Return true to allow
}

/**
 * FrameHandler processes frames with state tracking and callbacks.
 */
export class FrameHandler {
  readonly cursor: StreamCursor;
  private callbacks: FrameHandlerCallbacks;

  constructor(callbacks: FrameHandlerCallbacks = {}) {
    this.cursor = new StreamCursor();
    this.callbacks = callbacks;
  }

  /**
   * Handle a frame and call the appropriate callback.
   */
  handle(frame: Frame): void {
    const state = this.cursor.get(frame.sid);

    // Check sequence
    if (frame.seq !== 0n && state.lastSeq > 0n) {
      if (frame.seq <= state.lastSeq) {
        // Duplicate - skip
        return;
      }
      if (frame.seq !== state.lastSeq + 1n) {
        // Gap detected
        if (this.callbacks.onSeqGap) {
          const allow = this.callbacks.onSeqGap(frame.sid, state.lastSeq + 1n, frame.seq);
          if (!allow) return;
        }
      }
    }

    // Check base for patches
    if (frame.kind === 'patch' && frame.base && state.stateHash) {
      if (!verifyBase(state.stateHash, frame.base)) {
        if (this.callbacks.onBaseMismatch) {
          const allow = this.callbacks.onBaseMismatch(frame.sid, frame);
          if (!allow) return;
        } else {
          throw new BaseMismatchError();
        }
      }
    }

    // Update sequence
    state.lastSeq = frame.seq;

    // Dispatch to callback
    switch (frame.kind) {
      case 'doc':
        this.callbacks.onDoc?.(frame.sid, frame.seq, frame.payload, state);
        break;
      case 'patch':
        this.callbacks.onPatch?.(frame.sid, frame.seq, frame.payload, state);
        break;
      case 'row':
        this.callbacks.onRow?.(frame.sid, frame.seq, frame.payload, state);
        break;
      case 'ui':
        this.callbacks.onUI?.(frame.sid, frame.seq, frame.payload, state);
        break;
      case 'ack':
        this.callbacks.onAck?.(frame.sid, frame.seq, state);
        break;
      case 'err':
        this.callbacks.onErr?.(frame.sid, frame.seq, frame.payload, state);
        break;
    }

    // Handle final
    if (frame.final) {
      state.final = true;
      this.callbacks.onFinal?.(frame.sid, state);
    }
  }
}
