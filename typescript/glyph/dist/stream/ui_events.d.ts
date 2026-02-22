/**
 * Standard UI Event Types for GS1 streams.
 *
 * These are recommended payload schemas for kind=ui frames.
 * They provide a consistent way to stream agent/workflow status.
 */
import { GValue } from '../types';
/**
 * Progress event.
 * Payload: Progress@(pct 0.42 msg "processing step 3")
 */
export declare function progress(pct: number, msg: string): GValue;
/**
 * Log event.
 * Payload: Log@(level "info" msg "decoded 1000 rows" ts "2025-06-20T10:30:00Z")
 */
export declare function log(level: string, msg: string): GValue;
/** Info-level log */
export declare function logInfo(msg: string): GValue;
/** Warning-level log */
export declare function logWarn(msg: string): GValue;
/** Error-level log */
export declare function logError(msg: string): GValue;
/** Debug-level log */
export declare function logDebug(msg: string): GValue;
/**
 * Metric event.
 * Payload: Metric@(name "latency_ms" value 12.5 unit "ms")
 */
export declare function metric(name: string, value: number, unit?: string): GValue;
/**
 * Counter metric (integer value).
 */
export declare function counter(name: string, count: number): GValue;
/**
 * Artifact reference.
 * Payload: Artifact@(mime "image/png" ref "blob:sha256:..." name "plot.png")
 */
export declare function artifact(mime: string, ref: string, name: string): GValue;
/**
 * Resync request - sent when receiver needs a fresh snapshot.
 * Payload: ResyncRequest@(sid 1 seq 42 want "sha256:..." reason "BASE_MISMATCH")
 */
export declare function resyncRequest(sid: bigint, seq: bigint, want: string, reason: string): GValue;
/**
 * Error event for kind=err frames.
 * Payload: Error@(code "BASE_MISMATCH" msg "state hash mismatch" sid 1 seq 42)
 */
export declare function error(code: string, msg: string, sid: bigint, seq: bigint): GValue;
/** Emit any UI event value as bytes */
export declare function emitUI(v: GValue): Uint8Array;
/** Emit progress event as bytes */
export declare function emitProgress(pct: number, msg: string): Uint8Array;
/** Emit log event as bytes */
export declare function emitLog(level: string, msg: string): Uint8Array;
/** Emit metric event as bytes */
export declare function emitMetric(name: string, value: number, unit?: string): Uint8Array;
/** Emit artifact event as bytes */
export declare function emitArtifact(mime: string, ref: string, name: string): Uint8Array;
/** Emit error event as bytes */
export declare function emitError(code: string, msg: string, sid: bigint, seq: bigint): Uint8Array;
/** Emit resync request as bytes */
export declare function emitResyncRequest(sid: bigint, seq: bigint, want: string, reason: string): Uint8Array;
export interface ParsedUIEvent {
    type: string;
    fields: Record<string, unknown>;
}
/**
 * Parse a UI event payload and return its type and fields.
 */
export declare function parseUIEvent(payload: Uint8Array): ParsedUIEvent;
//# sourceMappingURL=ui_events.d.ts.map