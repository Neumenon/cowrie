/**
 * Standard UI Event Types for GS1 streams.
 * 
 * These are recommended payload schemas for kind=ui frames.
 * They provide a consistent way to stream agent/workflow status.
 */

import { GValue, g, field } from '../types';
import { emit } from '../emit';

const encoder = new TextEncoder();

// ============================================================
// UI Event Constructors
// ============================================================

/**
 * Progress event.
 * Payload: Progress@(pct 0.42 msg "processing step 3")
 */
export function progress(pct: number, msg: string): GValue {
  return g.struct('Progress',
    field('pct', g.float(pct)),
    field('msg', g.str(msg)),
  );
}

/**
 * Log event.
 * Payload: Log@(level "info" msg "decoded 1000 rows" ts "2025-06-20T10:30:00Z")
 */
export function log(level: string, msg: string): GValue {
  return g.struct('Log',
    field('level', g.str(level)),
    field('msg', g.str(msg)),
    field('ts', g.time(new Date())),
  );
}

/** Info-level log */
export function logInfo(msg: string): GValue {
  return log('info', msg);
}

/** Warning-level log */
export function logWarn(msg: string): GValue {
  return log('warn', msg);
}

/** Error-level log */
export function logError(msg: string): GValue {
  return log('error', msg);
}

/** Debug-level log */
export function logDebug(msg: string): GValue {
  return log('debug', msg);
}

/**
 * Metric event.
 * Payload: Metric@(name "latency_ms" value 12.5 unit "ms")
 */
export function metric(name: string, value: number, unit?: string): GValue {
  const fields = [
    field('name', g.str(name)),
    field('value', g.float(value)),
  ];
  if (unit) {
    fields.push(field('unit', g.str(unit)));
  }
  return g.struct('Metric', ...fields);
}

/**
 * Counter metric (integer value).
 */
export function counter(name: string, count: number): GValue {
  return g.struct('Metric',
    field('name', g.str(name)),
    field('value', g.int(count)),
    field('unit', g.str('count')),
  );
}

/**
 * Artifact reference.
 * Payload: Artifact@(mime "image/png" ref "blob:sha256:..." name "plot.png")
 */
export function artifact(mime: string, ref: string, name: string): GValue {
  return g.struct('Artifact',
    field('mime', g.str(mime)),
    field('ref', g.str(ref)),
    field('name', g.str(name)),
  );
}

// ============================================================
// Resync Events
// ============================================================

/**
 * Resync request - sent when receiver needs a fresh snapshot.
 * Payload: ResyncRequest@(sid 1 seq 42 want "sha256:..." reason "BASE_MISMATCH")
 */
export function resyncRequest(sid: bigint, seq: bigint, want: string, reason: string): GValue {
  return g.struct('ResyncRequest',
    field('sid', g.int(Number(sid))),
    field('seq', g.int(Number(seq))),
    field('want', g.str(want)),
    field('reason', g.str(reason)),
  );
}

// ============================================================
// Error Events
// ============================================================

/**
 * Error event for kind=err frames.
 * Payload: Error@(code "BASE_MISMATCH" msg "state hash mismatch" sid 1 seq 42)
 */
export function error(code: string, msg: string, sid: bigint, seq: bigint): GValue {
  return g.struct('Error',
    field('code', g.str(code)),
    field('msg', g.str(msg)),
    field('sid', g.int(Number(sid))),
    field('seq', g.int(Number(seq))),
  );
}

// ============================================================
// Emit Helpers (return Uint8Array for frame payloads)
// ============================================================

/** Emit any UI event value as bytes */
export function emitUI(v: GValue): Uint8Array {
  return encoder.encode(emit(v));
}

/** Emit progress event as bytes */
export function emitProgress(pct: number, msg: string): Uint8Array {
  return emitUI(progress(pct, msg));
}

/** Emit log event as bytes */
export function emitLog(level: string, msg: string): Uint8Array {
  return emitUI(log(level, msg));
}

/** Emit metric event as bytes */
export function emitMetric(name: string, value: number, unit?: string): Uint8Array {
  return emitUI(metric(name, value, unit));
}

/** Emit artifact event as bytes */
export function emitArtifact(mime: string, ref: string, name: string): Uint8Array {
  return emitUI(artifact(mime, ref, name));
}

/** Emit error event as bytes */
export function emitError(code: string, msg: string, sid: bigint, seq: bigint): Uint8Array {
  return emitUI(error(code, msg, sid, seq));
}

/** Emit resync request as bytes */
export function emitResyncRequest(sid: bigint, seq: bigint, want: string, reason: string): Uint8Array {
  return emitUI(resyncRequest(sid, seq, want, reason));
}

// ============================================================
// Parse Helper
// ============================================================

export interface ParsedUIEvent {
  type: string;
  fields: Record<string, unknown>;
}

/**
 * Parse a UI event payload and return its type and fields.
 */
export function parseUIEvent(payload: Uint8Array): ParsedUIEvent {
  const decoder = new TextDecoder();
  const text = decoder.decode(payload);
  
  // Simple parsing - extract type name and fields
  // Format: TypeName@(field1=value1 field2=value2) or TypeName{field1=value1}
  const match = text.match(/^(\w+)[@{]\((.*)\)$/s) || text.match(/^(\w+)\{(.*)\}$/s);
  if (!match) {
    throw new Error(`Invalid UI event format: ${text}`);
  }
  
  const type = match[1];
  const content = match[2];
  const fields: Record<string, unknown> = {};
  
  // Parse key=value pairs (simplified - doesn't handle all edge cases)
  const pairs = content.match(/(\w+)=("[^"]*"|\S+)/g);
  if (pairs) {
    for (const pair of pairs) {
      const [key, ...rest] = pair.split('=');
      let value: unknown = rest.join('=');
      
      // Parse value type
      if (typeof value === 'string') {
        if (value.startsWith('"') && value.endsWith('"')) {
          value = value.slice(1, -1);
        } else if (value === 't' || value === 'true') {
          value = true;
        } else if (value === 'f' || value === 'false') {
          value = false;
        } else if (/^-?\d+$/.test(value)) {
          value = parseInt(value, 10);
        } else if (/^-?\d*\.\d+$/.test(value)) {
          value = parseFloat(value);
        }
      }
      
      fields[key] = value;
    }
  }
  
  return { type, fields };
}
