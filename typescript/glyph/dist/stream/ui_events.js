"use strict";
/**
 * Standard UI Event Types for GS1 streams.
 *
 * These are recommended payload schemas for kind=ui frames.
 * They provide a consistent way to stream agent/workflow status.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.progress = progress;
exports.log = log;
exports.logInfo = logInfo;
exports.logWarn = logWarn;
exports.logError = logError;
exports.logDebug = logDebug;
exports.metric = metric;
exports.counter = counter;
exports.artifact = artifact;
exports.resyncRequest = resyncRequest;
exports.error = error;
exports.emitUI = emitUI;
exports.emitProgress = emitProgress;
exports.emitLog = emitLog;
exports.emitMetric = emitMetric;
exports.emitArtifact = emitArtifact;
exports.emitError = emitError;
exports.emitResyncRequest = emitResyncRequest;
exports.parseUIEvent = parseUIEvent;
const types_1 = require("../types");
const emit_1 = require("../emit");
const encoder = new TextEncoder();
// ============================================================
// UI Event Constructors
// ============================================================
/**
 * Progress event.
 * Payload: Progress@(pct 0.42 msg "processing step 3")
 */
function progress(pct, msg) {
    return types_1.g.struct('Progress', (0, types_1.field)('pct', types_1.g.float(pct)), (0, types_1.field)('msg', types_1.g.str(msg)));
}
/**
 * Log event.
 * Payload: Log@(level "info" msg "decoded 1000 rows" ts "2025-06-20T10:30:00Z")
 */
function log(level, msg) {
    return types_1.g.struct('Log', (0, types_1.field)('level', types_1.g.str(level)), (0, types_1.field)('msg', types_1.g.str(msg)), (0, types_1.field)('ts', types_1.g.time(new Date())));
}
/** Info-level log */
function logInfo(msg) {
    return log('info', msg);
}
/** Warning-level log */
function logWarn(msg) {
    return log('warn', msg);
}
/** Error-level log */
function logError(msg) {
    return log('error', msg);
}
/** Debug-level log */
function logDebug(msg) {
    return log('debug', msg);
}
/**
 * Metric event.
 * Payload: Metric@(name "latency_ms" value 12.5 unit "ms")
 */
function metric(name, value, unit) {
    const fields = [
        (0, types_1.field)('name', types_1.g.str(name)),
        (0, types_1.field)('value', types_1.g.float(value)),
    ];
    if (unit) {
        fields.push((0, types_1.field)('unit', types_1.g.str(unit)));
    }
    return types_1.g.struct('Metric', ...fields);
}
/**
 * Counter metric (integer value).
 */
function counter(name, count) {
    return types_1.g.struct('Metric', (0, types_1.field)('name', types_1.g.str(name)), (0, types_1.field)('value', types_1.g.int(count)), (0, types_1.field)('unit', types_1.g.str('count')));
}
/**
 * Artifact reference.
 * Payload: Artifact@(mime "image/png" ref "blob:sha256:..." name "plot.png")
 */
function artifact(mime, ref, name) {
    return types_1.g.struct('Artifact', (0, types_1.field)('mime', types_1.g.str(mime)), (0, types_1.field)('ref', types_1.g.str(ref)), (0, types_1.field)('name', types_1.g.str(name)));
}
// ============================================================
// Resync Events
// ============================================================
/**
 * Resync request - sent when receiver needs a fresh snapshot.
 * Payload: ResyncRequest@(sid 1 seq 42 want "sha256:..." reason "BASE_MISMATCH")
 */
function resyncRequest(sid, seq, want, reason) {
    return types_1.g.struct('ResyncRequest', (0, types_1.field)('sid', types_1.g.int(Number(sid))), (0, types_1.field)('seq', types_1.g.int(Number(seq))), (0, types_1.field)('want', types_1.g.str(want)), (0, types_1.field)('reason', types_1.g.str(reason)));
}
// ============================================================
// Error Events
// ============================================================
/**
 * Error event for kind=err frames.
 * Payload: Error@(code "BASE_MISMATCH" msg "state hash mismatch" sid 1 seq 42)
 */
function error(code, msg, sid, seq) {
    return types_1.g.struct('Error', (0, types_1.field)('code', types_1.g.str(code)), (0, types_1.field)('msg', types_1.g.str(msg)), (0, types_1.field)('sid', types_1.g.int(Number(sid))), (0, types_1.field)('seq', types_1.g.int(Number(seq))));
}
// ============================================================
// Emit Helpers (return Uint8Array for frame payloads)
// ============================================================
/** Emit any UI event value as bytes */
function emitUI(v) {
    return encoder.encode((0, emit_1.emit)(v));
}
/** Emit progress event as bytes */
function emitProgress(pct, msg) {
    return emitUI(progress(pct, msg));
}
/** Emit log event as bytes */
function emitLog(level, msg) {
    return emitUI(log(level, msg));
}
/** Emit metric event as bytes */
function emitMetric(name, value, unit) {
    return emitUI(metric(name, value, unit));
}
/** Emit artifact event as bytes */
function emitArtifact(mime, ref, name) {
    return emitUI(artifact(mime, ref, name));
}
/** Emit error event as bytes */
function emitError(code, msg, sid, seq) {
    return emitUI(error(code, msg, sid, seq));
}
/** Emit resync request as bytes */
function emitResyncRequest(sid, seq, want, reason) {
    return emitUI(resyncRequest(sid, seq, want, reason));
}
/**
 * Parse a UI event payload and return its type and fields.
 */
function parseUIEvent(payload) {
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
    const fields = {};
    // Parse key=value pairs (simplified - doesn't handle all edge cases)
    const pairs = content.match(/(\w+)=("[^"]*"|\S+)/g);
    if (pairs) {
        for (const pair of pairs) {
            const [key, ...rest] = pair.split('=');
            let value = rest.join('=');
            // Parse value type
            if (typeof value === 'string') {
                if (value.startsWith('"') && value.endsWith('"')) {
                    value = value.slice(1, -1);
                }
                else if (value === 't' || value === 'true') {
                    value = true;
                }
                else if (value === 'f' || value === 'false') {
                    value = false;
                }
                else if (/^-?\d+$/.test(value)) {
                    value = parseInt(value, 10);
                }
                else if (/^-?\d*\.\d+$/.test(value)) {
                    value = parseFloat(value);
                }
            }
            fields[key] = value;
        }
    }
    return { type, fields };
}
//# sourceMappingURL=ui_events.js.map