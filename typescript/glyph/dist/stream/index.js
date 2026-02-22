"use strict";
/**
 * GS1 (GLYPH Stream v1) - Stream framing protocol for GLYPH payloads.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.emitProgress = exports.emitUI = exports.error = exports.resyncRequest = exports.artifact = exports.counter = exports.metric = exports.logDebug = exports.logError = exports.logWarn = exports.logInfo = exports.log = exports.progress = exports.FrameHandler = exports.StreamCursor = exports.pongFrame = exports.pingFrame = exports.errFrame = exports.ackFrame = exports.uiFrame = exports.rowFrame = exports.patchFrame = exports.docFrame = exports.decodeFrames = exports.decodeFrame = exports.encodeFrames = exports.encodeFrame = exports.Reader = exports.hexToHash = exports.hashToHex = exports.verifyBase = exports.stateHashBytes = exports.stateHashLooseSync = exports.stateHashLoose = exports.sha256Sync = exports.sha256 = exports.parseCRC = exports.crcToHex = exports.verifyCRC = exports.computeCRC = exports.kindToString = exports.parseKind = exports.BaseMismatchError = exports.CRCMismatchError = exports.ParseError = exports.MAX_PAYLOAD_SIZE = exports.FLAGS = exports.VALUE_KINDS = exports.KIND_VALUES = exports.VERSION = void 0;
exports.parseUIEvent = exports.emitResyncRequest = exports.emitError = exports.emitArtifact = exports.emitMetric = exports.emitLog = void 0;
// Types
var types_1 = require("./types");
Object.defineProperty(exports, "VERSION", { enumerable: true, get: function () { return types_1.VERSION; } });
Object.defineProperty(exports, "KIND_VALUES", { enumerable: true, get: function () { return types_1.KIND_VALUES; } });
Object.defineProperty(exports, "VALUE_KINDS", { enumerable: true, get: function () { return types_1.VALUE_KINDS; } });
Object.defineProperty(exports, "FLAGS", { enumerable: true, get: function () { return types_1.FLAGS; } });
Object.defineProperty(exports, "MAX_PAYLOAD_SIZE", { enumerable: true, get: function () { return types_1.MAX_PAYLOAD_SIZE; } });
Object.defineProperty(exports, "ParseError", { enumerable: true, get: function () { return types_1.ParseError; } });
Object.defineProperty(exports, "CRCMismatchError", { enumerable: true, get: function () { return types_1.CRCMismatchError; } });
Object.defineProperty(exports, "BaseMismatchError", { enumerable: true, get: function () { return types_1.BaseMismatchError; } });
Object.defineProperty(exports, "parseKind", { enumerable: true, get: function () { return types_1.parseKind; } });
Object.defineProperty(exports, "kindToString", { enumerable: true, get: function () { return types_1.kindToString; } });
// CRC
var crc_1 = require("./crc");
Object.defineProperty(exports, "computeCRC", { enumerable: true, get: function () { return crc_1.computeCRC; } });
Object.defineProperty(exports, "verifyCRC", { enumerable: true, get: function () { return crc_1.verifyCRC; } });
Object.defineProperty(exports, "crcToHex", { enumerable: true, get: function () { return crc_1.crcToHex; } });
Object.defineProperty(exports, "parseCRC", { enumerable: true, get: function () { return crc_1.parseCRC; } });
// Hash
var hash_1 = require("./hash");
Object.defineProperty(exports, "sha256", { enumerable: true, get: function () { return hash_1.sha256; } });
Object.defineProperty(exports, "sha256Sync", { enumerable: true, get: function () { return hash_1.sha256Sync; } });
Object.defineProperty(exports, "stateHashLoose", { enumerable: true, get: function () { return hash_1.stateHashLoose; } });
Object.defineProperty(exports, "stateHashLooseSync", { enumerable: true, get: function () { return hash_1.stateHashLooseSync; } });
Object.defineProperty(exports, "stateHashBytes", { enumerable: true, get: function () { return hash_1.stateHashBytes; } });
Object.defineProperty(exports, "verifyBase", { enumerable: true, get: function () { return hash_1.verifyBase; } });
Object.defineProperty(exports, "hashToHex", { enumerable: true, get: function () { return hash_1.hashToHex; } });
Object.defineProperty(exports, "hexToHash", { enumerable: true, get: function () { return hash_1.hexToHash; } });
// GS1-T Reader/Writer
var gs1t_1 = require("./gs1t");
Object.defineProperty(exports, "Reader", { enumerable: true, get: function () { return gs1t_1.Reader; } });
Object.defineProperty(exports, "encodeFrame", { enumerable: true, get: function () { return gs1t_1.encodeFrame; } });
Object.defineProperty(exports, "encodeFrames", { enumerable: true, get: function () { return gs1t_1.encodeFrames; } });
Object.defineProperty(exports, "decodeFrame", { enumerable: true, get: function () { return gs1t_1.decodeFrame; } });
Object.defineProperty(exports, "decodeFrames", { enumerable: true, get: function () { return gs1t_1.decodeFrames; } });
// Frame constructors
Object.defineProperty(exports, "docFrame", { enumerable: true, get: function () { return gs1t_1.docFrame; } });
Object.defineProperty(exports, "patchFrame", { enumerable: true, get: function () { return gs1t_1.patchFrame; } });
Object.defineProperty(exports, "rowFrame", { enumerable: true, get: function () { return gs1t_1.rowFrame; } });
Object.defineProperty(exports, "uiFrame", { enumerable: true, get: function () { return gs1t_1.uiFrame; } });
Object.defineProperty(exports, "ackFrame", { enumerable: true, get: function () { return gs1t_1.ackFrame; } });
Object.defineProperty(exports, "errFrame", { enumerable: true, get: function () { return gs1t_1.errFrame; } });
Object.defineProperty(exports, "pingFrame", { enumerable: true, get: function () { return gs1t_1.pingFrame; } });
Object.defineProperty(exports, "pongFrame", { enumerable: true, get: function () { return gs1t_1.pongFrame; } });
// Cursor (state tracking)
var cursor_1 = require("./cursor");
Object.defineProperty(exports, "StreamCursor", { enumerable: true, get: function () { return cursor_1.StreamCursor; } });
Object.defineProperty(exports, "FrameHandler", { enumerable: true, get: function () { return cursor_1.FrameHandler; } });
// UI Events
var ui_events_1 = require("./ui_events");
// Constructors
Object.defineProperty(exports, "progress", { enumerable: true, get: function () { return ui_events_1.progress; } });
Object.defineProperty(exports, "log", { enumerable: true, get: function () { return ui_events_1.log; } });
Object.defineProperty(exports, "logInfo", { enumerable: true, get: function () { return ui_events_1.logInfo; } });
Object.defineProperty(exports, "logWarn", { enumerable: true, get: function () { return ui_events_1.logWarn; } });
Object.defineProperty(exports, "logError", { enumerable: true, get: function () { return ui_events_1.logError; } });
Object.defineProperty(exports, "logDebug", { enumerable: true, get: function () { return ui_events_1.logDebug; } });
Object.defineProperty(exports, "metric", { enumerable: true, get: function () { return ui_events_1.metric; } });
Object.defineProperty(exports, "counter", { enumerable: true, get: function () { return ui_events_1.counter; } });
Object.defineProperty(exports, "artifact", { enumerable: true, get: function () { return ui_events_1.artifact; } });
Object.defineProperty(exports, "resyncRequest", { enumerable: true, get: function () { return ui_events_1.resyncRequest; } });
Object.defineProperty(exports, "error", { enumerable: true, get: function () { return ui_events_1.error; } });
// Emit helpers
Object.defineProperty(exports, "emitUI", { enumerable: true, get: function () { return ui_events_1.emitUI; } });
Object.defineProperty(exports, "emitProgress", { enumerable: true, get: function () { return ui_events_1.emitProgress; } });
Object.defineProperty(exports, "emitLog", { enumerable: true, get: function () { return ui_events_1.emitLog; } });
Object.defineProperty(exports, "emitMetric", { enumerable: true, get: function () { return ui_events_1.emitMetric; } });
Object.defineProperty(exports, "emitArtifact", { enumerable: true, get: function () { return ui_events_1.emitArtifact; } });
Object.defineProperty(exports, "emitError", { enumerable: true, get: function () { return ui_events_1.emitError; } });
Object.defineProperty(exports, "emitResyncRequest", { enumerable: true, get: function () { return ui_events_1.emitResyncRequest; } });
// Parse helper
Object.defineProperty(exports, "parseUIEvent", { enumerable: true, get: function () { return ui_events_1.parseUIEvent; } });
//# sourceMappingURL=index.js.map