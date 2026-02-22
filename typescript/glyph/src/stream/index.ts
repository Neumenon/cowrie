/**
 * GS1 (GLYPH Stream v1) - Stream framing protocol for GLYPH payloads.
 */

// Types
export {
  VERSION,
  FrameKind,
  KIND_VALUES,
  VALUE_KINDS,
  Frame,
  FLAGS,
  MAX_PAYLOAD_SIZE,
  ParseError,
  CRCMismatchError,
  BaseMismatchError,
  parseKind,
  kindToString,
} from './types';

// CRC
export {
  computeCRC,
  verifyCRC,
  crcToHex,
  parseCRC,
} from './crc';

// Hash
export {
  sha256,
  sha256Sync,
  stateHashLoose,
  stateHashLooseSync,
  stateHashBytes,
  verifyBase,
  hashToHex,
  hexToHash,
} from './hash';

// GS1-T Reader/Writer
export {
  Reader,
  ReaderOptions,
  WriterOptions,
  encodeFrame,
  encodeFrames,
  decodeFrame,
  decodeFrames,
  // Frame constructors
  docFrame,
  patchFrame,
  rowFrame,
  uiFrame,
  ackFrame,
  errFrame,
  pingFrame,
  pongFrame,
} from './gs1t';

// Cursor (state tracking)
export {
  StreamCursor,
  SIDState,
  FrameHandler,
  FrameHandlerCallbacks,
} from './cursor';

// UI Events
export {
  // Constructors
  progress,
  log,
  logInfo,
  logWarn,
  logError,
  logDebug,
  metric,
  counter,
  artifact,
  resyncRequest,
  error,
  // Emit helpers
  emitUI,
  emitProgress,
  emitLog,
  emitMetric,
  emitArtifact,
  emitError,
  emitResyncRequest,
  // Parse helper
  parseUIEvent,
  ParsedUIEvent,
} from './ui_events';
