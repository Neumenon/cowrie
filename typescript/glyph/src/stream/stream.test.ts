/**
 * GS1-T Stream tests
 */

import {
  Frame,
  Reader,
  encodeFrame,
  encodeFrames,
  decodeFrame,
  decodeFrames,
  computeCRC,
  crcToHex,
  parseCRC,
  hashToHex,
  hexToHash,
  docFrame,
  patchFrame,
  ackFrame,
  uiFrame,
  errFrame,
  pingFrame,
  pongFrame,
  CRCMismatchError,
  ParseError,
  StreamCursor,
  FrameHandler,
  stateHashLooseSync,
  // UI Events
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
  emitProgress,
  emitLog,
  emitMetric,
  emitArtifact,
  parseUIEvent,
} from './index';
import { g } from '../types';
import { emit } from '../emit';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// ============================================================
// Writer Tests
// ============================================================

describe('encodeFrame', () => {
  test('minimal frame', () => {
    const frame = docFrame(0n, 0n, '{}');
    const bytes = encodeFrame(frame);
    const str = decoder.decode(bytes);
    
    expect(str).toBe('@frame{v=1 sid=0 seq=0 kind=doc len=2}\n{}\n');
  });
  
  test('with CRC', () => {
    const frame = docFrame(1n, 5n, '{x=1}');
    const bytes = encodeFrame(frame, { withCRC: true });
    const str = decoder.decode(bytes);
    
    expect(str).toContain('crc=');
  });
  
  test('with base', () => {
    const base = new Uint8Array(32);
    base[0] = 0x01;
    base[1] = 0x02;
    
    const frame = patchFrame(1n, 10n, '@patch\nset .x 1\n@end', base);
    const bytes = encodeFrame(frame);
    const str = decoder.decode(bytes);
    
    expect(str).toContain('base=sha256:');
  });
  
  test('final flag', () => {
    const frame: Frame = {
      version: 1,
      sid: 1n,
      seq: 100n,
      kind: 'doc',
      payload: encoder.encode('final'),
      final: true,
    };
    const bytes = encodeFrame(frame);
    const str = decoder.decode(bytes);
    
    expect(str).toContain('final=true');
  });
  
  test('empty payload', () => {
    const frame = ackFrame(1n, 42n);
    const bytes = encodeFrame(frame);
    const str = decoder.decode(bytes);
    
    expect(str).toBe('@frame{v=1 sid=1 seq=42 kind=ack len=0}\n\n');
  });
});

// ============================================================
// Reader Tests
// ============================================================

describe('decodeFrame', () => {
  test('minimal frame', () => {
    const input = '@frame{v=1 sid=0 seq=0 kind=doc len=2}\n{}\n';
    const frame = decodeFrame(encoder.encode(input));
    
    expect(frame).not.toBeNull();
    expect(frame!.version).toBe(1);
    expect(frame!.sid).toBe(0n);
    expect(frame!.seq).toBe(0n);
    expect(frame!.kind).toBe('doc');
    expect(decoder.decode(frame!.payload)).toBe('{}');
  });
  
  test('with CRC verification', () => {
    const payload = encoder.encode('hello');
    const crc = computeCRC(payload);
    
    const input = `@frame{v=1 sid=1 seq=5 kind=doc len=5 crc=${crcToHex(crc)}}\nhello\n`;
    const frame = decodeFrame(encoder.encode(input));
    
    expect(frame).not.toBeNull();
    expect(frame!.crc).toBe(crc);
  });
  
  test('CRC mismatch throws', () => {
    const input = '@frame{v=1 sid=1 seq=5 kind=doc len=5 crc=deadbeef}\nhello\n';
    
    expect(() => decodeFrame(encoder.encode(input))).toThrow(CRCMismatchError);
  });
  
  test('with base', () => {
    const baseHex = '0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20';
    const input = `@frame{v=1 sid=1 seq=10 kind=patch len=4 base=sha256:${baseHex}}\ntest\n`;
    const frame = decodeFrame(encoder.encode(input));
    
    expect(frame).not.toBeNull();
    expect(frame!.base).not.toBeUndefined();
    expect(frame!.base![0]).toBe(0x01);
    expect(frame!.base![1]).toBe(0x02);
  });
  
  test('payload with newlines', () => {
    const payload = '@patch\nset .x 1\nset .y 2\n@end';
    const input = `@frame{v=1 sid=1 seq=1 kind=patch len=${payload.length}}\n${payload}\n`;
    const frame = decodeFrame(encoder.encode(input));
    
    expect(frame).not.toBeNull();
    expect(decoder.decode(frame!.payload)).toBe(payload);
  });
  
  test('payload with braces', () => {
    const payload = '{a={b={c=1}}}';
    const input = `@frame{v=1 sid=1 seq=1 kind=doc len=${payload.length}}\n${payload}\n`;
    const frame = decodeFrame(encoder.encode(input));
    
    expect(frame).not.toBeNull();
    expect(decoder.decode(frame!.payload)).toBe(payload);
  });
  
  test('multiple frames', () => {
    const input = `@frame{v=1 sid=1 seq=0 kind=doc len=5}
hello
@frame{v=1 sid=1 seq=1 kind=patch len=6}
update
@frame{v=1 sid=1 seq=2 kind=ack len=0}

`;
    const frames = decodeFrames(encoder.encode(input));
    
    expect(frames.length).toBe(3);
    expect(frames[0].kind).toBe('doc');
    expect(decoder.decode(frames[0].payload)).toBe('hello');
    expect(frames[1].kind).toBe('patch');
    expect(decoder.decode(frames[1].payload)).toBe('update');
    expect(frames[2].kind).toBe('ack');
    expect(frames[2].payload.length).toBe(0);
  });
  
  test('all kinds', () => {
    const kinds = ['doc', 'patch', 'row', 'ui', 'ack', 'err', 'ping', 'pong'];
    for (const k of kinds) {
      const input = `@frame{v=1 sid=0 seq=0 kind=${k} len=1}\nx\n`;
      const frame = decodeFrame(encoder.encode(input));
      expect(frame).not.toBeNull();
      expect(frame!.kind).toBe(k);
    }
  });
  
  test('numeric kind', () => {
    const input = '@frame{v=1 sid=0 seq=0 kind=99 len=1}\nx\n';
    const frame = decodeFrame(encoder.encode(input));
    
    expect(frame).not.toBeNull();
    expect(frame!.kind).toBe(99);
  });
  
  test('header variations', () => {
    // Comma-separated
    const input1 = '@frame{v=1,sid=1,seq=0,kind=doc,len=1}\nx\n';
    expect(decodeFrame(encoder.encode(input1))).not.toBeNull();
    
    // Extra spaces
    const input2 = '@frame{  v=1   sid=1  seq=0  kind=doc   len=1  }\nx\n';
    expect(decodeFrame(encoder.encode(input2))).not.toBeNull();
  });
  
  test('empty input returns null', () => {
    expect(decodeFrame(new Uint8Array(0))).toBeNull();
  });
  
  test('payload too large throws', () => {
    const input = '@frame{v=1 sid=0 seq=0 kind=doc len=999999999}\n';
    const reader = new Reader({ maxPayload: 1024 });
    reader.push(encoder.encode(input));
    
    expect(() => reader.next()).toThrow(ParseError);
  });
});

// ============================================================
// Round-trip Tests
// ============================================================

describe('round-trip', () => {
  const testCases: { name: string; frame: Frame }[] = [
    { name: 'minimal doc', frame: docFrame(0n, 0n, '{}') },
    { 
      name: 'patch with base', 
      frame: patchFrame(1n, 5n, '@patch\nset .x 1\n@end', new Uint8Array([0x01, 0x02, ...new Array(30).fill(0)])) 
    },
    { name: 'row', frame: { version: 1, sid: 2n, seq: 100n, kind: 'row', payload: encoder.encode('Row@(id 1 name foo)') } },
    { name: 'ui', frame: uiFrame(1n, 50n, 'UIEvent@(type "progress" pct 0.5)') },
    { name: 'ack', frame: ackFrame(1n, 10n) },
    { name: 'err', frame: errFrame(1n, 11n, 'Err@(code "FAIL")') },
    { name: 'ping', frame: pingFrame(0n, 0n) },
    { name: 'pong', frame: pongFrame(0n, 0n) },
    { 
      name: 'final', 
      frame: { version: 1, sid: 1n, seq: 999n, kind: 'doc', payload: encoder.encode('done'), final: true } 
    },
    { 
      name: 'large seq', 
      frame: { version: 1, sid: 18446744073709551615n, seq: 18446744073709551615n, kind: 'doc', payload: encoder.encode('x') } 
    },
  ];
  
  for (const tc of testCases) {
    test(tc.name, () => {
      const encoded = encodeFrame(tc.frame);
      const decoded = decodeFrame(encoded);
      
      expect(decoded).not.toBeNull();
      expect(decoded!.version).toBe(tc.frame.version);
      expect(decoded!.sid).toBe(tc.frame.sid);
      expect(decoded!.seq).toBe(tc.frame.seq);
      expect(decoded!.kind).toBe(tc.frame.kind);
      expect(decoder.decode(decoded!.payload)).toBe(decoder.decode(tc.frame.payload));
      expect(decoded!.final ?? false).toBe(tc.frame.final ?? false);
      
      if (tc.frame.base) {
        expect(decoded!.base).not.toBeUndefined();
        expect(hashToHex(decoded!.base!)).toBe(hashToHex(tc.frame.base));
      }
    });
  }
  
  test('with CRC', () => {
    const original = docFrame(1n, 5n, 'test payload with CRC');
    const encoded = encodeFrame(original, { withCRC: true });
    const decoded = decodeFrame(encoded, { verifyCRC: true });
    
    expect(decoded).not.toBeNull();
    expect(decoded!.crc).not.toBeUndefined();
    expect(decoder.decode(decoded!.payload)).toBe('test payload with CRC');
  });
});

// ============================================================
// CRC Tests
// ============================================================

describe('CRC-32', () => {
  test('known values', () => {
    // Known CRC-32 IEEE test vectors
    const testCases = [
      { input: '', crc: 0x00000000 },
      { input: 'a', crc: 0xe8b7be43 },
      { input: 'abc', crc: 0x352441c2 },
      { input: 'hello', crc: 0x3610a686 },
    ];
    
    for (const tc of testCases) {
      const got = computeCRC(encoder.encode(tc.input));
      expect(got).toBe(tc.crc);
    }
  });
  
  test('crcToHex', () => {
    expect(crcToHex(0xdeadbeef)).toBe('deadbeef');
    expect(crcToHex(0x00000001)).toBe('00000001');
  });
  
  test('parseCRC', () => {
    expect(parseCRC('deadbeef')).toBe(0xdeadbeef);
    expect(parseCRC('crc32:deadbeef')).toBe(0xdeadbeef);
    expect(parseCRC('invalid')).toBeNull();
  });
});

// ============================================================
// Hash Tests
// ============================================================

describe('Hash', () => {
  test('hashToHex', () => {
    const h = new Uint8Array([0xab, 0xcd, 0xef, 0x01, ...new Array(28).fill(0)]);
    const hex = hashToHex(h);
    expect(hex.length).toBe(64);
    expect(hex.startsWith('abcdef01')).toBe(true);
  });
  
  test('hexToHash round-trip', () => {
    const original = new Uint8Array(32);
    for (let i = 0; i < 32; i++) {
      original[i] = i * 8;
    }
    
    const hex = hashToHex(original);
    const parsed = hexToHash(hex);
    
    expect(parsed).not.toBeNull();
    expect(hashToHex(parsed!)).toBe(hex);
  });
  
  test('hexToHash with prefix', () => {
    const hex = '0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20';
    const parsed = hexToHash(`sha256:${hex}`);
    expect(parsed).not.toBeNull();
    expect(parsed![0]).toBe(0x01);
  });
  
  test('hexToHash invalid', () => {
    expect(hexToHash('tooshort')).toBeNull();
    expect(hexToHash('gg' + '00'.repeat(31))).toBeNull();
  });
});

// ============================================================
// Incremental Reader Tests
// ============================================================

describe('Reader incremental', () => {
  test('partial data buffering', () => {
    const reader = new Reader();
    
    // Push partial header
    reader.push(encoder.encode('@frame{v=1 sid=0'));
    expect(reader.next()).toBeNull();
    
    // Push rest of header
    reader.push(encoder.encode(' seq=0 kind=doc len=5}\n'));
    expect(reader.next()).toBeNull();
    
    // Push payload
    reader.push(encoder.encode('hello\n'));
    const frame = reader.next();
    
    expect(frame).not.toBeNull();
    expect(decoder.decode(frame!.payload)).toBe('hello');
  });
  
  test('multiple frames in buffer', () => {
    const reader = new Reader();
    
    const input = `@frame{v=1 sid=1 seq=0 kind=doc len=1}
a
@frame{v=1 sid=1 seq=1 kind=doc len=1}
b
`;
    reader.push(encoder.encode(input));
    
    const f1 = reader.next();
    expect(f1).not.toBeNull();
    expect(decoder.decode(f1!.payload)).toBe('a');
    
    const f2 = reader.next();
    expect(f2).not.toBeNull();
    expect(decoder.decode(f2!.payload)).toBe('b');
    
    expect(reader.next()).toBeNull();
  });
});

// ============================================================
// StreamCursor Tests
// ============================================================

describe('StreamCursor', () => {
  test('basic operations', () => {
    const cursor = new StreamCursor();
    
    // Get creates state
    const state = cursor.get(1n);
    expect(state.sid).toBe(1n);
    expect(state.lastSeq).toBe(0n);
    
    // GetReadOnly returns undefined for unknown
    expect(cursor.getReadOnly(99n)).toBeUndefined();
    
    // AllSIDs
    cursor.get(2n);
    cursor.get(3n);
    expect(cursor.allSIDs().length).toBe(3);
    
    // Delete
    cursor.delete(2n);
    expect(cursor.getReadOnly(2n)).toBeUndefined();
  });
  
  test('processFrame updates sequence', () => {
    const cursor = new StreamCursor();
    
    cursor.processFrame(docFrame(1n, 1n, '{}'));
    expect(cursor.get(1n).lastSeq).toBe(1n);
    
    cursor.processFrame(docFrame(1n, 2n, '{}'));
    expect(cursor.get(1n).lastSeq).toBe(2n);
  });
  
  test('processFrame throws on gap', () => {
    const cursor = new StreamCursor();
    
    cursor.processFrame(docFrame(1n, 1n, '{}'));
    
    expect(() => {
      cursor.processFrame(docFrame(1n, 5n, '{}')); // Gap!
    }).toThrow(/sequence gap/);
  });
  
  test('processFrame throws on duplicate', () => {
    const cursor = new StreamCursor();
    
    cursor.processFrame(docFrame(1n, 1n, '{}'));
    
    expect(() => {
      cursor.processFrame(docFrame(1n, 1n, '{}')); // Duplicate
    }).toThrow(/not monotonic/);
  });
  
  test('patch verification', () => {
    const cursor = new StreamCursor();
    
    // Set initial state
    const doc = g.map({ key: 'x', value: g.int(1) });
    cursor.setState(1n, doc);
    
    const state = cursor.get(1n);
    expect(state.stateHash).not.toBeNull();
    
    // Patch with correct base should succeed
    const correctBase = state.stateHash!;
    cursor.processFrame(patchFrame(1n, 1n, '@patch\nset .x 2\n@end', correctBase));
    
    // Update state
    const doc2 = g.map({ key: 'x', value: g.int(2) });
    cursor.setState(1n, doc2);
    
    // Patch with wrong base should throw
    const wrongBase = new Uint8Array(32);
    wrongBase[0] = 0xde;
    
    expect(() => {
      cursor.processFrame(patchFrame(1n, 2n, '@patch\nset .x 3\n@end', wrongBase));
    }).toThrow();
  });
  
  test('ack tracking', () => {
    const cursor = new StreamCursor();
    
    // Process frames
    for (let seq = 1n; seq <= 5n; seq++) {
      cursor.processFrame(docFrame(1n, seq, '{}'));
    }
    
    // No acks yet
    expect(cursor.pendingAcks(1n).length).toBe(5);
    
    // Ack some
    cursor.ack(1n, 3n);
    expect(cursor.pendingAcks(1n).length).toBe(2); // 4, 5
    
    // Ack rest
    cursor.ack(1n, 5n);
    expect(cursor.pendingAcks(1n).length).toBe(0);
  });
  
  test('final flag', () => {
    const cursor = new StreamCursor();
    
    cursor.processFrame({
      version: 1,
      sid: 1n,
      seq: 1n,
      kind: 'doc',
      payload: encoder.encode('done'),
      final: true,
    });
    
    expect(cursor.get(1n).final).toBe(true);
  });
});

// ============================================================
// FrameHandler Tests
// ============================================================

describe('FrameHandler', () => {
  test('dispatches to callbacks', () => {
    const docs: string[] = [];
    const patches: string[] = [];
    const uiEvents: string[] = [];
    
    const handler = new FrameHandler({
      onDoc: (sid, seq, payload) => { docs.push(decoder.decode(payload)); },
      onPatch: (sid, seq, payload) => { patches.push(decoder.decode(payload)); },
      onUI: (sid, seq, payload) => { uiEvents.push(decoder.decode(payload)); },
    });
    
    handler.handle(docFrame(1n, 1n, '{x=1}'));
    handler.handle(patchFrame(1n, 2n, 'set .x 2'));
    handler.handle(uiFrame(1n, 3n, 'progress 50%'));
    handler.handle(patchFrame(1n, 4n, 'set .x 3'));
    
    expect(docs).toEqual(['{x=1}']);
    expect(patches).toEqual(['set .x 2', 'set .x 3']);
    expect(uiEvents).toEqual(['progress 50%']);
  });
  
  test('gap callback', () => {
    const gaps: [bigint, bigint][] = [];
    
    const handler = new FrameHandler({
      onSeqGap: (sid, expected, got) => {
        gaps.push([expected, got]);
        return true; // Allow gap
      },
    });
    
    handler.handle(docFrame(1n, 1n, 'a'));
    handler.handle(docFrame(1n, 5n, 'b')); // Gap!
    
    expect(gaps.length).toBe(1);
    expect(gaps[0]).toEqual([2n, 5n]);
    
    // State should still update if callback allows
    expect(handler.cursor.get(1n).lastSeq).toBe(5n);
  });
  
  test('final callback', () => {
    const finalSIDs: bigint[] = [];
    
    const handler = new FrameHandler({
      onFinal: (sid) => { finalSIDs.push(sid); },
    });
    
    handler.handle(docFrame(1n, 1n, 'a'));
    handler.handle({
      version: 1,
      sid: 1n,
      seq: 2n,
      kind: 'doc',
      payload: encoder.encode('done'),
      final: true,
    });
    
    expect(finalSIDs).toEqual([1n]);
  });
  
  test('duplicate skipped', () => {
    let count = 0;
    
    const handler = new FrameHandler({
      onDoc: () => { count++; },
    });
    
    handler.handle(docFrame(1n, 1n, 'a'));
    handler.handle(docFrame(1n, 1n, 'b')); // Duplicate
    handler.handle(docFrame(1n, 2n, 'c'));
    
    expect(count).toBe(2);
  });
});

// ============================================================
// UI Events Tests
// ============================================================

describe('UI Events', () => {
  test('progress', () => {
    const p = progress(0.5, 'halfway there');
    const emitted = emit(p);
    
    expect(emitted).toContain('Progress');
    expect(emitted).toContain('pct=0.5');
    expect(emitted).toContain('msg="halfway there"');
  });
  
  test('log levels', () => {
    const info = logInfo('info message');
    const warn = logWarn('warning message');
    const err = logError('error message');
    const debug = logDebug('debug message');
    
    expect(emit(info)).toContain('level=info');
    expect(emit(warn)).toContain('level=warn');
    expect(emit(err)).toContain('level=error');
    expect(emit(debug)).toContain('level=debug');
    
    // All should have timestamps
    expect(emit(info)).toContain('ts=');
  });
  
  test('metric', () => {
    const m = metric('latency_ms', 12.5, 'ms');
    const emitted = emit(m);
    
    expect(emitted).toContain('Metric');
    expect(emitted).toContain('name=latency_ms');
    expect(emitted).toContain('value=12.5');
    expect(emitted).toContain('unit=ms');
  });
  
  test('counter', () => {
    const c = counter('items', 100);
    const emitted = emit(c);
    
    expect(emitted).toContain('Metric');
    expect(emitted).toContain('name=items');
    expect(emitted).toContain('value=100');
    expect(emitted).toContain('unit=count');
  });
  
  test('artifact', () => {
    const a = artifact('image/png', 'blob:sha256:abc123', 'plot.png');
    const emitted = emit(a);
    
    expect(emitted).toContain('Artifact');
    expect(emitted).toContain('mime=image/png'); // "/" is bare-safe
    expect(emitted).toContain('ref="blob:sha256:abc123"');
    expect(emitted).toContain('name=plot.png');
  });
  
  test('resyncRequest', () => {
    const r = resyncRequest(1n, 42n, 'sha256:abc...', 'BASE_MISMATCH');
    const emitted = emit(r);
    
    expect(emitted).toContain('ResyncRequest');
    expect(emitted).toContain('sid=1');
    expect(emitted).toContain('seq=42');
    expect(emitted).toContain('reason=BASE_MISMATCH');
  });
  
  test('error', () => {
    const e = error('INVALID_STATE', 'something went wrong', 1n, 10n);
    const emitted = emit(e);
    
    expect(emitted).toContain('Error');
    expect(emitted).toContain('code=INVALID_STATE');
    expect(emitted).toContain('msg="something went wrong"');
  });
  
  test('emitProgress returns bytes', () => {
    const bytes = emitProgress(0.75, 'almost done');
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(decoder.decode(bytes)).toContain('Progress');
  });
  
  test('emitLog returns bytes', () => {
    const bytes = emitLog('info', 'test message');
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(decoder.decode(bytes)).toContain('Log');
  });
  
  test('emitMetric returns bytes', () => {
    const bytes = emitMetric('cpu', 45.2, '%');
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(decoder.decode(bytes)).toContain('Metric');
  });
  
  test('emitArtifact returns bytes', () => {
    const bytes = emitArtifact('text/plain', 'file:output.txt', 'output.txt');
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(decoder.decode(bytes)).toContain('Artifact');
  });
});
