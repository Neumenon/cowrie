/**
 * Suite 4: Mutation/Corruption Tests for cowrie TypeScript.
 *
 * Programmatic corruption of valid fixtures — verifies the decoder
 * never crashes on corrupted input (only returns errors or different values).
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from 'fs';
import path from 'path';
import { decode } from './index.ts';

const repoRoot = path.resolve(__dirname, '../../..');
const fixtureDir = path.join(repoRoot, 'testdata', 'fixtures');

const CORE_FIXTURES = [
  'core/null.cowrie',
  'core/true.cowrie',
  'core/int.cowrie',
  'core/float.cowrie',
  'core/string.cowrie',
  'core/array.cowrie',
  'core/object.cowrie',
];

function loadFixture(name: string): Uint8Array {
  return new Uint8Array(fs.readFileSync(path.join(fixtureDir, name)));
}

describe('mutation: truncation', () => {
  for (const fixture of CORE_FIXTURES) {
    it(`truncation at every offset: ${fixture}`, () => {
      const data = loadFixture(fixture);
      for (let i = 0; i < data.length; i++) {
        const truncated = data.slice(0, i);
        try {
          decode(truncated);
        } catch {
          // Any exception is fine — no crash
        }
      }
    });
  }
});

describe('mutation: single-byte flip', () => {
  for (const fixture of CORE_FIXTURES) {
    it(`bitflip at every offset: ${fixture}`, () => {
      const data = loadFixture(fixture);
      for (let i = 0; i < data.length; i++) {
        const corrupted = new Uint8Array(data);
        corrupted[i] ^= 0xFF;
        try {
          decode(corrupted);
        } catch {
          // Any exception is fine — no crash
        }
      }
    });
  }
});

describe('mutation: header corruption', () => {
  // Test a subset of fixtures for speed
  for (const fixture of CORE_FIXTURES.slice(0, 3)) {
    it(`magic byte corruption: ${fixture}`, () => {
      const data = loadFixture(fixture);
      for (let pos = 0; pos < Math.min(4, data.length); pos++) {
        for (let val = 0; val < 256; val++) {
          const corrupted = new Uint8Array(data);
          corrupted[pos] = val;
          try {
            decode(corrupted);
          } catch {
            // Any exception is fine
          }
        }
      }
    });
  }
});

describe('mutation: edge cases', () => {
  it('empty input', () => {
    assert.throws(() => decode(new Uint8Array(0)));
  });

  it('single byte - all values', () => {
    for (let b = 0; b < 256; b++) {
      try {
        decode(new Uint8Array([b]));
      } catch {
        // fine
      }
    }
  });

  it('just magic bytes', () => {
    try {
      decode(new Uint8Array([0x53, 0x4A]));
    } catch {
      // fine
    }
  });

  it('magic plus version', () => {
    try {
      decode(new Uint8Array([0x53, 0x4A, 0x02, 0x00]));
    } catch {
      // fine
    }
  });
});
