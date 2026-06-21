import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from "fs";
import path from "path";
import { decode, toAny, fromAny } from "./index.ts";

function mapErrorCode(err: unknown): string {
  if (!(err instanceof Error)) return "";
  const msg = err.message;
  if (msg.includes("Invalid magic bytes")) return "ERR_INVALID_MAGIC";
  if (msg.includes("Unsupported version")) return "ERR_INVALID_VERSION";
  if (msg.includes("Unexpected end of data") || msg.includes("truncated")) return "ERR_TRUNCATED";
  if (msg.startsWith("Invalid tag")) return "ERR_INVALID_TAG";
  if (msg.includes("trailing data")) return "ERR_TRAILING_DATA";
  if (msg.includes("channels out of range")) return "ERR_INVALID_AUDIO_CHANNELS";
  // Two paths produce a rate error: the bridge's intFieldFromObj ("rate out of
  // range for uint32") and SJ.audio ("sampleRate out of range"). Match both.
  if (msg.includes("rate out of range") || msg.includes("sampleRate out of range")) return "ERR_INVALID_AUDIO_RATE";
  return "";
}

describe("gen2 core fixtures", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const manifestPath = path.join(repoRoot, "testdata", "fixtures", "manifest.json");
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

  for (const c of manifest.cases || []) {
    if (c.gen !== 2 || (c.kind !== "decode" && c.kind !== "from_json")) continue;

    it(c.id, () => {
      const inputPath = path.join(repoRoot, "testdata", "fixtures", c.input);

      // decode = binary wire -> Value; from_json = JSON projection -> Value.
      const produce = () =>
        c.kind === "from_json"
          ? fromAny(JSON.parse(fs.readFileSync(inputPath, "utf8")))
          : decode(new Uint8Array(fs.readFileSync(inputPath)));

      if (c.expect.ok) {
        const value = produce();
        if (c.expect.json) {
          const expectedPath = path.join(repoRoot, "testdata", "fixtures", c.expect.json);
          const expected = JSON.parse(fs.readFileSync(expectedPath, "utf8"));
          assert.deepStrictEqual(toAny(value), expected);
        }
      } else {
        try {
          produce();
        } catch (err) {
          const code = mapErrorCode(err);
          assert.strictEqual(code, c.expect.error);
          return;
        }
        assert.fail(`${c.id}: expected error but ${c.kind} succeeded`);
      }
    });
  }
});
