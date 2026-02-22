import fs from "fs";
import path from "path";
import { decode, toAny } from "./index";

function mapErrorCode(err: unknown): string {
  if (!(err instanceof Error)) return "";
  const msg = err.message;
  if (msg.includes("Invalid magic bytes")) return "ERR_INVALID_MAGIC";
  if (msg.includes("Unsupported version")) return "ERR_INVALID_VERSION";
  if (msg.includes("Unexpected end of data") || msg.includes("truncated")) return "ERR_TRUNCATED";
  if (msg.startsWith("Invalid tag")) return "ERR_INVALID_TAG";
  return "";
}

describe("gen2 core fixtures", () => {
  const repoRoot = path.resolve(__dirname, "../../../..");
  const manifestPath = path.join(repoRoot, "testdata", "fixtures", "manifest.json");
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

  for (const c of manifest.cases || []) {
    if (c.gen !== 2 || c.kind !== "decode") continue;

    it(c.id, () => {
      const inputPath = path.join(repoRoot, "testdata", "fixtures", c.input);
      const data = fs.readFileSync(inputPath);

      if (c.expect.ok) {
        const value = decode(new Uint8Array(data));
        const actual = toAny(value);
        if (c.expect.json) {
          const expectedPath = path.join(repoRoot, "testdata", "fixtures", c.expect.json);
          const expected = JSON.parse(fs.readFileSync(expectedPath, "utf8"));
          expect(actual).toEqual(expected);
        }
      } else {
        try {
          decode(new Uint8Array(data));
        } catch (err) {
          const code = mapErrorCode(err);
          expect(code).toEqual(c.expect.error);
          return;
        }
        throw new Error(`${c.id}: expected error but decode succeeded`);
      }
    });
  }
});
