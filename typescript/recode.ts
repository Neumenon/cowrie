// Cross-language conformance harness: decode a Cowrie v1 stream from stdin and re-encode
// canonical bytes to stdout. Used by the golden recode gate. Run: tsx recode.ts < in.cow
//
// Strict mode (SPEC-v1 §5.3): pass --strict or set STRICT=1 to reject well-formed-but-
// non-canonical input (decode raises ERR_NON_CANONICAL -> non-zero exit). Without it,
// behavior is unchanged (lenient decode).
import { encode, decode } from "./src/gen2/index";

const strict = process.argv.includes("--strict") || process.env.STRICT === "1";

const chunks: Buffer[] = [];
process.stdin.on("data", (c: Buffer) => chunks.push(c));
process.stdin.on("end", () => {
  const data = new Uint8Array(Buffer.concat(chunks));
  try {
    process.stdout.write(Buffer.from(encode(decode(data, { strict }))));
  } catch (e) {
    process.stderr.write(String(e));
    process.exit(1);
  }
});
