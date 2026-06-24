// Cross-language conformance harness: decode a Cowrie v1 stream from stdin and re-encode
// canonical bytes to stdout. Used by the golden recode gate. Run: tsx recode.ts < in.cow
import { encode, decode } from "./src/gen2/index";

const chunks: Buffer[] = [];
process.stdin.on("data", (c: Buffer) => chunks.push(c));
process.stdin.on("end", () => {
  const data = new Uint8Array(Buffer.concat(chunks));
  try {
    process.stdout.write(Buffer.from(encode(decode(data))));
  } catch (e) {
    process.stderr.write(String(e));
    process.exit(1);
  }
});
