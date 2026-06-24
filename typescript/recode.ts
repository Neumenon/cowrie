// Cross-language conformance harness: decode a Cowrie v1 stream from stdin and re-encode
// canonical bytes to stdout. Used by the golden recode gate. Run: tsx recode.ts < in.cow
//
// Strict mode (SPEC-v1 §5.3): pass --strict or set STRICT=1 to reject well-formed-but-
// non-canonical input (decode raises ERR_NON_CANONICAL -> non-zero exit). Without it,
// behavior is unchanged (lenient decode).
//
// Address mode (SPEC-v1 §3): pass --addr to print the content address (multihash SHA-256
// of the canonical wire bytes) as lowercase hex instead of the canonical bytes. Combines
// with --strict.
//
// File modes (SPEC-v1 §7): read a Cowrie FILE (CWRF container) on stdin.
//   --file-id     : decode+verify, print the merkle_root as one line of lowercase hex (68 chars).
//   --file-recode : decode, RE-ENCODE the file, write raw bytes to stdout (reproduces input
//                   byte-for-byte for canonical files).
import { encode, decode, addressOfBytes, decodeFile, encodeFile, fileIdentity, tensorSpans, schemaFingerprint64, merkleRoot } from "./src/gen2/index";

const strict = process.argv.includes("--strict") || process.env.STRICT === "1";
// Dataset/stream layer Merkle root (SPEC-v1 §7 dataset_root): read uvarint(n) then n
// repetitions of (uvarint(len) || blob_bytes) from stdin, compute the §7 merkleRoot over
// that ORDERED list of blobs (shard file-roots), print the 34-byte multihash as 68 hex chars.
const datasetRoot = process.argv.includes("--dataset-root");
const addr = process.argv.includes("--addr");
const fileId = process.argv.includes("--file-id");
const fileRecode = process.argv.includes("--file-recode");
const tensorSpansMode = process.argv.includes("--tensor-spans");
// Fingerprint mode (SPEC-v1 §4): decode and print the §4 FPC-grammar fingerprint64 as
// 16 lowercase hex chars. Combines with --strict.
const fingerprint = process.argv.includes("--fingerprint");

const chunks: Buffer[] = [];
process.stdin.on("data", (c: Buffer) => chunks.push(c));
process.stdin.on("end", () => {
  const data = new Uint8Array(Buffer.concat(chunks));
  try {
    if (datasetRoot) {
      let pos = 0;
      const readUvarint = (): number => {
        let shift = 0n;
        let val = 0n;
        for (;;) {
          const b = data[pos];
          pos++;
          val |= BigInt(b & 0x7f) << shift;
          if ((b & 0x80) === 0) break;
          shift += 7n;
        }
        return Number(val);
      };
      const n = readUvarint();
      const blobs: Uint8Array[] = [];
      for (let i = 0; i < n; i++) {
        const len = readUvarint();
        blobs.push(data.subarray(pos, pos + len));
        pos += len;
      }
      process.stdout.write(Buffer.from(merkleRoot(blobs)).toString("hex") + "\n");
    } else if (fingerprint) {
      const fp = schemaFingerprint64(decode(data, { strict }));
      process.stdout.write(fp.toString(16).padStart(16, "0") + "\n");
    } else if (tensorSpansMode) {
      // Phase 2 zero-copy tensor locator: one line per tensor in document order,
      // "<data_offset> <data_len> <dtype> <shape>" (shape comma-joined or "-" for rank-0).
      const spans = tensorSpans(data, { strict });
      let out = "";
      for (const s of spans) {
        const shape = s.shape.length === 0 ? "-" : s.shape.join(",");
        out += `${s.dataOffset} ${s.dataLen} ${s.dtype} ${shape}\n`;
      }
      process.stdout.write(out);
    } else if (fileId) {
      process.stdout.write(Buffer.from(fileIdentity(data)).toString("hex") + "\n");
    } else if (fileRecode) {
      const frames = decodeFile(data, true);
      process.stdout.write(Buffer.from(encodeFile(frames)));
    } else {
      const canonical = encode(decode(data, { strict }));
      if (addr) {
        process.stdout.write(Buffer.from(addressOfBytes(canonical)).toString("hex") + "\n");
      } else {
        process.stdout.write(Buffer.from(canonical));
      }
    }
  } catch (e) {
    process.stderr.write(String(e));
    process.exit(1);
  }
});
