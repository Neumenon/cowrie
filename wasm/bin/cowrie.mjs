#!/usr/bin/env node
/**
 * `npx cowrie-ai` CLI entrypoint.
 * Automatically delegates to the native Rust `cowrie` binary when installed for sub-15µs speed
 * and `.cwwal` disk WAL support, and seamlessly falls back to bundled zero-import `cowrie_core.wasm`
 * so `npx cowrie-ai mcp-proxy --virtualize -- <cmd>` works out of the box on every OS & CPU.
 */

import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  AUTH_MODEL_SYNTH,
  AUTH_SYS_POLICY,
  AUTH_UNTRUSTED_EXT,
  AUTH_VERIFIED_TOOL,
  EPI_OBSERVED,
  createCowrieWasm,
  fromHex,
  toHex,
} from "../cowrie.mjs";
import { DEFAULT_VIRTUALIZE_BYTES, runStdioMcpProxy } from "../mcp-proxy.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PKG_VERSION = "1.2.0";

function isExecutableFile(p) {
  try {
    const st = fs.statSync(p);
    if (!st.isFile()) return false;
    fs.accessSync(p, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function findNativeBinary() {
  if (process.env.COWRIE_BIN && isExecutableFile(process.env.COWRIE_BIN)) {
    return process.env.COWRIE_BIN;
  }
  const binName = process.platform === "win32" ? "cowrie.exe" : "cowrie";
  const targetDir = process.env.CARGO_TARGET_DIR || "/tmp/cowrie-cargo-target";
  const localBuilt = path.join(targetDir, "release", binName);
  if (isExecutableFile(localBuilt)) {
    return localBuilt;
  }
  return null;
}

function findWasmBytes() {
  const candidates = [
    process.env.COWRIE_WASM_PATH,
    path.resolve(__dirname, "..", "cowrie_core.wasm"),
    path.join(
      process.env.CARGO_TARGET_DIR || "/tmp/cowrie-cargo-target",
      "wasm32-unknown-unknown",
      "release",
      "cowrie_core.wasm"
    ),
  ].filter(Boolean);
  for (const c of candidates) {
    if (fs.existsSync(c)) return fs.readFileSync(c);
  }
  throw new Error(
    "cowrie_core.wasm not found. Run `make wasm` or `npm run build:wasm` in wasm/."
  );
}

function usage() {
  process.stderr.write(
    `cowrie ${PKG_VERSION} (Node+WASM / Native CLI & Zero-Trust MCP Wire Firewall)\n` +
      `Usage:\n` +
      `  npx cowrie-ai mcp-proxy [--owner-pubkey <64-hex>] [--policy <policy.json>] [--trust-server-annotations] [--virtualize | --virtualize-bytes <N>] -- <mcp_cmd> [args...]\n` +
      `  npx cowrie-ai mcp-config [--client claude-code|claude-desktop|cursor] [--bin npx|cowrie] [--virtualize] -- <server-name> <mcp_cmd> [args...]\n` +
      `  npx cowrie-ai pack <in.json> <out.cowrie> [--auth policy|verified|untrusted|synth] [--raw]\n` +
      `  npx cowrie-ai verify <file.cowrie>\n` +
      `  npx cowrie-ai lens <file.cowrie> [--tier r0|r1|r2|r2-compact|r3]\n` +
      `  npx cowrie-ai get <file.cowrie> <path>\n` +
      `  npx cowrie-ai keygen [--out-seed-file <path>]\n` +
      `  npx cowrie-ai ed25519-pubkey (<seed-64-hex> | --seed-file <path>)\n` +
      `  npx cowrie-ai declassify-sign (<seed-64-hex> | --seed-file <path>) <cid-68-hex> <action> <expiry_unix>\n` +
      `  npx cowrie-ai --version | --help  (pass --wasm to force Node+WASM engine)\n`
  );
}

function parseAuth(s) {
  switch (s) {
    case "policy": return AUTH_SYS_POLICY;
    case "verified": return AUTH_VERIFIED_TOOL;
    case "untrusted": return AUTH_UNTRUSTED_EXT;
    case "synth": return AUTH_MODEL_SYNTH;
    default:
      process.stderr.write(`Unknown --auth '${s}'\n`);
      process.exit(2);
  }
}

async function main() {
  let args = process.argv.slice(2);
  let forceWasm = false;
  if (args[0] === "--wasm") {
    forceWasm = true;
    args = args.slice(1);
  }

  if (args.length === 0) {
    usage();
    process.exit(1);
  }
  if (args[0] === "--version" || args[0] === "-V" || args[0] === "version") {
    process.stdout.write(`cowrie ${PKG_VERSION}\n`);
    return;
  }
  if (args[0] === "--help" || args[0] === "-h" || args[0] === "help") {
    usage();
    return;
  }

  if (!forceWasm) {
    const nativeBin = findNativeBinary();
    if (nativeBin) {
      const res = spawnSync(nativeBin, args, { stdio: "inherit" });
      process.exit(res.status ?? 1);
    }
  }

  const cw = await createCowrieWasm(findWasmBytes());
  const cmd = args[0];

  if (cmd === "mcp-proxy") {
    let ownerPubkeyHex = process.env.COWRIE_OWNER_PUBKEY || null;
    let policy = {};
    let trustServerAnnotations = false;
    let mode = "enforce";
    let quiet = false;
    let virtualizeBytes = 0;
    let sep = -1;
    let i = 1;
    while (i < args.length) {
      if (args[i] === "--") {
        sep = i + 1;
        break;
      } else if (args[i] === "--mode" && i + 1 < args.length) {
        mode = args[i + 1];
        if (mode !== "enforce" && mode !== "observe") {
          process.stderr.write(`Invalid --mode '${mode}' (expected enforce|observe)\n`);
          process.exit(2);
        }
        i += 2;
      } else if (args[i] === "--quiet" || args[i] === "-q") {
        quiet = true;
        i += 1;
      } else if (args[i] === "--owner-pubkey" && i + 1 < args.length) {
        ownerPubkeyHex = args[i + 1];
        i += 2;
      } else if (args[i] === "--policy" && i + 1 < args.length) {
        policy = JSON.parse(fs.readFileSync(args[i + 1], "utf8"));
        i += 2;
      } else if (args[i] === "--audit-wal" && i + 1 < args.length) {
        process.stderr.write(
          "cowrie(wasm): --audit-wal requires the native `cowrie` binary (`pip install cowrie-ai`); continuing in-memory\n"
        );
        i += 2;
      } else if (args[i] === "--trust-server-annotations") {
        trustServerAnnotations = true;
        i += 1;
      } else if (args[i] === "--virtualize") {
        virtualizeBytes = DEFAULT_VIRTUALIZE_BYTES;
        i += 1;
      } else if (args[i] === "--virtualize-bytes" && i + 1 < args.length) {
        virtualizeBytes = Number(args[i + 1]);
        if (!Number.isInteger(virtualizeBytes) || virtualizeBytes < 0) {
          process.stderr.write(`Invalid --virtualize-bytes '${args[i + 1]}'\n`);
          process.exit(2);
        }
        i += 2;
      } else {
        process.stderr.write(`Unknown mcp-proxy option '${args[i]}'\n`);
        process.exit(2);
      }
    }
    if (sep < 0 || sep >= args.length) {
      usage();
      process.exit(2);
    }
    runStdioMcpProxy(
      cw,
      { ownerPubkeyHex, policy, trustServerAnnotations, mode, quiet, virtualizeBytes },
      args.slice(sep)
    );
    return;
  }

  if (cmd === "mcp-config") {
    let client = "cursor";
    let binMode = "npx";
    let modeArg = null;
    let virtualize = true;
    let trustAnn = false;
    let sep = -1;
    let i = 1;
    while (i < args.length) {
      if (args[i] === "--") { sep = i + 1; break; }
      else if (args[i] === "--client" && i + 1 < args.length) { client = args[i + 1]; i += 2; }
      else if (args[i] === "--bin" && i + 1 < args.length) { binMode = args[i + 1]; i += 2; }
      else if (args[i] === "--mode" && i + 1 < args.length) { modeArg = args[i + 1]; i += 2; }
      else if (args[i] === "--virtualize") { virtualize = true; i += 1; }
      else if (args[i] === "--no-virtualize") { virtualize = false; i += 1; }
      else if (args[i] === "--trust-server-annotations") { trustAnn = true; i += 1; }
      else { process.stderr.write(`Unknown mcp-config option '${args[i]}'\n`); process.exit(2); }
    }
    if (sep < 0 || sep + 1 >= args.length) {
      process.stderr.write("mcp-config requires `-- <server-name> <mcp_cmd> [args...]`\n");
      process.exit(2);
    }
    const serverName = args[sep];
    const targetCmd = args.slice(sep + 1);
    const command = binMode === "cowrie" ? "cowrie" : "npx";
    const proxyArgs = binMode === "cowrie" ? ["mcp-proxy"] : ["-y", "cowrie-ai", "mcp-proxy"];
    if (modeArg) proxyArgs.push("--mode", modeArg);
    if (virtualize) proxyArgs.push("--virtualize");
    if (trustAnn) proxyArgs.push("--trust-server-annotations");
    proxyArgs.push("--", ...targetCmd);
    if (client === "claude-code") {
      process.stdout.write(`claude mcp add ${serverName} -- ${command} ${proxyArgs.join(" ")}\n`);
    } else {
      process.stdout.write(
        `${JSON.stringify({ mcpServers: { [serverName]: { command, args: proxyArgs } } }, null, 2)}\n`
      );
    }
    return;
  }

  if (cmd === "pack") {
    if (args.length < 3) { usage(); process.exit(1); }
    let auth = AUTH_VERIFIED_TOOL;
    let rawMode = false;
    for (let i = 3; i < args.length; i++) {
      if (args[i] === "--auth" && i + 1 < args.length) { auth = parseAuth(args[++i]); }
      else if (args[i] === "--raw") { rawMode = true; }
      else { process.stderr.write(`Unknown pack option '${args[i]}'\n`); process.exit(2); }
    }
    const text = fs.readFileSync(args[1], "utf8");
    const frame = cw.encodeJson(text, auth, EPI_OBSERVED);
    const info = cw.verifyFrame(frame);
    if (rawMode) {
      fs.writeFileSync(args[2], frame);
      process.stdout.write(`PACKED ${args[1]} -> ${args[2]} | raw COWR frame=${frame.length}B | cid=${info.cidHex}\n`);
    } else {
      const shard = cw.packSingleShard(frame);
      fs.writeFileSync(args[2], shard);
      process.stdout.write(`PACKED ${args[1]} -> ${args[2]} | frame=${frame.length}B | shard=${shard.length}B (16KB aligned) | cid=${info.cidHex}\n`);
    }
    return;
  }

  if (cmd === "verify") {
    if (args.length < 2) { usage(); process.exit(1); }
    const buf = new Uint8Array(fs.readFileSync(args[1]));
    if (buf.length >= 4 && buf[0] === 0x43 && buf[1] === 0x4f && buf[2] === 0x57 && buf[3] === 0x52) {
      const info = cw.verifyFrame(buf);
      process.stdout.write(`VERIFY PASS | raw COWR frame=${buf.length}B | auth=0x${info.authority.toString(16).padStart(2, "0")} | cid=${info.cidHex}\n`);
    } else {
      const rep = cw.verifyShard(buf);
      process.stdout.write(`VERIFY PASS | root=${rep.rootHex}\n`);
    }
    return;
  }

  if (cmd === "lens") {
    if (args.length < 2) { usage(); process.exit(1); }
    let tier = "r2";
    for (let i = 2; i < args.length; i++) {
      if (args[i] === "--tier" && i + 1 < args.length) tier = args[++i];
    }
    const buf = new Uint8Array(fs.readFileSync(args[1]));
    const frame = buf[0] === 0x43 && buf[1] === 0x4f && buf[2] === 0x57 && buf[3] === 0x52
      ? buf
      : (() => {
          cw.verifyShard(buf);
          const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
          const off = Number(dv.getBigUint64(16, true));
          const len = Number(dv.getBigUint64(24, true));
          return buf.subarray(off, off + len);
        })();
    if (tier === "r2-compact") process.stdout.write(cw.projectR2Compact(frame));
    else process.stdout.write(`${cw.projectLens(frame, tier, 0)}${tier === "r0" || tier === "r3" ? "\n" : ""}`);
    return;
  }

  if (cmd === "get") {
    if (args.length < 3) { usage(); process.exit(1); }
    const buf = new Uint8Array(fs.readFileSync(args[1]));
    const frame = buf[0] === 0x43 && buf[1] === 0x4f && buf[2] === 0x57 && buf[3] === 0x52
      ? buf
      : (() => {
          cw.verifyShard(buf);
          const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
          const off = Number(dv.getBigUint64(16, true));
          const len = Number(dv.getBigUint64(24, true));
          return buf.subarray(off, off + len);
        })();
    process.stdout.write(`${cw.extractField(frame, args[2])}\n`);
    return;
  }

  if (cmd === "keygen") {
    let outSeedFile = null;
    for (let i = 1; i < args.length; i++) {
      if (args[i] === "--out-seed-file" && i + 1 < args.length) outSeedFile = args[++i];
    }
    const seed = new Uint8Array(crypto.randomBytes(32));
    const pk = cw.ed25519Pubkey(seed);
    if (outSeedFile) {
      fs.writeFileSync(outSeedFile, `${toHex(seed)}\n`, { mode: 0o600 });
      process.stdout.write(`pubkey=${toHex(pk)}\n`);
    } else {
      process.stdout.write(`seed=${toHex(seed)}\npubkey=${toHex(pk)}\n`);
    }
    return;
  }

  if (cmd === "ed25519-pubkey") {
    const seedHex = args[1] === "--seed-file" ? fs.readFileSync(args[2], "utf8").trim() : args[1];
    process.stdout.write(`${toHex(cw.ed25519Pubkey(fromHex(seedHex)))}\n`);
    return;
  }

  if (cmd === "declassify-sign") {
    let seedHex, cidHex, act, exp;
    if (args[1] === "--seed-file") {
      seedHex = fs.readFileSync(args[2], "utf8").trim();
      [cidHex, act, exp] = [args[3], args[4], args[5]];
    } else {
      [seedHex, cidHex, act, exp] = [args[1], args[2], args[3], args[4]];
    }
    const sig = cw.signDeclassification(fromHex(seedHex), fromHex(cidHex), act, BigInt(exp));
    process.stdout.write(`${toHex(sig)}\n`);
    return;
  }

  usage();
  process.exit(1);
}

main().catch((err) => {
  process.stderr.write(`cowrie error: ${err.message}\n`);
  process.exit(1);
});
