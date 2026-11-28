/**
 * Zero-dependency, pure Web-Standard (browser / Cloudflare Workers / Deno / Node) Cowrie MCP
 * protocol mediator backed by `cowrie_core.wasm`.
 *
 * Strictly NO `node:*` imports and NO Node `Buffer` globals so it runs natively inside
 * Cloudflare Workers V8 Isolates (zero `nodejs_compat` required) and browsers.
 */

import {
  AUTH_UNTRUSTED_EXT,
  AUTH_VERIFIED_TOOL,
  EPI_OBSERVED,
  fromHex,
} from "./cowrie.mjs";

export const DEFAULT_VIRTUALIZE_BYTES = 16384;
export const MAX_VIRTUAL_ENTRIES = 512;

const _enc = new TextEncoder();
const utf8ByteLength = (s) => _enc.encode(s).length;

const LOCAL_READONLY_TOOLS = new Set([
  "read_file", "get_file", "list_dir", "list_directory", "list_files", "ls",
  "stat", "grep", "rg", "search_files", "find_files", "inspect", "status",
  "get_status", "ping", "echo", "add", "get_time", "health", "help",
  "describe", "version", "list_tables", "query_readonly",
]);

const RESERVED_SENTINELS = new Set(["$bytes", "$tensor", "$float", "$map"]);

const COWRIE_GET_TOOL_DEF = {
  name: "cowrie_get",
  description:
    "Retrieve an exact RFC 6901 JSON Pointer, line range, or bounded grep slice from a virtualized Cowrie handle (preserving causal taint).",
  annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
  inputSchema: {
    type: "object",
    required: ["handle"],
    properties: {
      handle: {
        type: "string",
        description: "10-letter Cowrie handle from VIRTUALIZED_TOOL_OUTPUT (e.g. @abcdefghij)",
      },
      pointer: {
        type: "string",
        description: "Optional RFC 6901 JSON Pointer (e.g. /results/3, /output, /text)",
      },
      grep: {
        type: "string",
        description: "Optional substring or bounded wildcard pattern to search inside the target",
      },
      lines: {
        type: "array",
        items: { type: "integer" },
        description: "Optional 1-based inclusive [start, end] line range",
      },
      offset: { type: "integer", description: "Optional char offset for pagination" },
      max_chars: {
        type: "integer",
        description: "Optional max output characters (default 2400, max 8192)",
      },
    },
  },
};

function isHexDigit(ch) {
  return (
    (ch >= "0" && ch <= "9") ||
    (ch >= "a" && ch <= "f") ||
    (ch >= "A" && ch <= "F")
  );
}

/** Fail-closed RFC 8259 JSON parser rejecting duplicate object keys, bad escapes, and >64 depth. */
export function parseStrictJson(text) {
  if (typeof text !== "string") throw new Error("ERR_INVALID_JSON: expected string input");
  let i = 0;
  const n = text.length;
  const skipWs = () => {
    while (i < n && (text[i] === " " || text[i] === "\t" || text[i] === "\r" || text[i] === "\n")) i++;
  };
  const scanString = () => {
    if (text[i] !== '"') throw new Error("ERR_INVALID_JSON: expected string");
    const start = i++;
    while (i < n) {
      const ch = text[i++];
      if (ch === "\\") {
        if (i >= n) throw new Error("ERR_INVALID_JSON: unterminated escape");
        const esc = text[i++];
        if (esc === "u") {
          if (i + 4 > n) throw new Error("ERR_INVALID_JSON: truncated unicode escape");
          for (let h = 0; h < 4; h++) {
            if (!isHexDigit(text[i + h])) throw new Error("ERR_INVALID_JSON: invalid unicode hex");
          }
          i += 4;
        } else if (!'"\\/bfnrt'.includes(esc)) {
          throw new Error("ERR_INVALID_JSON: invalid escape sequence");
        }
      } else if (ch === '"') {
        return JSON.parse(text.slice(start, i));
      } else if (ch.charCodeAt(0) < 0x20) {
        throw new Error("ERR_INVALID_JSON: unescaped control character");
      }
    }
    throw new Error("ERR_INVALID_JSON: unterminated string");
  };
  const scanValue = (depth) => {
    if (depth > 64) throw new Error("ERR_DEPTH_EXCEEDED");
    skipWs();
    if (i >= n) throw new Error("ERR_INVALID_JSON: unexpected EOF");
    const ch = text[i];
    if (ch === "{") {
      i++;
      skipWs();
      const seen = new Set();
      if (text[i] === "}") { i++; return; }
      while (true) {
        skipWs();
        const k = scanString();
        if (seen.has(k)) throw new Error(`ERR_DUPLICATE_KEY: '${k}'`);
        seen.add(k);
        skipWs();
        if (text[i] !== ":") throw new Error("ERR_INVALID_JSON: expected ':'");
        i++;
        scanValue(depth + 1);
        skipWs();
        if (text[i] === ",") { i++; continue; }
        if (text[i] === "}") { i++; return; }
        throw new Error("ERR_INVALID_JSON: expected ',' or '}'");
      }
    } else if (ch === "[") {
      i++;
      skipWs();
      if (text[i] === "]") { i++; return; }
      while (true) {
        scanValue(depth + 1);
        skipWs();
        if (text[i] === ",") { i++; continue; }
        if (text[i] === "]") { i++; return; }
        throw new Error("ERR_INVALID_JSON: expected ',' or ']'");
      }
    } else if (ch === '"') {
      scanString();
    } else if (ch === "t" && text.startsWith("true", i)) {
      i += 4;
    } else if (ch === "f" && text.startsWith("false", i)) {
      i += 5;
    } else if (ch === "n" && text.startsWith("null", i)) {
      i += 4;
    } else {
      const m = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(text.slice(i));
      if (!m) throw new Error("ERR_INVALID_JSON: invalid token");
      i += m[0].length;
    }
  };
  scanValue(0);
  skipWs();
  if (i !== n) throw new Error("ERR_INVALID_JSON: trailing garbage");
  return JSON.parse(text);
}

/** Escape literal untrusted JSON objects containing reserved Cowrie sentinel keys (`$bytes`/`$tensor`/`$float`/`$map`). */
function toCowrieSafeValue(v, depth = 0) {
  if (depth > 56) {
    return typeof v === "string" ? v : JSON.stringify(v);
  }
  if (v === null || typeof v === "boolean" || typeof v === "string") return v;
  if (typeof v === "number") {
    if (Number.isNaN(v)) return { $float: "NaN" };
    if (v === Infinity) return { $float: "Infinity" };
    if (v === -Infinity) return { $float: "-Infinity" };
    return Object.is(v, -0) ? 0 : v;
  }
  if (Array.isArray(v)) return v.map((item) => toCowrieSafeValue(item, depth + 1));
  if (typeof v === "object") {
    const out = {};
    let hasSentinel = false;
    for (const [k, val] of Object.entries(v)) {
      if (RESERVED_SENTINELS.has(k)) hasSentinel = true;
      out[k] = toCowrieSafeValue(val, depth + 1);
    }
    return hasSentinel ? { $map: out } : out;
  }
  return String(v);
}

export function normalizeRpcId(id) {
  if (id === undefined) return null;
  if (id === null) return "null";
  if (typeof id === "number") return Number.isInteger(id) ? `n:${id}` : `f:${id}`;
  if (typeof id === "string") return `s:${id}`;
  return `o:${JSON.stringify(id)}`;
}

export function classifyMcpTool(toolName, readonlyHints, policy) {
  if (policy.privilegedOverrides?.has(toolName)) {
    return policy.privilegedOverrides.get(toolName);
  }
  const lower = String(toolName || "").toLowerCase();
  if (/(shell|bash|exec|command|eval|spawn|terminal)/.test(lower)) return "shell_exec";
  if (/(cred|secret|token|passwd|apikey|api_key)/.test(lower)) return "cred_read";
  if (/(write|delete|remove|edit|patch|chmod|create_file|mkdir)/.test(lower)) return "fs_write";
  if (/(post|send|upload|exfil|webhook|http|fetch|url|curl|web|browser|scrape|email|mail|slack)/.test(lower)) {
    return "net_exfil";
  }
  if (
    policy.allowReadonly?.has(toolName) ||
    LOCAL_READONLY_TOOLS.has(lower) ||
    (policy.trustServerAnnotations && readonlyHints.get(toolName) === true)
  ) {
    return "read_only";
  }
  return "shell_exec";
}

export function isUntrustedSourceTool(toolName, openWorldHints, policy) {
  if (policy.untrustedSources?.has(toolName) || openWorldHints.get(toolName) === true) {
    return true;
  }
  const lower = String(toolName || "").toLowerCase();
  return /(web|fetch|http|url|browser|scrape|search|email|mail|slack|issue|external|curl|github|jira|rss)/.test(lower);
}

function unwrapMcpResultPayload(msg) {
  const res = msg?.result;
  if (!res || typeof res !== "object") return null;
  if (Array.isArray(res.content) && res.content.length > 0 && res.content.every((b) => b?.type === "text" && typeof b?.text === "string")) {
    const joined = res.content.map((b) => b.text).join("\n");
    const rawBytes = utf8ByteLength(joined);
    const trimmed = joined.trim();
    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      try {
        return { payload: parseStrictJson(trimmed), rawBytes };
      } catch {
        // Fall back to text envelope
      }
    }
    return { payload: { text: joined }, rawBytes };
  }
  if (Array.isArray(res.contents)) {
    const texts = res.contents.filter((b) => typeof b?.text === "string").map((b) => b.text);
    if (texts.length > 0) {
      const joined = texts.join("\n");
      const rawBytes = utf8ByteLength(joined);
      const trimmed = joined.trim();
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        try {
          return { payload: parseStrictJson(trimmed), rawBytes };
        } catch {
          // Fall back to text envelope
        }
      }
      return { payload: { text: joined }, rawBytes };
    }
  }
  const rawJson = JSON.stringify(res);
  return { payload: res, rawBytes: utf8ByteLength(rawJson) };
}

/**
 * Stateful Cowrie MCP protocol mediator backed by `cowrie_core.wasm`.
 * Zero `node:*` imports; reusable in stdio proxies, Cloudflare Workers / Durable Objects, and browsers.
 */
export class CowrieMcpMediator {
  constructor(cw, options = {}) {
    this.cw = cw;
    this.virtualizeBytes = Number(options.virtualizeBytes ?? 0);
    this.mode = options.mode === "observe" ? "observe" : "enforce";
    this.quiet = Boolean(options.quiet);
    this.onLog = typeof options.onLog === "function" ? options.onLog : null;
    this.ownerPubkey =
      options.ownerPubkey instanceof Uint8Array
        ? options.ownerPubkey
        : typeof options.ownerPubkeyHex === "string" && options.ownerPubkeyHex.length === 64
          ? fromHex(options.ownerPubkeyHex)
          : null;
    this.policy = {
      allowReadonly: new Set(options.policy?.allow_readonly || []),
      untrustedSources: new Set(options.policy?.untrusted_sources || []),
      privilegedOverrides: new Map(Object.entries(options.policy?.privileged || {})),
      trustServerAnnotations: Boolean(options.trustServerAnnotations || options.policy?.trust_server_annotations),
    };
    this.pendingCalls = new Map();
    this.toolReadonlyHints = new Map();
    this.toolOpenWorldHints = new Map();
    this.knownCids = new Map(); // cidHex -> { cid: Uint8Array(34), tainted: bool } (Bend Law L04/L03!)
    this.taintedContextCids = new Map(); // cidHex -> Uint8Array(34)
    this.virtualStore = new Map();
    this.virtualOrder = [];
    this.evictedHandles = [];
    this.stats = { virtualized: 0, rawBytes: 0, atlasBytes: 0, blocked: 0, wouldBlock: 0 };
  }

  _log(line) {
    if (this.quiet) return;
    if (this.onLog) this.onLog(line);
    else if (typeof process !== "undefined" && process.stderr?.write) {
      process.stderr.write(`${line}\n`);
    }
  }

  recordCid(cidHex, cidU8, tainted) {
    const k = cidHex.toLowerCase();
    const prev = this.knownCids.get(k);
    // Sticky taint (Bend Law L03): once tainted, never downgrade!
    const stickyTainted = Boolean(tainted || prev?.tainted);
    this.knownCids.set(k, { cid: cidU8, tainted: stickyTainted });
    if (stickyTainted) {
      this.taintedContextCids.set(k, cidU8);
    }
  }

  _storeVirtual(handle, entry) {
    if (!this.virtualStore.has(handle)) {
      if (this.virtualOrder.length >= MAX_VIRTUAL_ENTRIES) {
        const old = this.virtualOrder.shift();
        if (old) {
          this.virtualStore.delete(old);
          this.evictedHandles.push(old);
        }
      }
      this.virtualOrder.push(handle);
    }
    this.virtualStore.set(handle, entry);
  }

  _encodeSafe(obj, auth) {
    try {
      return this.cw.encodeJson(toCowrieSafeValue(obj), auth, EPI_OBSERVED);
    } catch {
      const text = typeof obj === "string" ? obj : JSON.stringify(obj);
      return this.cw.encodeJson({ text: String(text || "") }, auth, EPI_OBSERVED);
    }
  }

  _handleLocalCowrieGet(id, params) {
    const args = params?.arguments && typeof params.arguments === "object" ? params.arguments : {};
    const handle = typeof args.handle === "string" ? args.handle : null;
    if (!handle) {
      return {
        jsonrpc: "2.0",
        id: id ?? null,
        error: { code: -32026, message: "ERR_INVALID_CONTRACT: cowrie_get requires `handle`" },
      };
    }
    const entry = this.virtualStore.get(handle);
    if (!entry) {
      return {
        jsonrpc: "2.0",
        id: id ?? null,
        error: { code: -32026, message: `ERR_INVALID_CONTRACT: unknown virtualized handle '${handle}'` },
      };
    }
    const tainted = this.cw.isTainted(entry.authority);
    this.recordCid(entry.cidHex, entry.cid, tainted);

    try {
      const slice = this.cw.queryFrameSlice(entry.frame, {
        pointer: typeof args.pointer === "string" ? args.pointer : undefined,
        grep: typeof args.grep === "string" ? args.grep : undefined,
        lines: Array.isArray(args.lines) ? args.lines : undefined,
        offset: Number(args.offset || 0),
        maxChars: Number(args.max_chars || 2400),
      });
      const delivered = tainted
        ? `<<<COWRIE:SLICE ${handle} AUTH=0x${entry.authority.toString(16).padStart(2, "0")}(UNTRUSTED_EXT|TAINTED)>>>\n${slice}\n<<<END:COWRIE>>>`
        : slice;
      return {
        jsonrpc: "2.0",
        id: id ?? null,
        result: {
          content: [{ type: "text", text: delivered }],
          _meta: { cowrie: { cid: entry.cidHex, handle, tainted } },
        },
      };
    } catch (err) {
      return {
        jsonrpc: "2.0",
        id: id ?? null,
        error: { code: -32026, message: `ERR_INVALID_CONTRACT: cowrie_get failed (${err.message})` },
      };
    }
  }

  handleClientLine(rawLine, nowUnixSecs = Math.floor(Date.now() / 1000)) {
    let parsed;
    try {
      parsed = parseStrictJson(rawLine);
    } catch (err) {
      return {
        localResponse: JSON.stringify({
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: String(err.message || err) },
        }),
      };
    }
    if (Array.isArray(parsed)) {
      if (parsed.length === 0) {
        return {
          localResponse: JSON.stringify({
            jsonrpc: "2.0",
            id: null,
            error: { code: -32700, message: "ERR_INVALID_JSON: empty batch" },
          }),
        };
      }
      // Stage pending entries and ONLY commit if every item in the batch is allowed!
      const out = [];
      const stagedPending = [];
      for (const item of parsed) {
        const res = this._evaluateRequestPure(item, nowUnixSecs);
        if (res.localResponse) return { localResponse: JSON.stringify(res.localResponse) };
        out.push(res.forward);
        if (res.pending) stagedPending.push(res.pending);
      }
      for (const [idNorm, pItem] of stagedPending) {
        this._pushPending(idNorm, pItem);
      }
      return { forwardLine: JSON.stringify(out) };
    }
    if (!parsed || typeof parsed !== "object") {
      return {
        localResponse: JSON.stringify({
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: "ERR_INVALID_JSON: JSON-RPC payload must be object or array" },
        }),
      };
    }
    const res = this._evaluateRequestPure(parsed, nowUnixSecs);
    if (res.localResponse) return { localResponse: JSON.stringify(res.localResponse) };
    if (res.pending) this._pushPending(res.pending[0], res.pending[1]);
    return { forwardLine: JSON.stringify(res.forward) };
  }

  evaluateRequest(msg, nowUnixSecs = Math.floor(Date.now() / 1000)) {
    const res = this._evaluateRequestPure(msg, nowUnixSecs);
    if (res.pending && !res.localResponse) {
      this._pushPending(res.pending[0], res.pending[1]);
    }
    return res;
  }

  _evaluateRequestPure(msg, nowUnixSecs) {
    if (!msg || typeof msg !== "object" || Array.isArray(msg)) {
      return {
        localResponse: {
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: "ERR_INVALID_JSON: JSON-RPC request must be an object" },
        },
      };
    }
    const method = typeof msg.method === "string" ? msg.method : "";
    const idNorm = normalizeRpcId(msg.id);

    if (method === "tools/call") {
      const params = msg.params && typeof msg.params === "object" ? msg.params : {};
      const toolName = typeof params.name === "string" ? params.name : "";

      if (this.virtualizeBytes > 0 && toolName === "cowrie_get") {
        return { localResponse: this._handleLocalCowrieGet(msg.id, params) };
      }

      const action = classifyMcpTool(toolName, this.toolReadonlyHints, this.policy);
      const isPrivileged = action !== "read_only";

      let lineage = null;
      if (Object.prototype.hasOwnProperty.call(params, "_cowrie_parents")) {
        const pv = params._cowrie_parents;
        if (!Array.isArray(pv) || pv.length === 0) {
          return {
            localResponse: {
              jsonrpc: "2.0",
              id: msg.id ?? null,
              error: {
                code: -32025,
                message: "ERR_TAINTED_EXECUTION_BLOCKED",
                data: { tool: toolName, tainted_cid: "empty_parent_lineage" },
              },
            },
          };
        }
        lineage = [];
        for (const s of pv) {
          if (typeof s !== "string" || s.length !== 68 || !/^[0-9a-fA-F]{68}$/.test(s)) {
            return {
              localResponse: {
                jsonrpc: "2.0",
                id: msg.id ?? null,
                error: {
                  code: -32025,
                  message: "ERR_TAINTED_EXECUTION_BLOCKED",
                  data: { tool: toolName, tainted_cid: "invalid_parent_cid" },
                },
              },
            };
          }
          const lowerCid = s.toLowerCase();
          const rec = this.knownCids.get(lowerCid);
          // Bend Law L04: unknown parent CID MUST fail closed!
          if (!rec) {
            if (isPrivileged && this.mode === "enforce") {
              this.stats.blocked += 1;
              return {
                localResponse: {
                  jsonrpc: "2.0",
                  id: msg.id ?? null,
                  error: {
                    code: -32025,
                    message: "ERR_TAINTED_EXECUTION_BLOCKED",
                    data: { tool: toolName, tainted_cid: lowerCid },
                  },
                },
              };
            }
            if (isPrivileged && this.mode === "observe") {
              this.stats.wouldBlock += 1;
              this._log(`[cowrie][observe] WOULD BLOCK unknown parent CID '${lowerCid.slice(0, 16)}...' for '${toolName}'`);
            }
            continue;
          }
          if (rec.tainted) {
            lineage.push([lowerCid, rec.cid]);
          }
        }
        if (lineage.length === 0) lineage = null;
      } else if (this.taintedContextCids.size > 0) {
        lineage = Array.from(this.taintedContextCids.entries());
      }

      if (isPrivileged && lineage && lineage.length > 0) {
        const rawTokens = Array.isArray(params._cowrie_declassify) ? params._cowrie_declassify : [];
        let allCleared = Boolean(this.ownerPubkey);
        if (allCleared) {
          for (const [cidHex, cidU8] of lineage) {
            const matched = rawTokens.some((t) => {
              if (!t || typeof t !== "object") return false;
              if (String(t.cid || "").toLowerCase() !== cidHex) return false;
              if (typeof t.sig !== "string" || t.sig.length !== 128) return false;
              if (typeof t.expiry !== "number" || !Number.isInteger(t.expiry) || t.expiry < 0) return false;
              try {
                return this.cw.verifyDeclassification(
                  this.ownerPubkey,
                  cidU8,
                  action,
                  t.expiry,
                  fromHex(t.sig),
                  nowUnixSecs
                );
              } catch {
                return false;
              }
            });
            if (!matched) {
              allCleared = false;
              break;
            }
          }
        }
        if (!allCleared) {
          if (this.mode === "observe") {
            this.stats.wouldBlock += 1;
            this._log(
              `[cowrie][observe] WOULD BLOCK tainted call '${toolName}' (action=${action}, cid=${lineage[0][0].slice(0, 16)}...) — pass --mode enforce to hard-block`
            );
          } else {
            this.stats.blocked += 1;
            this._log(
              `[cowrie][enforce] BLOCKED tainted call '${toolName}' (code=-32025, cid=${lineage[0][0].slice(0, 16)}...)`
            );
            return {
              localResponse: {
                jsonrpc: "2.0",
                id: msg.id ?? null,
                error: {
                  code: -32025,
                  message: "ERR_TAINTED_EXECUTION_BLOCKED",
                  data: { tool: toolName, tainted_cid: lineage[0][0] },
                },
              },
            };
          }
        }
      }

      const sanitized = { ...msg };
      if (sanitized.params && typeof sanitized.params === "object") {
        const cleanParams = { ...sanitized.params };
        delete cleanParams._cowrie_declassify;
        delete cleanParams._cowrie_parents;
        sanitized.params = cleanParams;
      }
      const pending = idNorm !== null ? [idNorm, { kind: "tools/call", toolName }] : null;
      return { forward: sanitized, pending };
    }

    if (method === "tools/list" && idNorm !== null) return { forward: msg, pending: [idNorm, { kind: "tools/list" }] };
    if (method === "resources/read" && idNorm !== null) return { forward: msg, pending: [idNorm, { kind: "resources/read" }] };
    if (method === "prompts/get" && idNorm !== null) return { forward: msg, pending: [idNorm, { kind: "prompts/get" }] };

    return { forward: msg, pending: null };
  }

  _pushPending(idNorm, item) {
    if (!this.pendingCalls.has(idNorm)) this.pendingCalls.set(idNorm, []);
    this.pendingCalls.get(idNorm).push(item);
  }

  _popPending(idNorm) {
    const q = this.pendingCalls.get(idNorm);
    if (!q || q.length === 0) return null;
    const item = q.shift();
    if (q.length === 0) this.pendingCalls.delete(idNorm);
    return item;
  }

  handleServerLine(rawLine) {
    let parsed;
    try {
      parsed = parseStrictJson(rawLine);
    } catch {
      if (this.pendingCalls.size > 0) {
        const frame = this._encodeSafe({ raw: rawLine }, AUTH_UNTRUSTED_EXT);
        const info = this.cw.verifyFrame(frame);
        this.recordCid(info.cidHex, info.cid, true);
      }
      return rawLine;
    }
    if (Array.isArray(parsed)) {
      let changed = false;
      for (const item of parsed) {
        if (this.processServerMessage(item)) changed = true;
      }
      return changed ? JSON.stringify(parsed) : rawLine;
    }
    return this.processServerMessage(parsed) ? JSON.stringify(parsed) : rawLine;
  }

  processServerMessage(msg) {
    if (!msg || typeof msg !== "object") return false;
    const idNorm = normalizeRpcId(msg.id);
    if (idNorm === null) return false;
    const hasResult = Object.prototype.hasOwnProperty.call(msg, "result");
    const hasError = Object.prototype.hasOwnProperty.call(msg, "error");
    if (!hasResult && !hasError) return false;

    const pending = this._popPending(idNorm);
    if (!pending) return false;

    if (pending.kind === "tools/list") {
      const tools = msg.result?.tools;
      if (Array.isArray(tools)) {
        for (const t of tools) {
          if (typeof t?.name === "string") {
            const ann = t.annotations || {};
            const ro = ann.readOnlyHint === true;
            const dest = ann.destructiveHint === true;
            const openWorld = ann.openWorldHint === true;
            this.toolReadonlyHints.set(t.name, ro && !dest && !openWorld);
            this.toolOpenWorldHints.set(t.name, openWorld);
          }
        }
        if (this.virtualizeBytes > 0) {
          if (!tools.some((t) => t?.name === "cowrie_get")) {
            tools.push(COWRIE_GET_TOOL_DEF);
          }
          return true;
        }
      }
      return false;
    }

    const untrusted =
      pending.kind === "resources/read" ||
      pending.kind === "prompts/get" ||
      isUntrustedSourceTool(pending.toolName, this.toolOpenWorldHints, this.policy);
    const auth = untrusted ? AUTH_UNTRUSTED_EXT : AUTH_VERIFIED_TOOL;
    const tainted = this.cw.isTainted(auth);

    const frame = this._encodeSafe(msg, auth);
    const info = this.cw.verifyFrame(frame);
    this.recordCid(info.cidHex, info.cid, tainted);

    if (!hasResult || !msg.result || typeof msg.result !== "object") return false;

    if (this.virtualizeBytes > 0) {
      const unwrapped = unwrapMcpResultPayload(msg);
      if (unwrapped && unwrapped.rawBytes >= this.virtualizeBytes) {
        try {
          const pFrame = this._encodeSafe(unwrapped.payload, auth);
          const pInfo = this.cw.verifyFrame(pFrame);
          const handle = this.cw.alphaHandle(pInfo.cid);
          const atlas = this.cw.projectVirtualAtlas(pFrame, 1800);
          const atlasBytes = utf8ByteLength(atlas);
          this.stats.virtualized += 1;
          this.stats.rawBytes += unwrapped.rawBytes;
          this.stats.atlasBytes += atlasBytes;
          const pct = unwrapped.rawBytes > 0 ? ((1 - atlasBytes / unwrapped.rawBytes) * 100).toFixed(1) : "0.0";
          const estTok = Math.max(0, Math.floor((unwrapped.rawBytes - atlasBytes) / 4));
          const label = pending.toolName || pending.kind;
          this._log(
            `[cowrie] virtualized '${label}' ${(unwrapped.rawBytes / 1024).toFixed(1)} KB -> ${(atlasBytes / 1024).toFixed(1)} KB (-${pct}%, ~${estTok} tok saved) handle=${handle} tainted=${tainted}`
          );
          this._storeVirtual(handle, {
            cid: info.cid,
            cidHex: info.cidHex,
            frame: pFrame,
            authority: auth,
          });
          delete msg.result.contents;
          msg.result.content = [{ type: "text", text: atlas }];
          msg.result._meta = {
            ...(msg.result._meta && typeof msg.result._meta === "object" ? msg.result._meta : {}),
            cowrie: {
              atlas_bytes: atlasBytes,
              cid: info.cidHex,
              handle,
              raw_bytes: unwrapped.rawBytes,
              tainted,
              virtualized: true,
            },
          };
          return true;
        } catch {
          // Fall through to metadata attachment
        }
      }
    }

    msg.result._meta = {
      ...(msg.result._meta && typeof msg.result._meta === "object" ? msg.result._meta : {}),
      cowrie: { cid: info.cidHex, tainted },
    };
    return true;
  }
}
