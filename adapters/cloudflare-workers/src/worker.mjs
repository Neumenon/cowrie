/**
 * Cowrie v1.2 Cloudflare Workers Edge MCP Proxy, Durable Object Session Store & Provenance Gateway.
 *
 * Runs zero-import `cowrie_core.wasm` (~102 KiB gzipped, ~0.7 ms isolate startup) natively inside
 * Cloudflare V8 Isolates with ZERO external npm dependencies:
 *   - GET  /health               : Liveness & engine metadata
 *   - POST /v1/verify            : Zero-alloc COWR/CWRF canonical + Merkle verification
 *   - POST /v1/lens              : Deterministic R0 / R1 / R2 / R2-Compact / Virtual Atlas lens
 *   - POST /v1/declassify/verify : Strict RFC 8032 Ed25519 capability token verification
 *   - POST /mcp                  : Streamable HTTP JSON-RPC 2.0 MCP proxy with `--virtualize`,
 *                                  synthetic `cowrie_get`, and causal `0x80` / `Ed25519` gating
 *                                  (stateful across isolate evictions via `CowrieMcpSessionDO`)
 */

import {
  AUTH_UNTRUSTED_EXT,
  AUTH_VERIFIED_TOOL,
  EPI_OBSERVED,
  createCowrieWasm,
  fromHex,
} from "../../../wasm/cowrie.mjs";
import { CowrieMcpMediator, DEFAULT_VIRTUALIZE_BYTES } from "../../../wasm/mcp-mediator.mjs";

const DO_CHUNK_BYTES = 65536; // Stay well under Cloudflare DO 128 KiB per-key value limit

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function instantiateCowrieWasm(wasmSource) {
  if (wasmSource instanceof WebAssembly.Module) {
    const inst = await WebAssembly.instantiate(wasmSource, {});
    const origInst = WebAssembly.instantiate;
    WebAssembly.instantiate = async () => ({ instance: inst, module: wasmSource });
    try {
      return await createCowrieWasm(new Uint8Array(0));
    } finally {
      WebAssembly.instantiate = origInst;
    }
  }
  return createCowrieWasm(wasmSource);
}

function buildMediatorOptions(env) {
  const vBytes = Number(env?.COWRIE_VIRTUALIZE_BYTES ?? DEFAULT_VIRTUALIZE_BYTES);
  return {
    virtualizeBytes: Number.isFinite(vBytes) ? vBytes : DEFAULT_VIRTUALIZE_BYTES,
    ownerPubkeyHex: env?.COWRIE_OWNER_PUBKEY || null,
    trustServerAnnotations: env?.COWRIE_TRUST_SERVER_ANNOTATIONS === "1",
    mode: env?.COWRIE_MODE === "observe" ? "observe" : "enforce",
    quiet: true,
  };
}

async function runMediatedMcpRequest(mediator, request, env, sessionId) {
  const rawReq = await request.text();
  const decision = mediator.handleClientLine(rawReq);
  if (decision.localResponse) {
    return new Response(decision.localResponse, {
      status: 200,
      headers: { "content-type": "application/json; charset=utf-8" },
    });
  }

  const upstreamUrl = env?.UPSTREAM_MCP_URL;
  if (!upstreamUrl) {
    return jsonResponse(
      {
        jsonrpc: "2.0",
        id: null,
        error: { code: -32000, message: "UPSTREAM_MCP_URL is not configured" },
      },
      502
    );
  }

  const fetcher = env?.UPSTREAM_FETCH || fetch;
  const upstreamRes = await fetcher(upstreamUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(request.headers.get("authorization")
        ? { authorization: request.headers.get("authorization") }
        : {}),
      "mcp-session-id": sessionId,
    },
    body: decision.forwardLine,
  });
  const upstreamText = await upstreamRes.text();
  const mediated = mediator.handleServerLine(upstreamText);
  return new Response(mediated, {
    status: upstreamRes.status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

/**
 * Create a Cloudflare Durable Object class persisting Cowrie MCP session state
 * (tainted CIDs + virtualized `COWR` frames) across global V8 isolate evictions & hibernation.
 */
export function createCowrieSessionDO(wasmSource) {
  let sharedCwPromise = null;
  const getWasm = () => {
    if (!sharedCwPromise) sharedCwPromise = instantiateCowrieWasm(wasmSource);
    return sharedCwPromise;
  };

  return class CowrieMcpSessionDO {
    constructor(state, env) {
      this.state = state;
      this.env = env || {};
      this.mediator = null;
      this.hydrated = false;
    }

    async _ensureHydrated(cw) {
      if (this.hydrated && this.mediator) return this.mediator;
      const m = new CowrieMcpMediator(cw, buildMediatorOptions(this.env));
      const storage = this.state?.storage;
      if (storage) {
        const savedKnown = await storage.get("cowrie:known_cids");
        if (Array.isArray(savedKnown)) {
          for (const rec of savedKnown) {
            if (rec && typeof rec.cidHex === "string" && rec.cidHex.length === 68) {
              m.recordCid(rec.cidHex, fromHex(rec.cidHex), Boolean(rec.tainted));
            }
          }
        }
        const savedTaint = await storage.get("cowrie:tainted_cids");
        if (Array.isArray(savedTaint)) {
          for (const cidHex of savedTaint) {
            if (typeof cidHex === "string" && cidHex.length === 68) {
              m.recordCid(cidHex, fromHex(cidHex), true);
            }
          }
        }
        const savedHandles = await storage.get("cowrie:virtual_order");
        if (Array.isArray(savedHandles)) {
          for (const handle of savedHandles) {
            const meta = await storage.get(`cowrie:v:${handle}`);
            if (!meta || typeof meta.cidHex !== "string") continue;
            let frameBytes = null;
            if (meta.frame instanceof Uint8Array) {
              frameBytes = meta.frame;
            } else if (Number.isInteger(meta.chunks) && meta.chunks > 0) {
              const parts = [];
              let total = 0;
              let ok = true;
              for (let c = 0; c < meta.chunks; c++) {
                const part = await storage.get(`cowrie:v:${handle}:c${c}`);
                if (!(part instanceof Uint8Array)) { ok = false; break; }
                parts.push(part);
                total += part.length;
              }
              if (ok) {
                frameBytes = new Uint8Array(total);
                let pos = 0;
                for (const p of parts) { frameBytes.set(p, pos); pos += p.length; }
              }
            }
            if (frameBytes) {
              m._storeVirtual(handle, {
                cid: fromHex(meta.cidHex),
                cidHex: meta.cidHex,
                frame: frameBytes,
                authority: Number(meta.authority),
              });
            }
          }
        }
      }
      this.mediator = m;
      this.hydrated = true;
      return m;
    }

    async _persistState() {
      const storage = this.state?.storage;
      if (!storage || !this.mediator) return;
      const m = this.mediator;
      await storage.put(
        "cowrie:known_cids",
        Array.from(m.knownCids.entries()).map(([cidHex, rec]) => ({ cidHex, tainted: rec.tainted }))
      );
      await storage.put("cowrie:tainted_cids", Array.from(m.taintedContextCids.keys()));
      await storage.put("cowrie:virtual_order", Array.from(m.virtualOrder));

      // Delete storage keys for handles evicted from the bounded ring buffer
      while (m.evictedHandles.length > 0 && typeof storage.delete === "function") {
        const old = m.evictedHandles.shift();
        const prev = await storage.get(`cowrie:v:${old}`);
        await storage.delete(`cowrie:v:${old}`);
        if (prev && Number.isInteger(prev.chunks)) {
          for (let c = 0; c < prev.chunks; c++) await storage.delete(`cowrie:v:${old}:c${c}`);
        }
      }

      for (const [handle, entry] of m.virtualStore.entries()) {
        if (entry.frame.length <= DO_CHUNK_BYTES) {
          await storage.put(`cowrie:v:${handle}`, {
            cidHex: entry.cidHex,
            frame: entry.frame,
            authority: entry.authority,
          });
        } else {
          const chunks = Math.ceil(entry.frame.length / DO_CHUNK_BYTES);
          for (let c = 0; c < chunks; c++) {
            await storage.put(
              `cowrie:v:${handle}:c${c}`,
              entry.frame.subarray(c * DO_CHUNK_BYTES, (c + 1) * DO_CHUNK_BYTES)
            );
          }
          await storage.put(`cowrie:v:${handle}`, {
            cidHex: entry.cidHex,
            chunks,
            authority: entry.authority,
          });
        }
      }
    }

    async fetch(request) {
      const cw = await getWasm();
      const m = await this._ensureHydrated(cw);
      const sessionId = request.headers.get("mcp-session-id") || "default";
      const response = await runMediatedMcpRequest(m, request, this.env, sessionId);
      await this._persistState();
      return response;
    }
  };
}

/**
 * Build a Cloudflare Worker handler bound to a `WebAssembly.Module` or raw WASM bytes.
 * When `env.COWRIE_MCP_SESSION` Durable Object binding is present, `/mcp` routes each
 * `mcp-session-id` to its dedicated `CowrieMcpSessionDO` instance.
 */
export function createCowrieWorker(wasmSource) {
  let cwPromise = null;
  const sessions = new Map();

  async function getWasm() {
    if (!cwPromise) cwPromise = instantiateCowrieWasm(wasmSource);
    return cwPromise;
  }

  function getMemoryMediator(cw, env, sessionId) {
    const key = sessionId || "default";
    let m = sessions.get(key);
    if (!m) {
      if (sessions.size > 256) {
        const oldest = sessions.keys().next().value;
        sessions.delete(oldest);
      }
      m = new CowrieMcpMediator(cw, buildMediatorOptions(env));
      sessions.set(key, m);
    }
    return m;
  }

  return {
    async fetch(request, env = {}, _ctx = undefined) {
      const url = new URL(request.url);
      const cw = await getWasm();

      if (request.method === "GET" && url.pathname === "/health") {
        return jsonResponse({
          ok: true,
          engine: "cowrie_core.wasm",
          version: "1.2.0",
          zero_imports: true,
          durable_objects: Boolean(env?.COWRIE_MCP_SESSION),
        });
      }

      if (request.method === "POST" && url.pathname === "/v1/verify") {
        try {
          const bytes = new Uint8Array(await request.arrayBuffer());
          if (bytes.length >= 4 && bytes[0] === 0x43 && bytes[1] === 0x4f && bytes[2] === 0x57 && bytes[3] === 0x52) {
            const info = cw.verifyFrame(bytes);
            return jsonResponse({
              ok: true,
              kind: "COWR",
              authority: info.authority,
              epistemic: info.epistemic,
              tainted: cw.isTainted(info.authority),
              cid: info.cidHex,
              handle: cw.alphaHandle(info.cid),
            });
          }
          const rep = cw.verifyShard(bytes);
          return jsonResponse({ ok: true, kind: "CWRF", merkle_root: rep.rootHex });
        } catch (err) {
          return jsonResponse({ ok: false, error: err.code || err.message }, 400);
        }
      }

      if (request.method === "POST" && url.pathname === "/v1/lens") {
        try {
          const body = await request.json();
          const tier = body.tier || "r2-compact";
          const auth = body.untrusted ? AUTH_UNTRUSTED_EXT : AUTH_VERIFIED_TOOL;
          const frame = cw.encodeJson(body.payload ?? {}, auth, EPI_OBSERVED);
          const info = cw.verifyFrame(frame);
          let rendered;
          if (tier === "r2-compact") rendered = cw.projectR2Compact(frame);
          else if (tier === "virtual-atlas") rendered = cw.projectVirtualAtlas(frame, Number(body.max_chars || 1800));
          else rendered = cw.projectLens(frame, tier, 0);
          return jsonResponse({
            ok: true,
            cid: info.cidHex,
            handle: cw.alphaHandle(info.cid),
            tainted: cw.isTainted(info.authority),
            rendered,
          });
        } catch (err) {
          return jsonResponse({ ok: false, error: err.code || err.message }, 400);
        }
      }

      if (request.method === "POST" && url.pathname === "/v1/declassify/verify") {
        try {
          const body = await request.json();
          const pkHex = body.pubkey || env?.COWRIE_OWNER_PUBKEY || "";
          const now = body.now_unix ?? Math.floor(Date.now() / 1000);
          const valid = cw.verifyDeclassification(
            fromHex(pkHex),
            fromHex(body.cid),
            body.action,
            BigInt(body.expiry),
            fromHex(body.sig),
            BigInt(now)
          );
          return jsonResponse({ ok: true, valid });
        } catch (err) {
          return jsonResponse({ ok: false, valid: false, error: err.message }, 400);
        }
      }

      if (request.method === "POST" && url.pathname === "/mcp") {
        const sessionId = request.headers.get("mcp-session-id") || "default";
        if (env?.COWRIE_MCP_SESSION) {
          const doId = env.COWRIE_MCP_SESSION.idFromName(sessionId);
          const stub = env.COWRIE_MCP_SESSION.get(doId);
          return stub.fetch(request);
        }
        const mediator = getMemoryMediator(cw, env, sessionId);
        return runMediatedMcpRequest(mediator, request, env, sessionId);
      }

      return jsonResponse({ error: "Not found" }, 404);
    },
  };
}
