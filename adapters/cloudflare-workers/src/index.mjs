/**
 * Wrangler production entrypoint importing the compiled `cowrie_core.wasm` module directly
 * and exporting both the HTTP Worker handler and the `CowrieMcpSessionDO` Durable Object.
 *
 * Run `make -C ../.. wasm && cp /tmp/cowrie-cargo-target/wasm32-unknown-unknown/release/cowrie_core.wasm ./src/cowrie_core.wasm`
 * before `npx wrangler deploy`.
 */

import wasmModule from "./cowrie_core.wasm";
import { createCowrieSessionDO, createCowrieWorker } from "./worker.mjs";

export const CowrieMcpSessionDO = createCowrieSessionDO(wasmModule);
export default createCowrieWorker(wasmModule);
