export declare const AUTH_SYS_POLICY: 0x01;
export declare const AUTH_VERIFIED_TOOL: 0x02;
export declare const AUTH_UNTRUSTED_EXT: 0x04;
export declare const AUTH_MODEL_SYNTH: 0x08;
export declare const BIT_TAINTED: 0x80;

export declare const EPI_OBSERVED: 0x01;
export declare const EPI_DERIVED: 0x02;
export declare const EPI_ESTIMATED: 0x03;

export type CowrieAction = "read_only" | "shell_exec" | "cred_read" | "fs_write" | "net_exfil";
export type CowrieLensTier = "r0" | "r1" | "r2" | "r3";

export declare const ACTION_CODES: Readonly<Record<CowrieAction, number>>;

export declare class CowrieWasmError extends Error {
  readonly code: string;
  readonly rc: number;
  constructor(code: string, rc: number);
}

export declare function toHex(u8: Uint8Array): string;
export declare function fromHex(hex: string): Uint8Array;

export interface VerifiedFrame {
  authority: number;
  epistemic: number;
  cid: Uint8Array;
  cidHex: string;
}

export interface VerifiedShard {
  root: Uint8Array;
  rootHex: string;
}

export interface VerifiedStatePage {
  ctxTokens: number;
}

export interface CowrieWasmClient {
  readonly exports: WebAssembly.Exports;
  sha256Hex(bytes: Uint8Array | string): string;
  encodeJson(obj: unknown, authority?: number, epistemic?: number): Uint8Array;
  verifyFrame(frameU8: Uint8Array): VerifiedFrame;
  decodeToJson(frameU8: Uint8Array): string;
  projectLens(frameU8: Uint8Array, tier?: CowrieLensTier, tensorCount?: number): string;
  projectR2Compact(frameU8: Uint8Array): string;
  extractField(frameU8: Uint8Array, fieldPath: string): string;
  packSingleShard(frameU8: Uint8Array): Uint8Array;
  verifyShard(shardU8: Uint8Array): VerifiedShard;
  resolvedFrameCid(shardU8: Uint8Array, frameIdx?: number): { cid: Uint8Array; cidHex: string };
  verifyStatePageBytes(
    spU8: Uint8Array,
    hmacKey?: Uint8Array | string | null,
    verifyHash?: boolean
  ): VerifiedStatePage;
  propagateAuthority(baseAuth: number, parentAuths?: Iterable<number>): number;
  isTainted(auth: number): boolean;
  ed25519Pubkey(seed32: Uint8Array): Uint8Array;
  signDeclassification(
    seed32: Uint8Array,
    targetCid34: Uint8Array,
    action: CowrieAction,
    expiryUnixSecs: number | bigint
  ): Uint8Array;
  verifyDeclassification(
    pubkey32: Uint8Array,
    targetCid34: Uint8Array,
    action: CowrieAction,
    expiryUnixSecs: number | bigint,
    sig64: Uint8Array,
    nowUnixSecs: number | bigint
  ): boolean;
}

export declare function createCowrieWasm(
  wasmBytesOrResponse: Uint8Array | ArrayBuffer | Response | PromiseLike<Response>
): Promise<CowrieWasmClient>;
