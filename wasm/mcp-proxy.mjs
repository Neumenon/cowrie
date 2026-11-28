/**
 * Node.js stdio runner wrapping `CowrieMcpMediator` (`mcp-mediator.mjs`).
 * Re-exports all mediator helpers so existing Node imports keep working unchanged.
 */

import { spawn } from "node:child_process";
import readline from "node:readline";

import {
  CowrieMcpMediator,
  DEFAULT_VIRTUALIZE_BYTES,
  MAX_VIRTUAL_ENTRIES,
  classifyMcpTool,
  isUntrustedSourceTool,
  normalizeRpcId,
  parseStrictJson,
} from "./mcp-mediator.mjs";

export {
  CowrieMcpMediator,
  DEFAULT_VIRTUALIZE_BYTES,
  MAX_VIRTUAL_ENTRIES,
  classifyMcpTool,
  isUntrustedSourceTool,
  normalizeRpcId,
  parseStrictJson,
};

export function runStdioMcpProxy(cw, options, cmdArgs) {
  if (!cmdArgs || cmdArgs.length === 0) {
    process.stderr.write("mcp-proxy requires `-- <mcp_cmd> [args...]`\n");
    process.exit(2);
  }
  const mediator = new CowrieMcpMediator(cw, options);
  const child = spawn(cmdArgs[0], cmdArgs.slice(1), {
    stdio: ["pipe", "pipe", "inherit"],
  });
  child.on("error", (err) => {
    process.stderr.write(`Failed to spawn MCP server '${cmdArgs[0]}': ${err.message}\n`);
    process.exit(1);
  });

  const childRl = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  childRl.on("line", (line) => {
    if (!line.trim()) return;
    const out = mediator.handleServerLine(line);
    process.stdout.write(`${out}\n`);
  });

  const stdinRl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  stdinRl.on("line", (line) => {
    if (!line.trim()) return;
    const res = mediator.handleClientLine(line);
    if (res.localResponse) {
      process.stdout.write(`${res.localResponse}\n`);
    } else if (res.forwardLine) {
      child.stdin.write(`${res.forwardLine}\n`);
    }
  });
  stdinRl.on("close", () => {
    child.stdin.end();
  });

  child.on("exit", (code) => {
    process.exit(code ?? 0);
  });
}
