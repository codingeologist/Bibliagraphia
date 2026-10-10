import assert from "node:assert/strict";
import test from "node:test";
import { mcpUrl, mcpConfiguration } from "./mcpConnection.js";

test("connection uses the public Bibliagraphia protocol endpoint", () => {
  assert.equal(mcpUrl, "https://bibliagraphia.com/mcp");
});

test("client configurations use the correct schemas and Streamable HTTP", () => {
  const url = mcpUrl;
  const server = { type: "http", url };
  assert.deepEqual(JSON.parse(mcpConfiguration("vscode", url)), { servers: { bibliagraphia: server } });
  assert.deepEqual(JSON.parse(mcpConfiguration("claude", url)), { mcpServers: { bibliagraphia: server } });
});
