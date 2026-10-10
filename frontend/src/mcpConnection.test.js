import assert from "node:assert/strict";
import test from "node:test";
import { hostedMcpUrl, isLocalHost, localMcpUrl, mcpConfiguration } from "./mcpConnection.js";

test("local connection uses the API port and preserves the local hostname", () => {
  assert.equal(localMcpUrl("127.0.0.1"), "http://127.0.0.1:8000/mcp");
  assert.equal(localMcpUrl("localhost"), "http://localhost:8000/mcp");
  assert.equal(localMcpUrl("::1"), "http://[::1]:8000/mcp");
  assert.equal(isLocalHost("127.0.0.1"), true);
  assert.equal(isLocalHost("bibliographia.com"), false);
});

test("client configurations use the correct schemas and Streamable HTTP", () => {
  const server = { type: "http", url: hostedMcpUrl };
  assert.deepEqual(JSON.parse(mcpConfiguration("vscode", hostedMcpUrl)), { servers: { bibliagraphia: server } });
  assert.deepEqual(JSON.parse(mcpConfiguration("claude", hostedMcpUrl)), { mcpServers: { bibliagraphia: server } });
});
