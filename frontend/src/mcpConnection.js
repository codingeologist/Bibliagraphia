export function localMcpUrl(hostname) {
  return new URL("/mcp", `http://${hostname.includes(":") ? `[${hostname}]` : hostname}:8000`).href;
}

export function isLocalHost(hostname) {
  return ["localhost", "127.0.0.1", "::1", "[::1]"].includes(hostname);
}

export function mcpConfiguration(client, url) {
  const server = { type: "http", url };
  return JSON.stringify(client === "vscode"
    ? { servers: { bibliagraphia: server } }
    : { mcpServers: { bibliagraphia: server } }, null, 2);
}
