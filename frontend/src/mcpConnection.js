export const mcpUrl = "https://bibliagraphia.com/mcp";

export function mcpConfiguration(client, url) {
  const server = { type: "http", url };
  return JSON.stringify(client === "vscode"
    ? { servers: { bibliagraphia: server } }
    : { mcpServers: { bibliagraphia: server } }, null, 2);
}
