import { useEffect, useState } from "react";
import SiteHeader from "./SiteHeader.jsx";
import { mcpUrl, mcpConfiguration } from "./mcpConnection.js";

function CopyExample({ title, text }) {
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    setStatus("");
    setError("");
  }, [text]);

  const copy = async () => {
    setStatus("");
    setError("");
    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error("Clipboard access is unavailable. Select and copy the text below instead.");
      }
      await navigator.clipboard.writeText(text);
      setStatus("Copied.");
    } catch (copyError) {
      setError(`Could not copy. ${copyError.message}`);
    }
  };

  return (
    <div className="min-w-0 mt-4">
      <div className="flex items-center justify-between gap-3 mb-2">
        <h3 className="text-[13px] font-semibold">{title}</h3>
        <button type="button" className="button button-secondary" aria-label={`Copy ${title}`} onClick={copy}>Copy</button>
      </div>
      <pre className="m-0 overflow-x-auto rounded-md border border-line bg-canvas p-3 text-[12px] leading-relaxed"><code>{text}</code></pre>
      {status && <p className="notice" role="status">{status}</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
    </div>
  );
}

export default function McpPage() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);
  const url = mcpUrl;

  return (
    <div className="app-shell min-h-screen">
      <SiteHeader currentPage="/connect-mcp" dark={dark} onToggleTheme={() => setDark((value) => !value)} />
      <main className="pt-7 pb-8">
        <section className="panel mb-5">
          <h2>Connect your AI assistant</h2>
          <p className="panel-copy">Let your AI assistant read Bible passages, compare translations and find connections using Bibliagraphia. MCP (Model Context Protocol) is the connection that gives your assistant access to these tools.</p>
          <p className="panel-copy">Choose your assistant below and follow its setup instructions. You can also read and explore directly in this app without setting up an AI assistant.</p>
          <CopyExample title="Server URL" text={url} />
          <p className="panel-copy mt-3">If your assistant asks for a connection type, choose <strong>Streamable HTTP</strong>. You do not need an API key (an access credential) to connect.</p>
        </section>

        <div className="grid gap-5 [grid-template-columns:repeat(auto-fit,minmax(min(100%,360px),1fr))]">
          <section className="panel">
            <h2>VS Code / GitHub Copilot</h2>
            <ol className="pl-5 text-[13px] leading-relaxed space-y-2 mt-3">
              <li>Create or open <code>.vscode/mcp.json</code> in your project.</li>
              <li>Add the configuration below. If you already have servers, add Bibliagraphia to the existing <code>servers</code> object.</li>
              <li>Use the file’s Start action, review the trust prompt, then enable Bibliagraphia’s tools in chat.</li>
            </ol>
            <CopyExample title="VS Code configuration" text={mcpConfiguration("vscode", url)} />
            <a className="inline-block mt-4 text-accent text-[12px]" href="https://code.visualstudio.com/docs/agent-customization/mcp-servers" target="_blank" rel="noreferrer">VS Code setup documentation</a>
          </section>
          <section className="panel">
            <h2>Claude Code</h2>
            <p className="panel-copy">Run this command in your terminal, then use <code>/mcp</code> in Claude Code to check the connection.</p>
            <CopyExample title="Claude Code command" text={`claude mcp add --transport http bibliagraphia ${url}`} />
            <p className="panel-copy mt-4">Alternatively, add this to your project’s <code>.mcp.json</code>. Merge it with any existing servers.</p>
            <CopyExample title="Claude Code configuration" text={mcpConfiguration("claude", url)} />
            <a className="inline-block mt-4 text-accent text-[12px]" href="https://code.claude.com/docs/en/mcp" target="_blank" rel="noreferrer">Claude Code setup documentation</a>
          </section>
          <section className="panel">
            <h2>Other MCP clients</h2>
            <p className="panel-copy">In your client’s MCP or connector settings, add a remote server named Bibliagraphia, paste the server URL above and choose Streamable HTTP.</p>
            <p className="panel-copy">Client configuration formats differ. Use your client’s instructions rather than pasting the VS Code format into another app.</p>
            <h3 className="text-[13px] font-semibold mt-4">If it does not connect</h3>
            <ul className="pl-5 text-[13px] leading-relaxed space-y-2 mt-2">
              <li>Test the connection in your AI assistant, not by opening the server URL in a browser.</li>
              <li>Use the full server URL, including <code>/mcp</code>, rather than the website homepage.</li>
              <li>After connecting, confirm the client lists Bibliagraphia’s tools.</li>
            </ul>
          </section>
          <section className="panel">
            <h2>Try asking</h2>
            <ul className="pl-5 text-[13px] leading-relaxed space-y-3 mt-3">
              <li>“Use Bibliagraphia to compare Genesis 1:1 across the available translations.”</li>
              <li>“Read Genesis chapter 12 and show the places mentioned.”</li>
              <li>“How is Genesis connected to Syria? Show the passages behind the links.”</li>
            </ul>
            <p className="panel-copy mt-4">Your assistant can search, read chapters, compare verses, find connections and look up places. AI explanations are available only when enabled by the people running Bibliagraphia.</p>
            <p className="panel-copy">Review AI-generated interpretations against the passages. Recorded links are not automatically theological conclusions.</p>
          </section>
        </div>
      </main>
    </div>
  );
}
