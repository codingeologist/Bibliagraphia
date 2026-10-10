#!/usr/bin/env python3
"""MCP client for Bibliagraphia — automates the streamable-HTTP handshake.

FastMCP's Client performs the whole session dance for you: it sends
`initialize`, captures the `mcp-session-id` response header, delivers
`notifications/initialized`, and echoes the session id on every later
request. No curl + awk + header juggling needed.

Usage (server must be running, e.g. `uvicorn app.api:app`):

    python scripts/mcp_client.py tools
    python scripts/mcp_client.py call search_nodes \\
        --args '{"q": "JOH", "label": "book"}'
    python scripts/mcp_client.py call get_verse \\
        --args '{"book_code": "JOH", "chapter": 3, "verse_number": 16}'
    python scripts/mcp_client.py interactive

The endpoint defaults to $BIBLIAGRAPHIA_MCP_URL, then
http://localhost:8000/mcp; override per run with --url.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Optional

from fastmcp import Client

DEFAULT_URL = "http://localhost:8000/mcp"


def _client(url: str) -> Client:
    """Build a FastMCP client for the streamable-HTTP endpoint."""
    return Client(url)


async def list_tools(url: str) -> None:
    """Print the tools the server exposes (name, description, args)."""
    async with _client(url) as client:
        tools = await client.list_tools()
        for tool in tools:
            print(f"## {tool.name}")
            desc = (tool.description or "").strip()
            if desc:
                print(f"   {desc.splitlines()[0]}")
            schema = getattr(tool, "input_schema", None) or getattr(
                tool, "inputSchema", {}
            )
            props = schema.get("properties", {})
            required = schema.get("required", [])
            if props:
                arg_summary = ", ".join(
                    f"{name}{'*' if name in required else ''}"
                    for name in props
                )
                print(f"   args: {arg_summary}  (* = required)")
            print()


async def call_tool(url: str, name: str, args: dict[str, Any]) -> int:
    """Call one tool and print its result as JSON. Returns exit code."""
    async with _client(url) as client:
        result = await client.call_tool(name, args)
        # fastmcp >= 2.x: CallToolResult with .content / .structured_content
        if getattr(result, "structured_content", None):
            print(json.dumps(result.structured_content, indent=2, default=str))
            return 0
        content = getattr(result, "content", result)
        if isinstance(content, list):
            for block in content:
                text = getattr(block, "text", None)
                if text is None:
                    print(json.dumps(block, indent=2, default=str))
                    continue
                try:  # pretty-print JSON payloads, plain text otherwise
                    print(json.dumps(json.loads(text), indent=2, default=str))
                except (json.JSONDecodeError, TypeError):
                    print(text)
        else:
            print(content)
        return 0


async def interactive(url: str) -> int:
    """Tiny REPL: `tool {json args}` per line, empty line or q to quit."""
    print(f"Connected to {url}. Commands: <tool> '<json args>' | q to quit")
    async with _client(url) as client:
        while True:
            try:
                line = input("mcp> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return 0
            if not line or line.lower() in {"q", "quit", "exit"}:
                return 0
            name, _, arg_str = line.partition(" ")
            args: dict[str, Any] = {}
            if arg_str.strip():
                try:
                    parsed = json.loads(arg_str)
                    if not isinstance(parsed, dict):
                        print("args must be a JSON object")
                        continue
                    args = parsed
                except json.JSONDecodeError as exc:
                    print(f"invalid JSON: {exc}")
                    continue
            try:
                result = await client.call_tool(name, args)
                content = getattr(result, "structured_content", None)
                if content is None:
                    blocks = getattr(result, "content", [])
                    texts = [
                        getattr(b, "text", json.dumps(b, default=str))
                        for b in (blocks if isinstance(blocks, list) else [blocks])
                    ]
                    content = "\n".join(texts)
                print(content)
            except Exception as exc:  # tool errors should not kill the REPL
                print(f"error: {exc}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bibliagraphia MCP client (handshake handled for you)."
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("BIBLIAGRAPHIA_MCP_URL", DEFAULT_URL),
        help=f"MCP endpoint (default $BIBLIAGRAPHIA_MCP_URL or {DEFAULT_URL})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("tools", help="list the tools the server exposes")
    call_p = sub.add_parser("call", help="call one tool")
    call_p.add_argument("name", help="tool name, e.g. get_verse")
    call_p.add_argument(
        "--args",
        default="{}",
        help='tool arguments as JSON, e.g. \'{"book_code": "JOH", "chapter": 3}\'',
    )
    sub.add_parser("interactive", help="ad-hoc tool calls (REPL)")

    ns = parser.parse_args()

    try:
        if ns.command == "tools":
            asyncio.run(list_tools(ns.url))
            return 0
        if ns.command == "call":
            args = json.loads(ns.args)
            if not isinstance(args, dict):
                parser.error("--args must be a JSON object")
            return asyncio.run(call_tool(ns.url, ns.name, args))
        if ns.command == "interactive":
            return asyncio.run(interactive(ns.url))
    except json.JSONDecodeError as exc:
        parser.error(f"invalid JSON: {exc}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
