# Author: Satish Chauhan

"""Start MCP (streamable HTTP) in a background thread when the GUI opens."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional, Tuple

_started = False
_lock = threading.Lock()


def _endpoint_settings_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", os.path.expanduser("~"))) / "AzureMigrationTool"
    else:
        base = Path(os.path.expanduser("~")) / ".azure_migration_tool"
    base.mkdir(parents=True, exist_ok=True)
    return base / "mcp_endpoint.json"


def write_mcp_endpoint_file(url: str, port: int) -> None:
    path = _endpoint_settings_path()
    path.write_text(
        json.dumps({"url": url, "port": port, "transport": "streamable-http"}, indent=2),
        encoding="utf-8",
    )


def clear_mcp_endpoint_file() -> None:
    path = _endpoint_settings_path()
    try:
        if path.is_file():
            path.unlink()
    except OSError:
        pass


def start_mcp_background(
    *,
    host: str = "127.0.0.1",
    port: Optional[int] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Start MCP HTTP server once per process.

    Returns (url, error_message). url is None if not started.
    Set AZURE_MIGRATION_TOOL_DISABLE_MCP=1 to skip.
    """
    global _started
    if os.environ.get("AZURE_MIGRATION_TOOL_DISABLE_MCP", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return None, "disabled"

    with _lock:
        if _started:
            try:
                from azure_migration_tool.mcp_server import DEFAULT_MCP_HTTP_PORT, mcp_http_url
            except ImportError:
                from mcp_server import DEFAULT_MCP_HTTP_PORT, mcp_http_url  # type: ignore[no-redef]
            p = port or DEFAULT_MCP_HTTP_PORT
            return mcp_http_url(host, p), None
        _started = True

    try:
        from azure_migration_tool.mcp_server import (
            DEFAULT_MCP_HTTP_PORT,
            create_mcp_server,
            mcp_http_url,
        )
    except ImportError:
        try:
            from mcp_server import DEFAULT_MCP_HTTP_PORT, create_mcp_server, mcp_http_url
        except ImportError:
            _started = False
            return None, "mcp package not installed"

    server = create_mcp_server()
    if server is None:
        _started = False
        return None, "mcp package not installed"

    use_port = int(port or DEFAULT_MCP_HTTP_PORT)
    url = mcp_http_url(host, use_port)

    def _run() -> None:
        try:
            server.run(transport="streamable-http", host=host, port=use_port)
        except Exception:
            pass

    threading.Thread(target=_run, name="amt-mcp-http", daemon=True).start()
    write_mcp_endpoint_file(url, use_port)
    return url, None
