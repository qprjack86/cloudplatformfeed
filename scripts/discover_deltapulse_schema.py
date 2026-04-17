#!/usr/bin/env python3
"""
DeltaPulse MCP Schema Discovery
Tests the DeltaPulse MCP endpoint to understand response formats for Roadmap and Message Center items.
"""

import json
from typing import Any, Dict, Optional

import requests

MCP_ENDPOINT = "https://deltapulse.app/mcp"
INIT_PARAMS = {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {"name": "discovery", "version": "1.0"},
}
INIT_PREVIEW_LIMIT = 500
JSON_PREVIEW_LIMIT = 2000
TOOL_CALL_PREVIEW_LIMIT = 3000
TOOLS_SUMMARY_LIMIT = 10
RESOURCES_SUMMARY_LIMIT = 15
TOOLS_DESCRIPTION_LIMIT = 80
RESOURCES_DESCRIPTION_LIMIT = 60
PROBE_LIMIT = 2
RESOURCE_PROBES = ("products", "digest/today")
TOOL_PROBES = ("search", "new")


def preview_json(value: Any, limit: int) -> str:
    """Render JSON with a length cap for readable console output."""
    rendered = json.dumps(value, indent=2)
    if len(rendered) <= limit:
        return rendered
    return rendered[:limit] + "..."


def print_catalog_preview(
    entries: list,
    *,
    label: str,
    identity_key: str,
    max_items: int,
    description_limit: int,
) -> None:
    """Print a compact summary list for tools/resources catalogs."""
    print(f"\nAvailable {label} ({len(entries)} total):")
    for entry in entries[:max_items]:
        identity = entry.get(identity_key, "N/A")
        description = str(entry.get("description", "N/A"))[:description_limit]
        print(f"  - {identity}: {description}")


def find_first_match(entries: list, key: str, fragment: str) -> Optional[Dict[str, Any]]:
    """Find the first entry whose key contains a fragment."""
    return next((entry for entry in entries if fragment in entry.get(key, "")), None)


def call_mcp_rpc(method: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Call DeltaPulse MCP via JSON-RPC 2.0."""
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params or {},
        "id": 1,
    }

    try:
        response = requests.post(MCP_ENDPOINT, json=payload, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Request failed for method '{method}': {exc}")
        return None

    try:
        envelope = response.json()
    except ValueError as exc:
        print(f"Invalid JSON response for method '{method}': {exc}")
        return None

    if not isinstance(envelope, dict):
        print(f"Unexpected response type for method '{method}': {type(envelope).__name__}")
        return None

    if "error" in envelope:
        print(f"MCP Error for method '{method}': {envelope['error']}")
        return None

    result = envelope.get("result")
    if not isinstance(result, dict):
        print(f"Unexpected result format for method '{method}': {type(result).__name__}")
        return None

    return result


def discover_available_methods() -> Optional[Dict[str, Any]]:
    """Discover available MCP methods and tools."""
    print("\n=== Discovering Available Methods ===")

    init_result = call_mcp_rpc("initialize", INIT_PARAMS)
    print(f"Initialize result: {preview_json(init_result, INIT_PREVIEW_LIMIT)}")

    tools_result = call_mcp_rpc("tools/list")
    if not tools_result:
        return None

    tools = tools_result.get("tools")
    if isinstance(tools, list):
        print_catalog_preview(
            tools,
            label="tools",
            identity_key="name",
            max_items=TOOLS_SUMMARY_LIMIT,
            description_limit=TOOLS_DESCRIPTION_LIMIT,
        )

    print(f"\nFull tools list: {preview_json(tools_result, JSON_PREVIEW_LIMIT)}")
    return tools_result


def discover_resources() -> Optional[Dict[str, Any]]:
    """Discover available MCP resources."""
    print("\n=== Discovering Available Resources ===")
    resources_result = call_mcp_rpc("resources/list")
    if not resources_result:
        return None

    resources = resources_result.get("resources")
    if isinstance(resources, list):
        print_catalog_preview(
            resources,
            label="resources",
            identity_key="uri",
            max_items=RESOURCES_SUMMARY_LIMIT,
            description_limit=RESOURCES_DESCRIPTION_LIMIT,
        )

    print(f"\nFull resources list: {preview_json(resources_result, JSON_PREVIEW_LIMIT)}")
    return resources_result


def discover_resource_content(uri: str) -> Optional[Dict[str, Any]]:
    """Read content of a specific resource."""
    print(f"\n=== Reading Resource: {uri} ===")
    result = call_mcp_rpc("resources/read", {"uri": uri})
    if not result:
        return None

    print(preview_json(result, JSON_PREVIEW_LIMIT))
    return result


def call_tool(tool_name: str, arguments: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Call a specific tool."""
    print(f"\n=== Calling Tool: {tool_name} ===")
    result = call_mcp_rpc("tools/call", {"name": tool_name, "arguments": arguments})
    if not result:
        return None

    print(preview_json(result, TOOL_CALL_PREVIEW_LIMIT))
    return result


def main() -> int:
    print("Starting DeltaPulse MCP Schema Discovery...")
    print(f"Endpoint: {MCP_ENDPOINT}\n")

    methods = discover_available_methods()
    resources = discover_resources()

    resource_entries = resources.get("resources") if isinstance(resources, dict) else []
    if isinstance(resource_entries, list):
        for probe in RESOURCE_PROBES:
            match = find_first_match(resource_entries, "uri", probe)
            if match and "uri" in match:
                discover_resource_content(match["uri"])

    tool_entries = methods.get("tools") if isinstance(methods, dict) else []
    if isinstance(tool_entries, list):
        for probe in TOOL_PROBES:
            match = find_first_match(tool_entries, "name", probe)
            if match and "name" in match:
                call_tool(match["name"], {"limit": PROBE_LIMIT})

    print("\n=== Discovery Complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

