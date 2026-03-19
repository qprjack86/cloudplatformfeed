#!/usr/bin/env python3
"""
DeltaPulse MCP Schema Discovery
Tests the DeltaPulse MCP endpoint to understand response formats for Roadmap and Message Center items.
"""

import requests
import json
from typing import Any, Dict

MCP_ENDPOINT = "https://deltapulse.app/mcp"

def call_mcp_rpc(method: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
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
        result = response.json()
        if "error" in result:
            print(f"MCP Error: {result['error']}")
            return None
        return result.get("result", {})
    except Exception as e:
        print(f"Request failed: {e}")
        return None


def discover_available_methods():
    """Discover available MCP methods and tools."""
    print("\n=== Discovering Available Methods ===")
    
    # Initialize
    init_result = call_mcp_rpc("initialize", {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "discovery", "version": "1.0"}
    })
    print(f"Initialize result: {json.dumps(init_result, indent=2)[:500]}")
    
    # List tools
    tools_result = call_mcp_rpc("tools/list")
    if tools_result:
        if "tools" in tools_result:
            print(f"\nAvailable tools ({len(tools_result['tools'])} total):")
            for tool in tools_result["tools"][:10]:  # Show first 10
                print(f"  - {tool.get('name')}: {tool.get('description', 'N/A')[:80]}")
        print(f"\nFull tools list: {json.dumps(tools_result, indent=2)[:2000]}")
        return tools_result
    return None


def discover_resources():
    """Discover available MCP resources."""
    print("\n=== Discovering Available Resources ===")
    resources_result = call_mcp_rpc("resources/list")
    if resources_result:
        if "resources" in resources_result:
            print(f"\nAvailable resources ({len(resources_result['resources'])} total):")
            for res in resources_result["resources"][:15]:  # Show first 15
                print(f"  - {res.get('uri')}: {res.get('description', 'N/A')[:60]}")
        print(f"\nFull resources list: {json.dumps(resources_result, indent=2)[:2000]}")
        return resources_result
    return None


def discover_resource_content(uri: str):
    """Read content of a specific resource."""
    print(f"\n=== Reading Resource: {uri} ===")
    result = call_mcp_rpc("resources/read", {"uri": uri})
    if result:
        print(json.dumps(result, indent=2)[:2000])
        return result
    return None


def call_tool(tool_name: str, arguments: Dict[str, Any]):
    """Call a specific tool."""
    print(f"\n=== Calling Tool: {tool_name} ===")
    result = call_mcp_rpc("tools/call", {
        "name": tool_name,
        "arguments": arguments
    })
    if result:
        print(json.dumps(result, indent=2)[:3000])
        return result
    return None


if __name__ == "__main__":
    print("Starting DeltaPulse MCP Schema Discovery...")
    print(f"Endpoint: {MCP_ENDPOINT}\n")
    
    # Discover available methods, tools, and resources
    methods = discover_available_methods()
    resources = discover_resources()
    
    # Try reading some key resources
    if resources and "resources" in resources:
        res_list = resources["resources"]
        if res_list:
            # Try to read products resource
            products_resource = next((r for r in res_list if "products" in r.get("uri", "")), None)
            if products_resource:
                discover_resource_content(products_resource["uri"])
            
            # Try to read today's digest
            digest_resource = next((r for r in res_list if "digest/today" in r.get("uri", "")), None)
            if digest_resource:
                discover_resource_content(digest_resource["uri"])
    
    # Try calling some tools if available
    if methods and "tools" in methods:
        tools = methods["tools"]
        search_tool = next((t for t in tools if "search" in t.get("name", "")), None)
        if search_tool:
            call_tool(search_tool["name"], {"limit": 2})
        
        list_new_tool = next((t for t in tools if "new" in t.get("name", "")), None)
        if list_new_tool:
            call_tool(list_new_tool["name"], {"limit": 2})
    
    print("\n=== Discovery Complete ===")

