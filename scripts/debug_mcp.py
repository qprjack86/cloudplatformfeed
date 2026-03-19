#!/usr/bin/env python3
"""Debug DeltaPulse MCP tool calls"""

import requests
import json

MCP_ENDPOINT = "https://deltapulse.app/mcp"

def debug_mcp_call(tool_name, arguments=None):
    """Call MCP tool and show raw response."""
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments or {}
        },
        "id": 1,
    }
    
    print(f"\n=== Calling | {tool_name} ===")
    print(f"Payload: {json.dumps(arguments or {}, indent=2)}")
    
    response = requests.post(MCP_ENDPOINT, json=payload, timeout=10)
    result = response.json()
    
    print(f"Raw response: {json.dumps(result, indent=2)[:2000]}")
    
    if "result" in result:
        content = result["result"].get("content", [])
        if content:
            text = content[0].get("text", "")
            print(f"Content text (first 1000 chars): {text[:1000]}")
            # Try to parse as JSON
            try:
                parsed = json.loads(text)
                print(f"Parsed JSON keys: {list(parsed.keys())}")
                if "items" in parsed:
                    print(f"Items count: {len(parsed['items'])}")
                    if parsed["items"]:
                        print(f"First item: {json.dumps(parsed['items'][0], indent=2)[:500]}")
            except json.JSONDecodeError as e:
                print(f"Failed to parse as JSON: {e}")

# Test tool calls
debug_mcp_call("list_new_items", {"limit": 5})
debug_mcp_call("list_new_items", {"limit": 5, "dateRange": "last_7_days"})
debug_mcp_call("list_products", {})
