#!/usr/bin/env python3
"""Debug DeltaPulse MCP tool calls."""

import json

MAX_PAYLOAD_PREVIEW_CHARS = 1000
MAX_ITEM_PREVIEW_CHARS = 1200


def preview_json(value, limit):
    rendered = json.dumps(value, indent=2, ensure_ascii=False)
    if len(rendered) <= limit:
        return rendered
    return rendered[:limit] + "... [truncated]"


def load_mcp_helpers():
    from fetch_m365_data import call_mcp_tool, create_http_session

    return call_mcp_tool, create_http_session


def debug_mcp_call(session, call_mcp_tool, tool_name, arguments=None):
    """Call MCP tool and print a concise debug summary."""
    args = arguments or {}

    print(f"\n=== Calling | {tool_name} ===")
    print(f"Payload: {preview_json(args, MAX_PAYLOAD_PREVIEW_CHARS)}")

    items = call_mcp_tool(session, tool_name, args)
    if not isinstance(items, list):
        print(f"Unexpected response type: {type(items).__name__}")
        return

    print(f"Items count: {len(items)}")
    if not items:
        print("No items returned (or call failed).")
        return

    first_item = items[0]
    if isinstance(first_item, dict):
        print(f"First item keys: {list(first_item.keys())}")
    print(f"First item: {preview_json(first_item, MAX_ITEM_PREVIEW_CHARS)}")


def main():
    call_mcp_tool, create_http_session = load_mcp_helpers()
    session = create_http_session()

    try:
        scenarios = [
            ("list_new_items", {"limit": 5}),
            ("list_new_items", {"limit": 5, "dateRange": "last_7_days"}),
            ("list_products", {}),
        ]
        for tool_name, arguments in scenarios:
            debug_mcp_call(session, call_mcp_tool, tool_name, arguments)
    finally:
        session.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
