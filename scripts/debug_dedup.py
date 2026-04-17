#!/usr/bin/env python3
"""Debug M365 deduplication."""

import json
import sys
from pathlib import Path

def load_m365_helpers():
    scripts_dir = Path(__file__).resolve().parent.parent / "scripts"
    scripts_path = str(scripts_dir)
    inserted = False

    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)
        inserted = True

    try:
        from fetch_m365_data import (
            build_article_from_m365_item,
            call_mcp_tool,
            create_http_session,
            dedupe_m365_articles,
        )
    finally:
        if inserted and scripts_path in sys.path:
            sys.path.remove(scripts_path)

    return (
        call_mcp_tool,
        create_http_session,
        build_article_from_m365_item,
        dedupe_m365_articles,
    )


def main():
    (
        call_mcp_tool,
        create_http_session,
        build_article_from_m365_item,
        dedupe_m365_articles,
    ) = load_m365_helpers()

    session = create_http_session()

    print("Fetching 3 sample M365 items...")
    new_items = call_mcp_tool(session, "list_new_items", {"limit": 3})
    print(f"Got {len(new_items)} items\n")

    if not new_items:
        print("No items returned; nothing to dedupe.")
        return 0

    print("=== Raw Item ===")
    print(json.dumps(new_items[0], indent=2))

    articles = [build_article_from_m365_item(item) for item in new_items]

    print("\n=== After Conversion to Article ===")
    print(json.dumps(articles[0], indent=2))

    print("\n=== Deduplication Test ===")
    print(f"Before dedup: {len(articles)} articles")

    deduped = dedupe_m365_articles(articles)
    print(f"After dedup: {len(deduped)} articles")

    if len(deduped) < len(articles):
        print(f"Filtered out {len(articles) - len(deduped)} articles")
        for index, article in enumerate(articles):
            published = article.get("published", "")
            print(f"  Item {index}: published={published[:19]}...")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
