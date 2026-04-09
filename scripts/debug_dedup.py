#!/usr/bin/env python3
"""Debug M365 deduplication"""

import json
import sys
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from fetch_m365_data import call_mcp_tool, create_http_session, build_article_from_m365_item, dedupe_m365_articles

session = create_http_session()

# Fetch a few items
print("Fetching 3 sample M365 items...")
new_items = call_mcp_tool(session, "list_new_items", {"limit": 3})
print(f"Got {len(new_items)} items\n")

if new_items:
    # Show raw item
    print("=== Raw Item ===")
    item = new_items[0]
    print(json.dumps(item, indent=2))
    
    # Convert to article
    print("\n=== After Conversion to Article ===")
    article = build_article_from_m365_item(item)
    print(json.dumps(article, indent=2))
    
    # Try deduping
    print("\n=== Deduplication Test ===")
    articles = [build_article_from_m365_item(i) for i in new_items]
    print(f"Before dedup: {len(articles)} articles")
    
    deduped = dedupe_m365_articles(articles)
    print(f"After dedup: {len(deduped)} articles")
    
    if len(deduped) < len(articles):
        print(f"Filtered out {len(articles) - len(deduped)} articles")
        # Check why
        now_datetime = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
        cutoff = now_datetime - __import__('datetime').timedelta(days=30)
        for i, a in enumerate(articles):
            pub = a.get("published", "")
            print(f"  Item {i}: published={pub[:19]}...")
