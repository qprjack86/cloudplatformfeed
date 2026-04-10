#!/usr/bin/env python3
"""
Manage category mappings for M365 and Azure products interactively.

(Improvement #4: Configurable Category Mappings)
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "site.json"


def load_config():
    """Load configuration file."""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_config(config):
    """Save configuration file."""
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"✅ Config saved to {CONFIG_PATH}")


def list_categories(platform="m365"):
    """List all categories and their keywords."""
    config = load_config()
    mappings = config.get("categoryMappings", {}).get(platform, {})
    
    print(f"\n{'='*60}")
    print(f"  {platform.upper()} Category Mappings")
    print(f"{'='*60}\n")
    
    for category, keywords in sorted(mappings.items()):
        keyword_count = len(keywords)
        print(f"  {category}: ({keyword_count} keywords)")
        for i, kw in enumerate(keywords[:3]):
            print(f"    • {kw}")
        if len(keywords) > 3:
            print(f"    ... and {len(keywords) - 3} more")
        print()


def add_keyword(platform, category, keyword):
    """Add a keyword to a category."""
    config = load_config()
    mappings = config.get("categoryMappings", {})
    
    if platform not in mappings:
        print(f"❌ Platform '{platform}' not found in config")
        return
    
    if category not in mappings[platform]:
        print(f"❌ Category '{category}' not found in {platform}")
        print(f"   Available categories: {', '.join(mappings[platform].keys())}")
        return
    
    if keyword not in mappings[platform][category]:
        mappings[platform][category].append(keyword)
        config["categoryMappings"] = mappings
        save_config(config)
        print(f"✅ Added '{keyword}' to {platform}/{category}")
    else:
        print(f"⚠️  '{keyword}' already exists in {platform}/{category}")


def remove_keyword(platform, category, keyword):
    """Remove a keyword from a category."""
    config = load_config()
    mappings = config.get("categoryMappings", {})
    
    if platform not in mappings:
        print(f"❌ Platform '{platform}' not found in config")
        return
    
    if category not in mappings[platform]:
        print(f"❌ Category '{category}' not found in {platform}")
        return
    
    if keyword in mappings[platform][category]:
        mappings[platform][category].remove(keyword)
        config["categoryMappings"] = mappings
        save_config(config)
        print(f"✅ Removed '{keyword}' from {platform}/{category}")
    else:
        print(f"⚠️  '{keyword}' not found in {platform}/{category}")


def show_help():
    """Show usage information."""
    print("""
Usage: manage_categories.py <command> [options]

Commands:
  list [platform]          List all categories and keywords (default: m365)
                          Platforms: m365, azure
  
  add <platform> <category> <keyword>
                          Add a keyword to a category
                          Example: add m365 Collaboration Teams
  
  remove <platform> <category> <keyword>
                          Remove a keyword from a category
                          Example: remove m365 Collaboration Teams
  
  help                    Show this help message

Examples:
  python manage_categories.py list
  python manage_categories.py list azure
  python manage_categories.py add m365 "AI & Automation" "Copilot Pro"
  python manage_categories.py remove azure Compute container
""")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        show_help()
        return 0
    
    cmd = sys.argv[1].lower()
    
    if cmd == "help" or cmd == "--help" or cmd == "-h":
        show_help()
        return 0
    elif cmd == "list":
        platform = sys.argv[2] if len(sys.argv) > 2 else "m365"
        list_categories(platform)
        return 0
    elif cmd == "add":
        if len(sys.argv) < 5:
            print("❌ Usage: add <platform> <category> <keyword>")
            return 1
        add_keyword(sys.argv[2], sys.argv[3], sys.argv[4])
        return 0
    elif cmd == "remove":
        if len(sys.argv) < 5:
            print("❌ Usage: remove <platform> <category> <keyword>")
            return 1
        remove_keyword(sys.argv[2], sys.argv[3], sys.argv[4])
        return 0
    else:
        print(f"❌ Unknown command: {cmd}")
        show_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
