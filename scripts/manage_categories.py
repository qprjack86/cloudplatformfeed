#!/usr/bin/env python3
"""
Manage category mappings for M365 and Azure products via CLI.

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


def _resolve_platform_mappings(mappings, platform):
    """Return mappings for a platform or print a consistent error."""
    platform_mappings = mappings.get(platform)
    if platform_mappings is None:
        print(f"❌ Platform '{platform}' not found in config")
        return None
    return platform_mappings


def _resolve_category_keywords(platform_mappings, platform, category, show_available=False):
    """Return keyword list for a category or print a consistent error."""
    keywords = platform_mappings.get(category)
    if keywords is None:
        print(f"❌ Category '{category}' not found in {platform}")
        if show_available:
            print(f"   Available categories: {', '.join(platform_mappings.keys())}")
        return None
    return keywords


def _mutate_keyword(platform, category, keyword, operation):
    """Apply add/remove keyword operation and persist when changed."""
    config = load_config()
    mappings = config.get("categoryMappings", {})

    platform_mappings = _resolve_platform_mappings(mappings, platform)
    if platform_mappings is None:
        return

    category_keywords = _resolve_category_keywords(
        platform_mappings,
        platform,
        category,
        show_available=(operation == "add"),
    )
    if category_keywords is None:
        return

    if operation == "add":
        if keyword in category_keywords:
            print(f"⚠️  '{keyword}' already exists in {platform}/{category}")
            return
        category_keywords.append(keyword)
        action = "Added"
    elif operation == "remove":
        if keyword not in category_keywords:
            print(f"⚠️  '{keyword}' not found in {platform}/{category}")
            return
        category_keywords.remove(keyword)
        action = "Removed"
    else:
        print(f"❌ Unsupported operation: {operation}")
        return

    config["categoryMappings"] = mappings
    save_config(config)
    print(f"✅ {action} '{keyword}' {'to' if operation == 'add' else 'from'} {platform}/{category}")


def list_categories(platform="m365"):
    """List all categories and their keywords."""
    config = load_config()
    mappings = config.get("categoryMappings", {})
    platform_mappings = _resolve_platform_mappings(mappings, platform)
    if platform_mappings is None:
        return
    
    print(f"\n{'='*60}")
    print(f"  {platform.upper()} Category Mappings")
    print(f"{'='*60}\n")
    
    for category, keywords in sorted(platform_mappings.items()):
        keyword_count = len(keywords)
        print(f"  {category}: ({keyword_count} keywords)")
        for kw in keywords[:3]:
            print(f"    • {kw}")
        if len(keywords) > 3:
            print(f"    ... and {len(keywords) - 3} more")
        print()


def add_keyword(platform, category, keyword):
    """Add a keyword to a category."""
    _mutate_keyword(platform, category, keyword, operation="add")


def remove_keyword(platform, category, keyword):
    """Remove a keyword from a category."""
    _mutate_keyword(platform, category, keyword, operation="remove")


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
    help_aliases = {"help", "--help", "-h"}
    keyword_commands = {
        "add": add_keyword,
        "remove": remove_keyword,
    }
    
    if cmd in help_aliases:
        show_help()
        return 0
    if cmd == "list":
        platform = sys.argv[2] if len(sys.argv) > 2 else "m365"
        list_categories(platform)
        return 0
    if cmd in keyword_commands:
        if len(sys.argv) < 5:
            print(f"❌ Usage: {cmd} <platform> <category> <keyword>")
            return 1
        keyword_commands[cmd](sys.argv[2], sys.argv[3], sys.argv[4])
        return 0

    print(f"❌ Unknown command: {cmd}")
    show_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
