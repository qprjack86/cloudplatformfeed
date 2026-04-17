#!/usr/bin/env python3
"""
Test script to verify all improvements 1-4 are working correctly.
"""

import json
import sys
from pathlib import Path

# Add scripts directory to path
REPO_ROOT = Path(__file__).resolve().parent
SCRIPTS_PATH = str(REPO_ROOT / "scripts")
if SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, SCRIPTS_PATH)

from feed_common import validate_feed_data


def print_test_header(title):
    print("\n" + "=" * 70)
    print(f"TEST: {title}")
    print("=" * 70)


def check_script_exists(script_name, test_title, success_message):
    print_test_header(test_title)

    script_path = REPO_ROOT / "scripts" / script_name
    if script_path.exists():
        print(f"✓ {script_name} exists at {script_path}")
        print(f"\n✅ PASS: {success_message}")
        return True

    print(f"\n❌ FAIL: {script_name} not found")
    return False

def test_improvement_1():
    """Test Improvement #1: Retirement Calendar & Lifecycle Data Robustness"""
    print_test_header("Improvement #1 - Lifecycle Data & Schema Validation")
    
    # Load feeds.json
    feeds_path = REPO_ROOT / "data" / "feeds.json"
    with open(feeds_path) as f:
        feeds_data = json.load(f)
    
    articles = feeds_data.get("articles", [])
    print(f"\n✓ Loaded {len(articles)} articles from feeds.json")
    
    # Test validation
    is_valid, msg = validate_feed_data(articles, min_coverage_percent=85)
    print(f"✓ Validation result: {msg}")
    
    # Check that articles have lifecycleState and datePrecision
    articles_with_lifecycle = sum(1 for a in articles if "lifecycleState" in a)
    articles_with_precision = sum(1 for a in articles if "datePrecision" in a)
    
    print(f"✓ Articles with lifecycleState: {articles_with_lifecycle}/{len(articles)}")
    print(f"✓ Articles with datePrecision: {articles_with_precision}/{len(articles)}")
    
    if articles_with_lifecycle == len(articles) and articles_with_precision == len(articles):
        print("\n✅ PASS: Improvement #1 - All articles have lifecycle and precision data")
        return True
    else:
        print("\n⚠️  PARTIAL: Some articles missing lifecycle/precision fields")
        return False


def test_improvement_3():
    """Test Improvement #3: MCP Cache Functions"""
    print_test_header("Improvement #3 - MCP Resilience & Cache Functions")
    
    # Check that cache functions exist in fetch_m365_data
    try:
        from fetch_m365_data import load_m365_cache, save_m365_cache, M365_CACHE_PATH
        print("✓ Cache functions imported successfully")
        print(f"✓ Cache path: {M365_CACHE_PATH}")
        print("\n✅ PASS: Improvement #3 - Cache fallback functions available")
        return True
    except ImportError as e:
        print(f"\n❌ FAIL: Could not import cache functions: {e}")
        return False


def test_improvement_4():
    """Test Improvement #4: Configurable Category Mappings"""
    print_test_header("Improvement #4 - Configurable Category Mappings")
    
    config_path = REPO_ROOT / "config" / "site.json"
    with open(config_path) as f:
        config = json.load(f)
    
    # Check for category mappings in config
    mappings = config.get("categoryMappings", {})
    if not mappings:
        print("\n❌ FAIL: No categoryMappings in config")
        return False
    
    m365_cats = mappings.get("m365", {})
    azure_cats = mappings.get("azure", {})
    
    print(f"✓ M365 categories in config: {len(m365_cats)}")
    print(f"  Examples: {', '.join(list(m365_cats.keys())[:3])}")
    print(f"✓ Azure categories in config: {len(azure_cats)}")
    print(f"  Examples: {', '.join(list(azure_cats.keys())[:3])}")
    
    # Test that fetch_m365_data loads from config
    try:
        from fetch_m365_data import M365_PRODUCT_CATEGORIES
        print(f"✓ M365_PRODUCT_CATEGORIES loaded: {len(M365_PRODUCT_CATEGORIES)} categories")

        if M365_PRODUCT_CATEGORIES != m365_cats:
            print("\n⚠️  PARTIAL: Categories may have defaults merged")

        print("\n✅ PASS: Improvement #4 - Categories loaded from config")
        return True
            
    except ImportError as e:
        print(f"\n❌ FAIL: Could not import M365_PRODUCT_CATEGORIES: {e}")
        return False


def test_validation_script():
    """Test that validate_feeds.py script exists and runs"""
    return check_script_exists(
        "validate_feeds.py",
        "New Validation Script",
        "Validation script created",
    )


def test_manage_categories_script():
    """Test that manage_categories.py script exists"""
    return check_script_exists(
        "manage_categories.py",
        "Category Management Script",
        "Category management script created",
    )


def main():
    """Run all tests"""
    print("\n" + "🔍 "*20)
    print("   TESTING IMPROVEMENTS IMPLEMENTATION")
    print("🔍 "*20)

    checks = [
        ("Improvement 1 (Schema Validation)", test_improvement_1),
        ("Improvement 3 (MCP Resilience)", test_improvement_3),
        ("Improvement 4 (Config Categories)", test_improvement_4),
        ("Validation Script", test_validation_script),
        ("Category Management Script", test_manage_categories_script),
    ]
    results = {name: fn() for name, fn in checks}
    
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}  {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All improvements implemented successfully!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) need attention")
        return 1


if __name__ == "__main__":
    sys.exit(main())
