#!/usr/bin/env python3
"""
Validate feeds.json and m365_data.json schema and data quality.
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def validate_feeds_json(feeds_path):
    """Validate feeds.json structure and required fields."""
    try:
        with open(feeds_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        return False, f"File not found: {feeds_path}"
    except json.JSONDecodeError as e:
        return False, f"Cannot parse JSON: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"
    
    # Check top-level structure
    required_top = ["lastUpdated", "totalArticles", "articles"]
    for field in required_top:
        if field not in data:
            return False, f"Missing top-level field: {field}"
    
    # Check articles
    articles = data.get("articles", [])
    if not isinstance(articles, list):
        return False, "articles must be an array"
    
    if len(articles) != data.get("totalArticles", 0):
        return False, f"Article count mismatch: {len(articles)} vs totalArticles={data.get('totalArticles')}"
    
    required_article_fields = ["title", "link", "published", "blog", "blogId", "author"]
    optional_with_defaults = {"lifecycleState": "ga", "datePrecision": "day"}
    
    errors = []
    warnings = []
    
    for idx, article in enumerate(articles):
        if not isinstance(article, dict):
            errors.append(f"Article {idx}: not an object")
            continue
        
        # Check required fields
        for field in required_article_fields:
            value = article.get(field, "")
            if not isinstance(value, str) or not value.strip():
                errors.append(f"Article {idx}: missing or empty '{field}'")
        
        # Check optional fields with defaults - set defaults if missing
        for field, default in optional_with_defaults.items():
            if field not in article or not article.get(field):
                article[field] = default
        
        # Validate published date format (ISO 8601)
        published = article.get("published", "")
        if published and not published.startswith(("202", "201", "203")):
            warnings.append(f"Article {idx}: suspicious published date format: {published[:20]}")
    
    # Limit errors to first 10 for display
    if len(errors) > 10:
        errors_display = errors[:10] + [f"... and {len(errors) - 10} more errors"]
    else:
        errors_display = errors
    
    if errors:
        return False, "Validation errors:\n  " + "\n  ".join(errors_display)
    
    status = f"✅ Valid: {len(articles)} articles"
    if warnings:
        status += f"\n⚠️  {len(warnings)} warnings:\n  " + "\n  ".join(warnings[:3])
    
    return True, status


def validate_m365_data_json(m365_path):
    """Validate m365_data.json structure."""
    try:
        with open(m365_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        return False, f"File not found: {m365_path}"
    except json.JSONDecodeError as e:
        return False, f"Cannot parse JSON: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"
    
    required_top = ["generatedAt", "totalArticles", "articles"]
    for field in required_top:
        if field not in data:
            return False, f"Missing top-level field: {field}"
    
    articles = data.get("articles", [])
    if not isinstance(articles, list):
        return False, "articles must be an array"
    
    if len(articles) != data.get("totalArticles", 0):
        return False, f"Article count mismatch: {len(articles)} vs totalArticles={data.get('totalArticles')}"
    
    required_fields = ["title", "link", "published", "source"]
    errors = []
    
    for idx, article in enumerate(articles):
        if not isinstance(article, dict):
            errors.append(f"Article {idx}: not an object")
            continue
        
        for field in required_fields:
            value = article.get(field, "")
            if not isinstance(value, str) or not value.strip():
                errors.append(f"Article {idx}: missing or empty '{field}'")
    
    if errors:
        errors_display = errors[:10] + ([f"... and {len(errors) - 10} more"] if len(errors) > 10 else [])
        return False, "Validation errors:\n  " + "\n  ".join(errors_display)
    
    return True, f"✅ Valid: {len(articles)} M365 articles"


def main():
    """Validate both feeds.json and m365_data.json."""
    feeds_path = REPO_ROOT / "data" / "feeds.json"
    m365_path = REPO_ROOT / "data" / "m365_data.json"
    
    print("🔍 Validating feeds...")
    feeds_valid, feeds_msg = validate_feeds_json(feeds_path)
    print(f"  Azure feeds.json: {feeds_msg}")
    
    print("🔍 Validating M365 data...")
    m365_valid, m365_msg = validate_m365_data_json(m365_path)
    print(f"  M365 m365_data.json: {m365_msg}")
    
    if feeds_valid and m365_valid:
        print("\n✅ All validations passed!")
        return 0
    else:
        print("\n❌ Validation failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
