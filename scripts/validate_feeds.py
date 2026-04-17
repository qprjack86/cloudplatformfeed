#!/usr/bin/env python3
"""
Validate feeds.json and m365_data.json schema and data quality.
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_json_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return True, json.load(f)
    except FileNotFoundError:
        return False, f"File not found: {path}"
    except json.JSONDecodeError as e:
        return False, f"Cannot parse JSON: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"


def _validate_top_level_structure(data, required_top):
    for field in required_top:
        if field not in data:
            return False, f"Missing top-level field: {field}"

    articles = data.get("articles", [])
    if not isinstance(articles, list):
        return False, "articles must be an array"

    if len(articles) != data.get("totalArticles", 0):
        return False, f"Article count mismatch: {len(articles)} vs totalArticles={data.get('totalArticles')}"

    return True, articles


def _collect_article_required_field_errors(articles, required_fields):
    errors = []
    for idx, article in enumerate(articles):
        if not isinstance(article, dict):
            errors.append(f"Article {idx}: not an object")
            continue

        for field in required_fields:
            value = article.get(field, "")
            if not isinstance(value, str) or not value.strip():
                errors.append(f"Article {idx}: missing or empty '{field}'")
    return errors


def _format_validation_errors(errors, overflow_suffix="more errors"):
    if len(errors) > 10:
        errors_display = errors[:10] + [f"... and {len(errors) - 10} {overflow_suffix}"]
    else:
        errors_display = errors
    return "Validation errors:\n  " + "\n  ".join(errors_display)


def validate_feeds_json(feeds_path):
    """Validate feeds.json structure and required fields."""
    loaded, data_or_error = _load_json_file(feeds_path)
    if not loaded:
        return False, data_or_error

    data = data_or_error
    required_top = ["lastUpdated", "totalArticles", "articles"]
    top_ok, articles_or_error = _validate_top_level_structure(data, required_top)
    if not top_ok:
        return False, articles_or_error

    articles = articles_or_error
    required_article_fields = ["title", "link", "published", "blog", "blogId", "author"]
    optional_with_defaults = {"lifecycleState": "ga", "datePrecision": "day"}

    warnings = []

    for idx, article in enumerate(articles):
        if not isinstance(article, dict):
            continue

        # Check optional fields with defaults - set defaults if missing
        for field, default in optional_with_defaults.items():
            if field not in article or not article.get(field):
                article[field] = default

        # Validate published date format (ISO 8601)
        published = article.get("published", "")
        if published and not published.startswith(("202", "201", "203")):
            warnings.append(f"Article {idx}: suspicious published date format: {published[:20]}")

    errors = _collect_article_required_field_errors(articles, required_article_fields)
    if errors:
        return False, _format_validation_errors(errors, overflow_suffix="more errors")

    status = f"✅ Valid: {len(articles)} articles"
    if warnings:
        status += f"\n⚠️  {len(warnings)} warnings:\n  " + "\n  ".join(warnings[:3])

    return True, status


def validate_m365_data_json(m365_path):
    """Validate m365_data.json structure."""
    loaded, data_or_error = _load_json_file(m365_path)
    if not loaded:
        return False, data_or_error

    data = data_or_error
    required_top = ["generatedAt", "totalArticles", "articles"]
    top_ok, articles_or_error = _validate_top_level_structure(data, required_top)
    if not top_ok:
        return False, articles_or_error

    articles = articles_or_error
    required_fields = ["title", "link", "published", "source"]
    errors = _collect_article_required_field_errors(articles, required_fields)

    if errors:
        return False, _format_validation_errors(errors, overflow_suffix="more")

    return True, f"✅ Valid: {len(articles)} M365 articles"


def main():
    """Validate both feeds.json and m365_data.json."""
    validation_jobs = [
        (
            "🔍 Validating feeds...",
            "  Azure feeds.json: ",
            validate_feeds_json,
            REPO_ROOT / "data" / "feeds.json",
        ),
        (
            "🔍 Validating M365 data...",
            "  M365 m365_data.json: ",
            validate_m365_data_json,
            REPO_ROOT / "data" / "m365_data.json",
        ),
    ]

    results = []
    for intro, label, validator, path in validation_jobs:
        print(intro)
        valid, message = validator(path)
        print(f"{label}{message}")
        results.append(valid)

    if all(results):
        print("\n✅ All validations passed!")
        return 0

    print("\n❌ Validation failed!")
    return 1


if __name__ == "__main__":
    sys.exit(main())
