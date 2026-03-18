import pathlib
import sys
import unittest
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import fetch_feeds


class NormalizeArticleUrlTests(unittest.TestCase):
    def test_strips_tracking_parameters_and_normalizes_url(self):
        url = (
            "HTTPS://WWW.Example.com//path//to//post/"
            "?utm_source=newsletter&b=2&a=1&gclid=abc#section"
        )
        self.assertEqual(
            fetch_feeds.normalize_article_url(url),
            "https://example.com/path/to/post?a=1&b=2",
        )

    def test_keeps_non_default_port(self):
        url = "https://www.example.com:8443/api/item/?trk=x&id=42"
        self.assertEqual(
            fetch_feeds.normalize_article_url(url),
            "https://example.com:8443/api/item?id=42",
        )


class DedupeArticlesTests(unittest.TestCase):
    def test_discards_stale_and_duplicate_articles(self):
        now = datetime.now(timezone.utc)
        fresh = (now - timedelta(days=1)).isoformat()
        same_day = (now - timedelta(days=1, hours=1)).isoformat()
        stale = (now - timedelta(days=31)).isoformat()

        articles = [
            {
                "title": "Important Azure release notes for workload teams everywhere",
                "link": "https://www.example.com/path/to/post?utm_source=newsletter&a=1",
                "published": fresh,
            },
            {
                "title": "Different title but same canonical link",
                "link": "https://example.com/path/to/post?a=1&gclid=abc",
                "published": fresh,
            },
            {
                "title": "Important Azure release notes for workload teams everywhere!!!",
                "link": "https://example.com/path/to/another-post",
                "published": same_day,
            },
            {
                "title": "Old article should be removed",
                "link": "https://example.com/path/to/old-post",
                "published": stale,
            },
            {
                "title": "Another unique release update for operations teams",
                "link": "https://example.com/path/to/unique-post",
                "published": fresh,
            },
        ]

        deduped = fetch_feeds.dedupe_articles(articles)
        self.assertEqual(
            [article["title"] for article in deduped],
            [
                "Important Azure release notes for workload teams everywhere",
                "Another unique release update for operations teams",
            ],
        )


class ClassifyLifecycleTests(unittest.TestCase):
    def test_detects_preview_titles(self):
        article = {"title": "[In preview] New accelerator for distributed workloads"}
        self.assertEqual(fetch_feeds.classify_lifecycle(article), "in_preview")

    def test_detects_generally_available_titles(self):
        article = {"title": "Generally available: hardened deployment feature"}
        self.assertEqual(fetch_feeds.classify_lifecycle(article), "launched_ga")

    def test_in_development_rules_take_precedence_over_ga_terms(self):
        article = {
            "title": "Generally available retirement timeline update for legacy SKU"
        }
        self.assertEqual(fetch_feeds.classify_lifecycle(article), "in_development")


class AttachLinksToSummaryTests(unittest.TestCase):
    def test_adds_markdown_links_for_matching_bullets(self):
        summary = (
            "- In preview:\n"
            "  - Feature Alpha now in preview\n"
            "  - none noted in selected window"
        )
        summary_articles = [
            {
                "title": "Feature Alpha now in preview",
                "link": "https://example.com/alpha",
            }
        ]

        result = fetch_feeds.attach_links_to_summary(summary, summary_articles)
        self.assertIn(
            "  - [Feature Alpha now in preview](https://example.com/alpha)",
            result,
        )
        self.assertIn("  - none noted in selected window", result)

    def test_preserves_existing_markdown_links(self):
        summary = (
            "- In preview:\n"
            "  - [Already linked item](https://example.com/existing)"
        )
        result = fetch_feeds.attach_links_to_summary(summary, [])
        self.assertIn(
            "  - [Already linked item](https://example.com/existing)",
            result,
        )
        self.assertEqual(result.count("https://example.com/existing"), 1)


if __name__ == "__main__":
    unittest.main()
