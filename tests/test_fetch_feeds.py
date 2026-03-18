import pathlib
import sys
import unittest
import tempfile
import json
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


class PublishFailsafeTests(unittest.TestCase):
    def test_triggers_on_large_relative_drop(self):
        triggered, details = fetch_feeds.evaluate_publish_failsafe(
            new_count=90,
            previous_count=200,
        )
        self.assertTrue(triggered)
        self.assertIn("relative_trigger=True", details)

    def test_triggers_on_absolute_floor_when_baseline_is_healthy(self):
        triggered, details = fetch_feeds.evaluate_publish_failsafe(
            new_count=70,
            previous_count=120,
        )
        self.assertTrue(triggered)
        self.assertIn("absolute_trigger=True", details)

    def test_does_not_trigger_for_normal_variation(self):
        triggered, details = fetch_feeds.evaluate_publish_failsafe(
            new_count=125,
            previous_count=200,
        )
        self.assertFalse(triggered)
        self.assertIn("relative_trigger=False", details)
        self.assertIn("absolute_trigger=False", details)

    def test_does_not_trigger_when_baseline_is_missing_or_unreadable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = pathlib.Path(tmpdir) / "missing.json"
            previous_count = fetch_feeds.load_previous_article_count(str(missing))
            self.assertIsNone(previous_count)
            triggered, _ = fetch_feeds.evaluate_publish_failsafe(
                new_count=1,
                previous_count=previous_count,
            )
            self.assertFalse(triggered)

            bad = pathlib.Path(tmpdir) / "bad.json"
            bad.write_text("{ not json", encoding="utf-8")
            previous_count = fetch_feeds.load_previous_article_count(str(bad))
            self.assertIsNone(previous_count)
            triggered, _ = fetch_feeds.evaluate_publish_failsafe(
                new_count=1,
                previous_count=previous_count,
            )
            self.assertFalse(triggered)

    def test_absolute_floor_does_not_lock_recovery_when_baseline_below_floor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            baseline = pathlib.Path(tmpdir) / "feeds.json"
            baseline.write_text(
                json.dumps({"totalArticles": 70}),
                encoding="utf-8",
            )
            previous_count = fetch_feeds.load_previous_article_count(str(baseline))
            self.assertEqual(previous_count, 70)

        triggered, details = fetch_feeds.evaluate_publish_failsafe(
            new_count=75,
            previous_count=previous_count,
        )
        self.assertFalse(triggered)
        self.assertIn("absolute_trigger=False", details)


if __name__ == "__main__":
    unittest.main()
