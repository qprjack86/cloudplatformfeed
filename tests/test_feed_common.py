import json
import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import feed_common


def _valid_article(**overrides):
    article = {
        "title": "Sample title",
        "link": "https://example.com/article",
        "published": "2026-04-01T00:00:00+00:00",
        "summary": "Sample summary",
        "blog": "Sample Blog",
        "blogId": "sample-blog",
        "author": "Author",
    }
    article.update(overrides)
    return article


class SiteConfigTests(unittest.TestCase):
    def test_load_site_config_enforces_canonical_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "site.json"
            path.write_text(
                json.dumps(
                    {
                        "canonicalHost": "WWW.Example.COM",
                        "canonicalUrl": "https://example.com/",
                    }
                ),
                encoding="utf-8",
            )
            loaded = feed_common.load_site_config(path)

        self.assertEqual(loaded["canonicalHost"], "example.com")
        self.assertEqual(loaded["canonicalUrl"], "https://example.com")

    def test_load_site_config_rejects_non_root_url(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "site.json"
            path.write_text(
                json.dumps(
                    {
                        "canonicalHost": "example.com",
                        "canonicalUrl": "https://example.com/path",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                feed_common.load_site_config(path)


class UrlHelperTests(unittest.TestCase):
    def test_canonicalize_url_filters_tracking_and_sorts_query(self):
        url = (
            "HTTPS://WWW.Example.com//a//b/?utm_source=newsletter&gclid=abc&z=3&a=1"
        )

        normalized = feed_common.canonicalize_url(
            url,
            tracking_query_prefixes=("utm_",),
            tracking_query_keys={"gclid"},
        )

        self.assertEqual(normalized, "https://example.com/a/b?a=1&z=3")

    def test_extract_youtube_video_id_supports_watch_and_short_urls(self):
        self.assertEqual(
            feed_common.extract_youtube_video_id("https://www.youtube.com/watch?v=abc123"),
            "abc123",
        )
        self.assertEqual(
            feed_common.extract_youtube_video_id("https://youtu.be/xyz987?t=10"),
            "xyz987",
        )


class ChecksumAndFailsafeTests(unittest.TestCase):
    def test_build_checksums_payload_shape(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = pathlib.Path(tmpdir) / "artifact.json"
            file_path.write_text('{"ok": true}\n', encoding="utf-8")

            payload = feed_common.build_checksums_payload(
                [file_path],
                generated_at="2026-03-20T00:00:00+00:00",
            )

        self.assertEqual(payload["generatedAt"], "2026-03-20T00:00:00+00:00")
        self.assertEqual(len(payload["artifacts"]), 1)
        self.assertEqual(payload["artifacts"][0]["algorithm"], "sha256")

    def test_evaluate_publish_failsafe_thresholds(self):
        triggered, details = feed_common.evaluate_publish_failsafe(
            new_count=90,
            previous_count=200,
            min_articles=80,
            min_ratio=0.60,
        )

        self.assertTrue(triggered)
        self.assertIn("relative_trigger=True", details)

    def test_artifact_checksum_record_rejects_parent_segments(self):
        with self.assertRaises(ValueError):
            feed_common._artifact_checksum_record("../artifact.json", "2026-04-01T00:00:00+00:00")

    def test_load_previous_article_count_uses_total_articles_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "baseline.json"
            path.write_text(json.dumps({"totalArticles": 12}), encoding="utf-8")

            count = feed_common.load_previous_article_count(path)

        self.assertEqual(count, 12)

    def test_load_previous_article_count_logs_when_counts_missing(self):
        logs = []
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "baseline.json"
            path.write_text(json.dumps({"other": True}), encoding="utf-8")

            count = feed_common.load_previous_article_count(path, logger=logs.append)

        self.assertIsNone(count)
        self.assertTrue(any("missing both articles array and totalArticles" in line for line in logs))


class ValidationHelperTests(unittest.TestCase):
    def test_validate_article_schema_applies_defaults(self):
        article = _valid_article()

        is_valid, msg = feed_common.validate_article_schema(article)

        self.assertTrue(is_valid, msg)
        self.assertEqual(article["lifecycleState"], "ga")
        self.assertEqual(article["datePrecision"], "day")

    def test_validate_article_schema_preserves_required_field_failures(self):
        article = _valid_article(title="")

        is_valid, msg = feed_common.validate_article_schema(article)

        self.assertFalse(is_valid)
        self.assertIn("Missing or empty required field: title", msg)
        self.assertEqual(article["lifecycleState"], "ga")
        self.assertEqual(article["datePrecision"], "day")

    def test_validate_feed_data_caps_issue_logging(self):
        articles = [_valid_article(title="") for _ in range(8)]
        logs = []

        is_valid, summary = feed_common.validate_feed_data(
            articles,
            min_coverage_percent=0,
            logger=logs.append,
        )

        self.assertTrue(is_valid)
        self.assertIn("Validation passed:", summary)
        self.assertEqual(len(logs), feed_common.ARTICLE_VALIDATION_ISSUE_LIMIT)
        self.assertTrue(all(line.startswith("  ⚠️  Article ") for line in logs))


if __name__ == "__main__":
    unittest.main()
