import pathlib
import sys
import unittest
import tempfile
import json
from unittest import mock
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

    def test_detects_retirement_titles(self):
        article = {"title": "Retirement: legacy SKU support ends in 2026"}
        self.assertEqual(fetch_feeds.classify_lifecycle(article), "retiring")

    def test_detects_in_development_titles(self):
        article = {"title": "[In development] New accelerator for distributed workloads"}
        self.assertEqual(fetch_feeds.classify_lifecycle(article), "in_development")


class RenderSummaryMarkdownTests(unittest.TestCase):
    def test_includes_retiring_section_when_populated(self):
        buckets = {
            "in_preview": [],
            "launched_ga": [],
            "retiring": [{"label": "Legacy SKU retirement announced", "link": "https://example.com/retire"}],
            "in_development": [],
        }

        result = fetch_feeds.render_summary_markdown(buckets)

        self.assertIn(
            "- Retiring:\n  • [Legacy SKU retirement announced](https://example.com/retire)",
            result,
        )


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


class SiteConfigTests(unittest.TestCase):
    def test_load_site_config_accepts_valid_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = pathlib.Path(tmpdir) / "site.json"
            config.write_text(
                json.dumps(
                    {
                        "canonicalHost": "www.Example.COM",
                        "canonicalUrl": "https://example.com/",
                    }
                ),
                encoding="utf-8",
            )
            loaded = fetch_feeds.load_site_config(str(config))

        self.assertEqual(loaded["canonicalHost"], "example.com")
        self.assertEqual(loaded["canonicalUrl"], "https://example.com")

    def test_load_site_config_rejects_non_https_urls(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = pathlib.Path(tmpdir) / "site.json"
            config.write_text(
                json.dumps(
                    {
                        "canonicalHost": "example.com",
                        "canonicalUrl": "http://example.com",
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                fetch_feeds.load_site_config(str(config))

    def test_load_site_config_rejects_host_mismatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = pathlib.Path(tmpdir) / "site.json"
            config.write_text(
                json.dumps(
                    {
                        "canonicalHost": "example.com",
                        "canonicalUrl": "https://other.example.com",
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                fetch_feeds.load_site_config(str(config))


class ChecksumMetadataTests(unittest.TestCase):
    def test_build_checksums_payload_includes_expected_shape(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            feeds = root / "data" / "feeds.json"
            feed_xml = root / "data" / "feed.xml"
            feeds.parent.mkdir(parents=True, exist_ok=True)
            feeds.write_text('{"ok":true}\n', encoding="utf-8")
            feed_xml.write_text('<rss></rss>\n', encoding="utf-8")

            payload = fetch_feeds.build_checksums_payload(
                [feeds, feed_xml],
                generated_at="2026-03-19T00:00:00+00:00",
            )

        self.assertEqual(payload["generatedAt"], "2026-03-19T00:00:00+00:00")
        self.assertEqual(len(payload["artifacts"]), 2)
        self.assertEqual(
            payload["artifacts"][0],
            {
                "path": feeds.as_posix(),
                "algorithm": "sha256",
                "value": "e5f1eb4d806641698a35efe20e098efd20d7d57a9b90ee69079d5bb650920726",
                "generatedAt": "2026-03-19T00:00:00+00:00",
            },
        )
        self.assertEqual(payload["artifacts"][1]["path"], feed_xml.as_posix())
        self.assertEqual(payload["artifacts"][1]["algorithm"], "sha256")
        self.assertEqual(payload["artifacts"][1]["generatedAt"], "2026-03-19T00:00:00+00:00")
        self.assertEqual(
            payload["artifacts"][1]["value"],
            "4aff68cc72ca39863a0639b0a6683a6b089cda528e2451390fbea4e61f9267b6",
        )

    def test_write_checksums_file_writes_json_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            feeds = root / "data" / "feeds.json"
            feed_xml = root / "data" / "feed.xml"
            checksums = root / "data" / "checksums.json"
            feeds.parent.mkdir(parents=True, exist_ok=True)
            feeds.write_text('{"ok":true}\n', encoding="utf-8")
            feed_xml.write_text('<rss></rss>\n', encoding="utf-8")

            payload = fetch_feeds.write_checksums_file(
                [feeds, feed_xml],
                output_path=checksums,
                generated_at="2026-03-19T00:00:00+00:00",
            )

            written = json.loads(checksums.read_text(encoding="utf-8"))

        self.assertEqual(written, payload)
        self.assertEqual(written["artifacts"][0]["path"], feeds.as_posix())
        self.assertEqual(written["artifacts"][1]["path"], feed_xml.as_posix())


class RunMetricsTests(unittest.TestCase):
    def test_build_run_metrics_for_normal_publish(self):
        metrics = fetch_feeds.build_run_metrics(
            raw_article_count=300,
            unique_article_count=210,
            previous_article_count=205,
            failsafe_triggered=False,
            failsafe_details="guard passed",
            published=True,
            summary_payload={
                "status": "available",
                "reason": None,
                "articleCount": 7,
            },
            savill_video={"title": "Latest Azure Infrastructure Update"},
        )

        self.assertIsInstance(metrics.get("generatedAt"), str)
        self.assertEqual(metrics["rawArticleCount"], 300)
        self.assertEqual(metrics["uniqueArticleCount"], 210)
        self.assertEqual(metrics["previousArticleCount"], 205)
        self.assertFalse(metrics["failsafeTriggered"])
        self.assertTrue(metrics["published"])
        self.assertEqual(metrics["summaryStatus"], "available")
        self.assertIsNone(metrics["summaryReason"])
        self.assertEqual(metrics["summaryArticleCount"], 7)
        self.assertTrue(metrics["savillVideoFound"])

    def test_build_run_metrics_for_failsafe_early_exit(self):
        metrics = fetch_feeds.build_run_metrics(
            raw_article_count=220,
            unique_article_count=60,
            previous_article_count=210,
            failsafe_triggered=True,
            failsafe_details="large drop detected",
            published=False,
            summary_payload=None,
            savill_video=None,
        )

        self.assertTrue(metrics["failsafeTriggered"])
        self.assertEqual(metrics["failsafeDetails"], "large drop detected")
        self.assertFalse(metrics["published"])
        self.assertIsNone(metrics["summaryStatus"])
        self.assertIsNone(metrics["summaryReason"])
        self.assertIsNone(metrics["summaryArticleCount"])
        self.assertFalse(metrics["savillVideoFound"])

    def test_write_run_metrics_skips_when_path_missing(self):
        result = fetch_feeds.write_run_metrics({"hello": "world"}, output_path="")
        self.assertFalse(result)

    def test_write_run_metrics_writes_json_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "run-metrics.json"
            metrics = {"published": True, "rawArticleCount": 5}
            result = fetch_feeds.write_run_metrics(metrics, output_path=str(path))
            self.assertTrue(result)
            written = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(written, metrics)


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


class YouTubeVideoHelperTests(unittest.TestCase):
    def test_extract_youtube_video_id_supports_watch_and_short_urls(self):
        self.assertEqual(
            fetch_feeds._extract_youtube_video_id("https://www.youtube.com/watch?v=abc123XYZ"),
            "abc123XYZ",
        )
        self.assertEqual(
            fetch_feeds._extract_youtube_video_id("https://youtu.be/abc123XYZ?t=10"),
            "abc123XYZ",
        )

    def test_select_best_youtube_video_entry_falls_back_to_latest(self):
        entries = [
            {"title": "Latest random upload", "link": "https://www.youtube.com/watch?v=latest"},
            {"title": "Older random upload", "link": "https://www.youtube.com/watch?v=older"},
        ]

        best, used_fallback = fetch_feeds._select_best_youtube_video_entry(
            entries,
            lambda _: 0,
        )

        self.assertTrue(used_fallback)
        self.assertEqual(best["link"], "https://www.youtube.com/watch?v=latest")

    def test_resolve_youtube_channel_id_from_seed_extracts_channel_id(self):
        session = mock.Mock()
        response = mock.Mock()
        response.text = '<script>{"channelId":"UC123abcXYZ"}</script>'
        response.raise_for_status.return_value = None
        session.get.return_value = response

        channel_id = fetch_feeds._resolve_youtube_channel_id_from_seed(
            session,
            "https://www.youtube.com/watch?v=abc123XYZ",
            (5, 20),
        )

        self.assertEqual(channel_id, "UC123abcXYZ")


if __name__ == "__main__":
    unittest.main()
