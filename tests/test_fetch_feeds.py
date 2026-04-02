import pathlib
import sys
import unittest
import tempfile
import json
import os
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
        now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0)
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


class ParseIsoDatetimeTests(unittest.TestCase):
    def test_parses_high_precision_fractional_seconds(self):
        parsed = fetch_feeds.parse_iso_datetime("2026-03-20T19:15:35.2206794Z")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(), "2026-03-20T19:15:35.220679+00:00")


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


class FeedConcurrencyTests(unittest.TestCase):
    def test_techcommunity_parallel_fetch_continues_after_individual_failure(self):
        good_feed = mock.Mock()
        good_feed.bozo = False
        good_feed.entries = [
            {
                "title": "Healthy feed item",
                "link": "https://example.com/post",
                "published": "Mon, 24 Mar 2026 12:00:00 GMT",
                "summary": "Hello",
                "author": "Microsoft",
            }
        ]

        def fake_fetch(url):
            if "board.id=good-board" in url:
                return good_feed
            raise ValueError("simulated board failure")

        with mock.patch.dict(
            fetch_feeds.BLOGS,
            {"good-board": "Good Board", "bad-board": "Bad Board"},
            clear=True,
        ), mock.patch.object(fetch_feeds, "fetch_feed", side_effect=fake_fetch) as fetch_mock:
            articles = fetch_feeds.fetch_tech_community_feeds()

        self.assertEqual(fetch_mock.call_count, 2)
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["blogId"], "good-board")


class RssGenerationTests(unittest.TestCase):
    def test_generate_rss_feed_wraps_text_fields_in_cdata(self):
        article = {
            "title": "Launch & Learn <Now>",
            "link": "https://example.com/item",
            "published": "2026-03-22T10:30:00+00:00",
            "summary": "Summary with <b>html</b> & characters",
            "blog": "Azure Updates",
            "blogId": "azureupdates",
            "author": "Microsoft",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            old_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                os.makedirs("data", exist_ok=True)
                fetch_feeds.generate_rss_feed([article])
                xml_text = pathlib.Path("data/feed.xml").read_text(encoding="utf-8")
            finally:
                os.chdir(old_cwd)

        self.assertIn("<title><![CDATA[Launch & Learn <Now>]]></title>", xml_text)
        self.assertIn("<description><![CDATA[Summary with <b>html</b> & characters]]></description>", xml_text)
        self.assertIn("<dc:creator><![CDATA[Microsoft]]></dc:creator>", xml_text)
        self.assertIn("<category><![CDATA[Azure Updates]]></category>", xml_text)

    def test_generate_rss_feed_handles_cdata_terminator_safely(self):
        article = {
            "title": "Edge Case",
            "link": "https://example.com/item",
            "published": "2026-03-22T10:30:00+00:00",
            "summary": "Contains ]]> token",
            "blog": "Azure Updates",
            "blogId": "azureupdates",
            "author": "Microsoft",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            old_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                os.makedirs("data", exist_ok=True)
                fetch_feeds.generate_rss_feed([article])
                xml_text = pathlib.Path("data/feed.xml").read_text(encoding="utf-8")
            finally:
                os.chdir(old_cwd)

        self.assertIn("Contains ]]&gt; token", xml_text)


class AzureUpdatesApiFallbackTests(unittest.TestCase):
    def test_parse_azure_update_item_extracts_metadata_and_date(self):
        item = {
            "id": "123456",
            "title": "Generally Available: Sample Azure capability",
            "description": "<p>Rich <b>summary</b> text.</p>",
            "status": "Launched",
            "generalAvailabilityDate": "2026-04",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["blogId"], "azureupdates")
        self.assertEqual(article["lifecycle"], "launched_ga")
        self.assertEqual(article["azureStatus"], "Launched")
        self.assertEqual(article["azureTargetDate"], "2026-04")
        self.assertEqual(article["azureGeneralAvailabilityDate"], "2026-04")
        self.assertEqual(article["published"], "2026-03-22T10:30:00+00:00")
        self.assertEqual(article["link"], "https://azure.microsoft.com/en-us/updates/123456/")

    def test_parse_azure_update_item_extracts_preview_date(self):
        item = {
            "id": "654321",
            "title": "Public Preview: Sample Azure capability",
            "status": "In preview",
            "previewAvailabilityDate": "2026-05",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azurePreviewDate"], "2026-05")
        self.assertEqual(article["azureTargetDate"], "2026-05")
        self.assertNotIn("azureGeneralAvailabilityDate", article)

    def test_parse_azure_update_item_extracts_preview_and_ga_dates(self):
        item = {
            "id": "777777",
            "title": "Generally Available: Dual milestone update",
            "status": "Launched",
            "previewAvailabilityDate": "2026-04",
            "generalAvailabilityDate": "2026-05",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azurePreviewDate"], "2026-04")
        self.assertEqual(article["azureGeneralAvailabilityDate"], "2026-05")
        self.assertEqual(article["azureTargetDate"], "2026-05")

    def test_parse_azure_update_item_falls_back_to_target_date(self):
        item = {
            "id": "888888",
            "title": "Coming soon: Legacy target date field",
            "status": "In development",
            "targetDate": "2026-08",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azureTargetDate"], "2026-08")
        self.assertNotIn("azurePreviewDate", article)
        self.assertNotIn("azureGeneralAvailabilityDate", article)

    def test_parse_azure_update_item_extracts_retirement_date_from_title(self):
        item = {
            "id": "889000",
            "title": "Retirement: Example service will be retired on July 31, 2031",
            "description": "Planning guidance.",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["lifecycle"], "retiring")
        self.assertEqual(article["azureRetirementDate"], "2031-07-31")

    def test_parse_azure_update_item_extracts_retirement_date_from_description_dmy(self):
        item = {
            "id": "889001",
            "title": "Retirement: Example service",
            "description": (
                ("noise " * 140)
                + "The service will be retired on 30 November 2030 after transition."
            ),
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azureRetirementDate"], "2030-11-30")

    def test_parse_azure_update_item_extracts_retirement_month_only(self):
        item = {
            "id": "889002",
            "title": "Retirement: Managed NGINX add-on retiring November 2026",
            "description": "Please migrate workloads.",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azureRetirementDate"], "2026-11")

    def test_parse_azure_update_item_prefers_retirement_context_date(self):
        item = {
            "id": "889003",
            "title": "Retirement: Example service timeline",
            "description": (
                "Migration begins March 1, 2030. "
                "This service will be retired on January 12, 2027."
            ),
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azureRetirementDate"], "2027-01-12")

    def test_parse_azure_update_item_omits_retirement_date_when_not_found(self):
        item = {
            "id": "889004",
            "title": "Retirement: Legacy feature notice",
            "description": "Support policy details without explicit date value.",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertNotIn("azureRetirementDate", article)

    def test_parse_azure_update_item_does_not_override_structured_dates_for_retirement(self):
        item = {
            "id": "889005",
            "title": "Retirement: Example feature",
            "description": "Retired on July 31, 2031.",
            "targetDate": "2027-04",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azureTargetDate"], "2027-04")
        self.assertNotIn("azureRetirementDate", article)

    def test_parse_azure_update_item_prefers_later_title_retirement_date(self):
        item = {
            "id": "889006",
            "title": "Retirement: NP-series example service will be retired on May 31, 2027",
            "description": (
                "Operational note: related batch support will end on April 2, 2026. "
                "Please migrate before retirement."
            ),
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["azureRetirementDate"], "2027-05-31")

    def test_fetch_azure_updates_feed_falls_back_to_rss_on_api_exception(self):
        rss_articles = [{"title": "RSS fallback article"}]
        with mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_api",
            side_effect=RuntimeError("api unavailable"),
        ) as api_mock, mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_rss",
            return_value=rss_articles,
        ) as rss_mock:
            result = fetch_feeds.fetch_azure_updates_feed()

        self.assertEqual(result, rss_articles)
        api_mock.assert_called_once()
        rss_mock.assert_called_once()

    def test_fetch_azure_updates_feed_falls_back_to_rss_when_api_returns_no_valid_items(self):
        rss_articles = [{"title": "RSS fallback article"}]
        with mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_api",
            return_value=[],
        ) as api_mock, mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_rss",
            return_value=rss_articles,
        ) as rss_mock:
            result = fetch_feeds.fetch_azure_updates_feed()

        self.assertEqual(result, rss_articles)
        api_mock.assert_called_once()
        rss_mock.assert_called_once()

    def test_fetch_azure_updates_feed_keeps_api_results_when_non_empty(self):
        api_articles = [{"title": "API article", "published": "2026-03-22T10:30:00+00:00"}]
        with mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_api",
            return_value=api_articles,
        ) as api_mock, mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_rss",
        ) as rss_mock:
            result = fetch_feeds.fetch_azure_updates_feed()

        self.assertEqual(result, api_articles)
        api_mock.assert_called_once()
        rss_mock.assert_not_called()


class AzttyFeedTests(unittest.TestCase):
    def test_fetch_aztty_feed_maps_entries_and_retirement_fields(self):
        feed = mock.Mock()
        feed.bozo = False
        feed.entries = [
            {
                "title": "Retirement: Example service will be retired on July 31, 2031",
                "summary": "Migrate before the retirement date.",
                "link": "https://aztty.azurewebsites.net/announcements/1",
                "author": "Microsoft",
                "published": "Mon, 24 Mar 2026 12:00:00 GMT",
            }
        ]

        with mock.patch.object(fetch_feeds, "fetch_feed", return_value=feed) as fetch_mock:
            articles = fetch_feeds.fetch_aztty_feed(
                fetch_feeds.AZTTY_DEPRECATIONS_FEED,
                "Azure Deprecations (aztty)",
                "azuredeprecations",
                "deprecation",
            )

        fetch_mock.assert_called_once_with(fetch_feeds.AZTTY_DEPRECATIONS_FEED)
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["announcementType"], "deprecation")
        self.assertEqual(articles[0]["lifecycle"], "retiring")
        self.assertEqual(articles[0]["azureRetirementDate"], "2031-07-31")

    def test_fetch_aztty_announcements_continues_when_one_feed_fails(self):
        surviving_feed_articles = [
            {
                "title": "Update: Example feature is now available",
                "link": "https://aztty.azurewebsites.net/announcements/2",
                "published": "2026-03-24T12:00:00+00:00",
                "summary": "Example",
                "blog": "Azure Updates (aztty)",
                "blogId": "azttyupdates",
                "author": "Microsoft",
                "announcementType": "update",
            }
        ]

        with mock.patch.object(
            fetch_feeds,
            "fetch_aztty_feed",
            side_effect=[RuntimeError("deprecations unavailable"), surviving_feed_articles],
        ) as fetch_mock:
            results = fetch_feeds.fetch_aztty_announcements()

        self.assertEqual(fetch_mock.call_count, 2)
        self.assertEqual(results, surviving_feed_articles)


class RetirementCalendarTests(unittest.TestCase):
    def test_build_azure_retirement_calendar_dedupes_and_aggregates_sources(self):
        articles = [
            {
                "title": "Retirement: Example service",
                "link": "https://aztty.azurewebsites.net/announcements/1",
                "published": "2026-03-24T12:00:00+00:00",
                "blog": "Azure Deprecations (aztty)",
                "blogId": "azuredeprecations",
                "announcementType": "deprecation",
                "azureRetirementDate": "2031-07-31",
            },
            {
                "title": "Update: Example service",
                "link": "https://azure.microsoft.com/en-us/updates/123456/",
                "published": "2026-03-25T12:00:00+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "azureRetirementDate": "2031-07-31",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event["title"], "Example service")
        self.assertEqual(event["blogId"], "azureupdates")
        self.assertEqual(event["link"], "https://azure.microsoft.com/en-us/updates/123456/")
        self.assertEqual(event["retirementDate"], "2031-07-31")
        self.assertEqual(event["sourceCount"], 2)
        self.assertIn("Azure Deprecations (aztty)", event["sources"])
        self.assertIn("Azure Updates", event["sources"])

    def test_build_azure_retirement_calendar_filters_past_and_invalid_dates(self):
        articles = [
            {
                "title": "Retirement: Valid future month",
                "link": "https://example.com/future",
                "published": "2026-03-24T12:00:00+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "azureRetirementDate": "2030-11",
            },
            {
                "title": "Retirement: Old item",
                "link": "https://example.com/old",
                "published": "2026-03-24T12:00:00+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "azureRetirementDate": "2020-01-01",
            },
            {
                "title": "Retirement: Invalid item",
                "link": "https://example.com/invalid",
                "published": "2026-03-24T12:00:00+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "azureRetirementDate": "not-a-date",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["retirementDate"], "2030-11")
        self.assertEqual(events[0]["datePrecision"], "month")


class MainOutputSchemaTests(unittest.TestCase):
    def test_main_writes_azure_retirement_calendar(self):
        aztty_articles = [
            {
                "title": "Retirement: Example service retirement notice",
                "link": "https://example.com/retirement",
                "published": "2031-01-10T12:00:00+00:00",
                "summary": "Retirement details",
                "blog": "Azure Deprecations (aztty)",
                "blogId": "azuredeprecations",
                "author": "Microsoft",
                "announcementType": "deprecation",
                "lifecycle": "retiring",
                "azureRetirementDate": "2031-07-31",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            old_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                with mock.patch.object(fetch_feeds, "fetch_tech_community_feeds", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_aks_blog", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_devblogs_feeds", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_azure_updates_feed", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_aztty_announcements", return_value=aztty_articles), \
                    mock.patch.object(fetch_feeds, "fetch_savill_video", return_value=None), \
                    mock.patch.object(
                        fetch_feeds,
                        "generate_ai_summary",
                        return_value={
                            "status": "unavailable",
                            "reason": "no_articles_in_window",
                            "windowDays": fetch_feeds.SUMMARY_WINDOW_DAYS,
                            "publishingDays": [],
                        },
                    ), \
                    mock.patch.object(fetch_feeds, "generate_rss_feed"), \
                    mock.patch.object(fetch_feeds, "write_checksums_file"), \
                    mock.patch.object(fetch_feeds, "write_run_metrics"):
                    fetch_feeds.main()
            finally:
                os.chdir(old_cwd)

            payload = json.loads((pathlib.Path(tmpdir) / "data" / "feeds.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["totalArticles"], 1)
        self.assertIn("azureRetirementCalendar", payload)
        self.assertEqual(len(payload["azureRetirementCalendar"]), 1)
        self.assertEqual(
            payload["azureRetirementCalendar"][0]["retirementDate"],
            "2031-07-31",
        )
        self.assertEqual(
            payload["azureRetirementCalendar"][0]["title"],
            "Example service retirement notice",
        )


if __name__ == "__main__":
    unittest.main()
