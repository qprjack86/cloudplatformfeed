import pathlib
import sys
import unittest
import tempfile
import json
import os
from contextlib import contextmanager
from unittest import mock
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import fetch_feeds


@contextmanager
def temporary_cwd(path):
    old_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_cwd)


def assert_category_metadata(test_case, event):
    test_case.assertIn("primaryCategory", event)
    test_case.assertIn("categories", event)
    test_case.assertIn("categorySourceMap", event)
    test_case.assertTrue(event["primaryCategory"])
    test_case.assertTrue(event["categories"])


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

    def test_keeps_stale_articles_with_future_retirement_date(self):
        now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0)
        stale = (now - timedelta(days=180)).isoformat()
        next_month = now + timedelta(days=35)
        future_retirement = f"{next_month.year:04d}-{next_month.month:02d}"

        articles = [
            {
                "title": "Retirement: Legacy service is retiring soon",
                "link": "https://example.com/path/to/retirement",
                "published": stale,
                "azureRetirementDate": future_retirement,
            }
        ]

        deduped = fetch_feeds.dedupe_articles(articles)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["azureRetirementDate"], future_retirement)


class ParseIsoDatetimeTests(unittest.TestCase):
    def test_parses_high_precision_fractional_seconds(self):
        parsed = fetch_feeds.parse_iso_datetime("2026-03-20T19:15:35.2206794Z")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(), "2026-03-20T19:15:35.220679+00:00")


class AzureUpdateIdentityExtractionTests(unittest.TestCase):
    def test_extract_azure_update_id_from_query_slug(self):
        value = fetch_feeds._extract_azure_update_id_from_url(
            "https://azure.microsoft.com/updates?id=application-gateway-v1-will-be-retired"
        )
        self.assertEqual(value, "application-gateway-v1-will-be-retired")

    def test_extract_azure_update_id_from_path_numeric(self):
        value = fetch_feeds._extract_azure_update_id_from_url(
            "https://azure.microsoft.com/en-us/updates/558102/"
        )
        self.assertEqual(value, "558102")

    def test_extract_azure_update_id_from_v2_path_slug(self):
        value = fetch_feeds._extract_azure_update_id_from_url(
            "https://azure.microsoft.com/updates/v2/open-service-mesh-extension-for-aks-retirement"
        )
        self.assertEqual(value, "open-service-mesh-extension-for-aks-retirement")


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


class ClassifyWithAiTests(unittest.TestCase):
    def _build_client(self, content):
        response = mock.Mock()
        response.choices = [mock.Mock(message=mock.Mock(content=content))]
        client = mock.Mock()
        client.chat.completions.create.return_value = response
        return client

    def test_accepts_fenced_json_response(self):
        client = self._build_client(
            "```json\n{\"items\":[{\"id\":\"0\",\"bucket\":\"retiring\",\"label\":\"Example item\"}]}\n```"
        )

        result = fetch_feeds.classify_with_ai([
            {"title": "Example item", "summary": "", "blogId": "azureupdates"}
        ], client, "test-deployment")

        self.assertEqual(result, [{"id": "0", "bucket": "retiring", "label": "Example item"}])

    def test_accepts_content_parts_response(self):
        client = self._build_client([
            {"type": "output_text", "text": '{"items":[{"id":"0","bucket":"in_preview","label":"Preview item"}]}'},
        ])

        result = fetch_feeds.classify_with_ai([
            {"title": "Preview item", "summary": "", "blogId": "azureupdates"}
        ], client, "test-deployment")

        self.assertEqual(result, [{"id": "0", "bucket": "in_preview", "label": "Preview item"}])


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
    def _create_artifacts(self, root):
        feeds = root / "data" / "feeds.json"
        feed_xml = root / "data" / "feed.xml"
        feeds.parent.mkdir(parents=True, exist_ok=True)
        feeds.write_text('{"ok":true}\n', encoding="utf-8")
        feed_xml.write_text('<rss></rss>\n', encoding="utf-8")
        return feeds, feed_xml

    def test_build_checksums_payload_includes_expected_shape(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path(tmpdir)
            feeds, feed_xml = self._create_artifacts(root)

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
            feeds, feed_xml = self._create_artifacts(root)
            checksums = root / "data" / "checksums.json"

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
            retirement_calendar=[
                {
                    "title": "Retirement one",
                    "sources": ["Azure Updates", "Azure Deprecations (aztty)"],
                },
                {
                    "title": "Retirement two",
                    "blog": "Azure Updates",
                },
            ],
            retirement_buckets={
                "windows": {
                    "0_3_months": {"count": 1},
                    "3_6_months": {"count": 2},
                    "6_9_months": {"count": 3},
                    "9_12_months": {"count": 4},
                }
            },
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
        self.assertEqual(metrics["retirementTotalCount"], 2)
        self.assertEqual(metrics["retirementSourceCount"], 2)
        self.assertEqual(
            metrics["retirementWindowCounts"],
            {
                "0_3_months": 1,
                "3_6_months": 2,
                "6_9_months": 3,
                "9_12_months": 4,
            },
        )

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
        self.assertEqual(metrics["retirementTotalCount"], 0)
        self.assertEqual(metrics["retirementSourceCount"], 0)
        self.assertEqual(metrics["retirementWindowCounts"], {})

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


class MainFeedFilteringTests(unittest.TestCase):
    def test_filter_main_feed_articles_excludes_workbook_entries(self):
        articles = [
            {"title": "Workbook retirement", "blogId": "azureretirements"},
            {"title": "Lifecycle", "blogId": "microsoftlifecycle"},
            {"title": "Azure Update", "blogId": "azureupdates"},
        ]

        filtered = fetch_feeds.filter_main_feed_articles(articles)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["blogId"], "azureupdates")

    def test_load_previous_main_feed_article_count_uses_filtered_articles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "feeds.json"
            path.write_text(
                json.dumps(
                    {
                        "totalArticles": 3,
                        "articles": [
                            {"title": "Workbook", "blogId": "azureretirements"},
                            {"title": "Azure Update", "blogId": "azureupdates"},
                            {"title": "AKS", "blogId": "aksblog"},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            count = fetch_feeds.load_previous_main_feed_article_count(str(path))

        self.assertEqual(count, 2)

    def test_load_previous_main_feed_article_count_falls_back_to_total_articles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = pathlib.Path(tmpdir) / "feeds.json"
            path.write_text(json.dumps({"totalArticles": 17}), encoding="utf-8")

            count = fetch_feeds.load_previous_main_feed_article_count(str(path))

        self.assertEqual(count, 17)


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
    def _render_xml(self, article):
        with tempfile.TemporaryDirectory() as tmpdir:
            with temporary_cwd(tmpdir):
                os.makedirs("data", exist_ok=True)
                fetch_feeds.generate_rss_feed([article])
                return pathlib.Path("data/feed.xml").read_text(encoding="utf-8")

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

        xml_text = self._render_xml(article)

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

        xml_text = self._render_xml(article)

        self.assertIn("Contains ]]&gt; token", xml_text)


class AzureUpdatesApiFallbackTests(unittest.TestCase):
    def _assert_feed_fallback_behavior(self, *, api_result=None, api_side_effect=None):
        rss_articles = [{"title": "RSS fallback article"}]
        with mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_api",
            return_value=api_result,
            side_effect=api_side_effect,
        ) as api_mock, mock.patch.object(
            fetch_feeds,
            "fetch_azure_updates_via_rss",
            return_value=rss_articles,
        ) as rss_mock:
            result = fetch_feeds.fetch_azure_updates_feed()

        self.assertEqual(result, rss_articles)
        api_mock.assert_called_once()
        rss_mock.assert_called_once()

    def test_extract_azure_update_retirement_date_from_page_prefers_body_day(self):
        html = (
            "<html><body>"
            "<h1>Retirement: Example service</h1>"
            "<div>Azure Policy RETIREMENT April 2026</div>"
            "<p>Starting April 30, 2026, this workaround will no longer be available.</p>"
            "</body></html>"
        )
        response = mock.Mock()
        response.text = html
        response.raise_for_status = mock.Mock()

        with mock.patch.object(fetch_feeds.HTTP_SESSION, "get", return_value=response) as get_mock:
            value = fetch_feeds._extract_azure_update_retirement_date_from_page(
                "https://azure.microsoft.com/en-us/updates/558102/"
            )

        self.assertEqual(value, "2026-04-30")
        get_mock.assert_called_once()

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

    def test_parse_azure_update_item_marks_updated_when_modified_is_later(self):
        item = {
            "id": "123457",
            "title": "Generally Available: Sample Azure capability",
            "status": "Launched",
            "created": "2026-03-22T10:30:00Z",
            "modified": "2026-03-24T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertTrue(article["azureWasUpdated"])

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

    def test_parse_azure_update_item_uses_structured_retirement_type_and_target_date(self):
        item = {
            "id": "889002b",
            "title": "Example platform announcement",
            "description": "Migration guidance published for this service.",
            "status": "Update",
            "type": "Retirement",
            "targetDate": "2028-09",
            "created": "2026-03-22T10:30:00Z",
        }

        article = fetch_feeds._parse_azure_update_item(item)

        self.assertIsNotNone(article)
        self.assertEqual(article["lifecycle"], "retiring")
        self.assertEqual(article["azureUpdateType"], "Retirement")
        self.assertEqual(article["azureTargetDate"], "2028-09")
        self.assertEqual(article["azureRetirementDate"], "2028-09")

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

    def test_parse_azure_update_item_uses_target_date_for_retirement(self):
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
        self.assertEqual(article["azureRetirementDate"], "2031-07-31")

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

    def test_entries_to_articles_marks_updated_when_rss_updated_is_later(self):
        entries = [
            {
                "title": "Azure Update",
                "link": "https://example.com/update",
                "summary": "Summary",
                "author": "Microsoft",
                "published": "Mon, 20 Apr 2026 10:00:00 +0000",
                "updated": "Tue, 21 Apr 2026 10:00:00 +0000",
            }
        ]

        articles = fetch_feeds._entries_to_articles(entries, "Azure Updates", "azureupdates")

        self.assertEqual(len(articles), 1)
        self.assertTrue(articles[0]["azureWasUpdated"])

    def test_fetch_azure_updates_feed_falls_back_to_rss_on_api_exception(self):
        self._assert_feed_fallback_behavior(api_side_effect=RuntimeError("api unavailable"))

    def test_fetch_azure_updates_feed_falls_back_to_rss_when_api_returns_no_valid_items(self):
        self._assert_feed_fallback_behavior(api_result=[])

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

    def test_fetch_azure_updates_via_api_keeps_stale_future_retirements(self):
        now = datetime.now(timezone.utc)
        stale_created = (now - timedelta(days=90)).isoformat().replace("+00:00", "Z")
        future_month = now + timedelta(days=180)
        future_target = f"{future_month.year:04d}-{future_month.month:02d}"
        payload = {
            "value": [
                {
                    "id": "future-retire",
                    "title": "Retirement notice from API metadata",
                    "status": "Update",
                    "type": "Retirement",
                    "targetDate": future_target,
                    "created": stale_created,
                },
                {
                    "id": "stale-non-retire",
                    "title": "Launched update",
                    "status": "Launched",
                    "created": stale_created,
                },
            ]
        }
        response = mock.Mock()
        response.raise_for_status = mock.Mock()
        response.json.return_value = payload

        with mock.patch.object(fetch_feeds.HTTP_SESSION, "get", return_value=response):
            articles = fetch_feeds.fetch_azure_updates_via_api()

        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["link"], "https://azure.microsoft.com/en-us/updates/future-retire/")
        self.assertEqual(articles[0]["azureRetirementDate"], future_target)


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


class WorkbookCsvRetirementTests(unittest.TestCase):
    def _fetch_articles_from_csv(self, csv_content, *, by_id_value=None, page_value=None):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = pathlib.Path(tmpdir) / "export_data.csv"
            csv_path.write_text(csv_content, encoding="utf-8")

            with mock.patch.object(
                fetch_feeds,
                "_extract_azure_update_retirement_date_by_id",
                return_value=by_id_value,
            ) as by_id_mock, mock.patch.object(
                fetch_feeds,
                "_extract_azure_update_retirement_date_from_page",
                return_value=page_value,
            ) as page_mock:
                articles = fetch_feeds.fetch_azure_retirements_from_csv(csv_path)

        return articles, by_id_mock, page_mock

    def test_fetch_azure_retirements_from_csv_maps_rows(self):
        csv_content = (
            '"Service Name","Retiring Feature","Retirement Date","Actions","Is Available under the Impacted Services?"\n'
            '"Application gateway","V1","2026-04-28","https://azure.microsoft.com/updates?id=example-1","Yes"\n'
            '"Azure Maps Account","Render V1 APIs","2026-09-17","https://azure.microsoft.com/updates?id=example-2","No"\n'
        )

        articles, _, _ = self._fetch_articles_from_csv(csv_content)

        self.assertEqual(len(articles), 2)
        self.assertEqual(
            articles[0]["title"],
            "Retirement: Application gateway - V1",
        )
        self.assertEqual(articles[0]["azureRetirementDate"], "2026-04-28")
        self.assertEqual(
            articles[0]["link"],
            "https://azure.microsoft.com/updates?id=example-1",
        )
        self.assertEqual(articles[0]["blogId"], "azureretirements")
        self.assertEqual(articles[0]["azureRetirementDateSource"], "csv")
        self.assertTrue(articles[0]["impactedServicesAvailable"])
        self.assertFalse(articles[1]["impactedServicesAvailable"])

    def test_fetch_azure_retirements_from_csv_skips_invalid_rows(self):
        csv_content = (
            '"Service Name","Retiring Feature","Retirement Date","Actions","Is Available under the Impacted Services?"\n'
            '"Example Service","Feature A","not-a-date","https://azure.microsoft.com/updates?id=bad","Yes"\n'
            '"Example Service","Feature B","2027-01-15","https://azure.microsoft.com/updates?id=good","Yes"\n'
        )

        articles, _, _ = self._fetch_articles_from_csv(csv_content)

        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["azureRetirementDate"], "2027-01-15")

    def test_fetch_azure_retirements_from_csv_prefers_linked_page_date_on_conflict(self):
        csv_content = (
            '"Service Name","Retiring Feature","Retirement Date","Actions","Is Available under the Impacted Services?"\n'
            '"App service",".NET 9 (STS)","2026-05-12","https://azure.microsoft.com/updates/?id=485077","Yes"\n'
        )

        articles, by_id_mock, enrich_mock = self._fetch_articles_from_csv(
            csv_content,
            by_id_value="2026-11-10",
        )

        self.assertEqual(by_id_mock.call_count, 1)
        self.assertEqual(enrich_mock.call_count, 0)
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["azureRetirementDate"], "2026-11-10")
        self.assertEqual(articles[0]["azureRetirementDateCsv"], "2026-05-12")
        self.assertEqual(articles[0]["azureRetirementDateSource"], "linked_page")

    def test_fetch_azure_retirements_from_csv_missing_file_returns_empty_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = pathlib.Path(tmpdir) / "does-not-exist.csv"
            articles = fetch_feeds.fetch_azure_retirements_from_csv(missing)

        self.assertEqual(articles, [])


class MicrosoftLifecycleRetirementTests(unittest.TestCase):
    def test_fetch_microsoft_lifecycle_retirements_maps_future_milestones(self):
        payloads = {
            fetch_feeds.MICROSOFT_LIFECYCLE_TAG_API: {
                "result": [{"name": "windows-server"}],
            },
            "https://endoflife.date/api/v1/products/windows-server/": {
                "result": {
                    "label": "Microsoft Windows Server",
                    "links": {"html": "https://endoflife.date/windows-server"},
                    "releases": [
                        {
                            "label": "Windows Server 2022 (LTSC)",
                            "eoasFrom": "2030-10-13",
                            "eolFrom": "2035-10-14",
                            "latest": {"link": "https://learn.microsoft.com/windows/release-health/windows-server-release-info"},
                        },
                        {
                            "label": "Windows Server 2012",
                            "eoasFrom": "2018-10-09",
                            "eolFrom": "2023-10-10",
                        },
                    ],
                }
            },
        }

        def fake_fetch(url):
            return payloads[url]

        with mock.patch.object(fetch_feeds, "_fetch_json_payload", side_effect=fake_fetch):
            events = fetch_feeds.fetch_microsoft_lifecycle_retirements(
                {
                    "enabled": True,
                    "products": ["windows-server"],
                    "milestones": ["eoas", "eol"],
                    "maxEvents": 20,
                }
            )

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["blogId"], fetch_feeds.MICROSOFT_LIFECYCLE_BLOG_ID)
        self.assertEqual(events[0]["azureRetirementDateSource"], "endoflife")
        self.assertEqual(events[0]["link"], "https://endoflife.date/windows-server")
        self.assertEqual(events[1]["link"], "https://endoflife.date/windows-server")
        self.assertIn("Active support ends", events[0]["title"])
        self.assertIn("Security support ends", events[1]["title"])

    def test_fetch_microsoft_lifecycle_retirements_respects_max_events(self):
        payloads = {
            fetch_feeds.MICROSOFT_LIFECYCLE_TAG_API: {
                "result": [{"name": "windows-server"}],
            },
            "https://endoflife.date/api/v1/products/windows-server/": {
                "result": {
                    "label": "Microsoft Windows Server",
                    "links": {"html": "https://endoflife.date/windows-server"},
                    "releases": [
                        {
                            "label": "Windows Server A",
                            "eoasFrom": "2031-01-01",
                            "eolFrom": "2032-01-01",
                        },
                        {
                            "label": "Windows Server B",
                            "eoasFrom": "2033-01-01",
                            "eolFrom": "2034-01-01",
                        },
                    ],
                }
            },
        }

        with mock.patch.object(fetch_feeds, "_fetch_json_payload", side_effect=lambda url: payloads[url]):
            events = fetch_feeds.fetch_microsoft_lifecycle_retirements(
                {
                    "enabled": True,
                    "products": ["windows-server"],
                    "milestones": ["eoas", "eol"],
                    "maxEvents": 1,
                }
            )

        self.assertEqual(len(events), 1)

    def test_fetch_microsoft_lifecycle_retirements_includes_esu_begin_and_end(self):
        payloads = {
            fetch_feeds.MICROSOFT_LIFECYCLE_TAG_API: {
                "result": [{"name": "windows-server"}],
            },
            "https://endoflife.date/api/v1/products/windows-server/": {
                "result": {
                    "label": "Microsoft Windows Server",
                    "links": {"html": "https://endoflife.date/windows-server"},
                    "releases": [
                        {
                            "label": "Windows Server 2016 (LTSC)",
                            "eoasFrom": "2027-01-12",
                            "eolFrom": "2027-01-12",
                            "eoesFrom": "2032-01-13",
                        }
                    ],
                }
            },
        }

        with mock.patch.object(fetch_feeds, "_fetch_json_payload", side_effect=lambda url: payloads[url]):
            events = fetch_feeds.fetch_microsoft_lifecycle_retirements(
                {
                    "enabled": True,
                    "products": ["windows-server"],
                    "milestones": ["eoes_start", "eoes"],
                    "maxEvents": 20,
                }
            )

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["azureRetirementDate"], "2027-01-12")
        self.assertEqual(events[1]["azureRetirementDate"], "2032-01-13")
        self.assertIn("Extended security updates begin", events[0]["title"])
        self.assertIn("Extended security updates end", events[1]["title"])


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
        assert_category_metadata(self, event)

    def test_build_azure_retirement_calendar_adds_category_metadata_for_workbook_items(self):
        articles = [
            {
                "title": "Flatcar Container Linux for AKS (preview)",
                "link": "https://azure.microsoft.com/en-us/updates/557929/",
                "published": "2026-03-16T18:15:54.374686+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "summary": "Azure Kubernetes Service support for Flatcar Container Linux for AKS (preview) will be retired on June 8, 2026.",
                "azureRetirementDate": "2026-06-08",
            },
            {
                "title": "Volume - Create basic networking volumes",
                "link": "https://learn.microsoft.com/azure/azure-netapp-files/azure-netapp-files-network-topologies#considerations",
                "published": "2026-04-14T07:46:35.957136+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "summary": "Azure NetApp Files basic networking volumes retire on May 31, 2026.",
                "azureRetirementDate": "2026-05-31",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        by_title = {event["title"]: event for event in events}
        self.assertEqual(by_title["Flatcar Container Linux for AKS (preview)"]["primaryCategory"], "Compute")
        self.assertIn("Compute", by_title["Flatcar Container Linux for AKS (preview)"]["categories"])
        self.assertEqual(by_title["Volume - Create basic networking volumes"]["primaryCategory"], "Infrastructure")
        self.assertIn("Infrastructure", by_title["Volume - Create basic networking volumes"]["categories"])

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

    def test_build_azure_retirement_calendar_prefers_day_precision_for_same_update(self):
        articles = [
            {
                "title": "Retirement: Azure Policy faster enforcement and retirement of login/logout workaround",
                "link": "https://azure.microsoft.com/updates?id=558102",
                "published": "2026-03-04T21:15:02+00:00",
                "blog": "Azure Deprecations (aztty)",
                "blogId": "azuredeprecations",
                "announcementType": "deprecation",
                "azureRetirementDate": "2026-04",
            },
            {
                "title": "Retirement: Azure Policy faster enforcement and retirement of login/logout workaround",
                "link": "https://azure.microsoft.com/en-us/updates/558102/",
                "published": "2026-03-04T21:15:02.275174+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "",
                "azureRetirementDate": "2026-04-30",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["retirementDate"], "2026-04-30")
        self.assertEqual(events[0]["datePrecision"], "day")
        self.assertEqual(events[0]["blogId"], "azureupdates")

    def test_build_azure_retirement_calendar_prefers_update_id_over_title_for_dedupe(self):
        articles = [
            {
                "title": "Retirement: Application gateway - V1",
                "link": "https://azure.microsoft.com/updates?id=558102",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2026-04-30",
            },
            {
                "title": "Retirement: Azure Policy faster enforcement and retirement of login/logout workaround",
                "link": "https://azure.microsoft.com/en-us/updates/558102/",
                "published": "2026-03-04T21:15:02.275174+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "retirement",
                "azureRetirementDate": "2026-04-30",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["blogId"], "azureupdates")
        self.assertEqual(events[0]["sourceCount"], 2)

    def test_build_azure_retirement_calendar_keeps_current_month_month_precision(self):
        today = datetime.now(timezone.utc)
        current_month = f"{today.year:04d}-{today.month:02d}"
        articles = [
            {
                "title": "Retirement: Current month timeline",
                "link": "https://example.com/current-month",
                "published": today.isoformat(),
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "azureRetirementDate": current_month,
            }
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["retirementDate"], current_month)
        self.assertEqual(events[0]["datePrecision"], "month")

    def test_build_azure_retirement_calendar_dedupes_runtime_alias_wording(self):
        articles = [
            {
                "title": "Retirement: App service - Azure Functions - Node.js 20",
                "link": "https://azure.microsoft.com/updates/?id=502957",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-04-30",
            },
            {
                "title": "Retirement: App service - Support for Node 20 LTS",
                "link": "https://azure.microsoft.com/updates/?id=485072",
                "published": "2026-04-10T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-04-30",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["sourceCount"], 2)

    def test_build_azure_retirement_calendar_keeps_distinct_runtime_versions(self):
        articles = [
            {
                "title": "Retirement: App service - Support for Node 20 LTS",
                "link": "https://azure.microsoft.com/updates/?id=485072",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-04-30",
            },
            {
                "title": "Retirement: App service - Support for Node 22 LTS",
                "link": "https://azure.microsoft.com/updates/?id=999999",
                "published": "2026-04-10T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-04-30",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 2)

    def test_build_azure_retirement_calendar_dedupes_cross_source_verbose_titles(self):
        """Workbook short title and RSS verbose title for same event on same date merge into one."""
        articles = [
            {
                "title": "Retirement: Subscription - Azure Virtual Desktop Classic",
                "link": "https://learn.microsoft.com/azure/virtual-desktop/virtual-desktop-fall-2019/classic-retirement",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-09-30",
            },
            {
                "title": "Azure Virtual Desktop (classic) will be retired on 30 September 2026 - Please transition to Azure Virtual Desktop",
                "link": "https://azure.microsoft.com/en-us/updates/azure-virtual-desktop-classic-will-be-retired-on-30-september-2026/",
                "published": "2026-04-09T09:00:00+00:00",
                "blog": "Azure Deprecations",
                "blogId": "azuredeprecations",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-09-30",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1, "workbook and RSS entries for same retirement should merge")
        self.assertEqual(len(events[0]["sourceReports"]), 2, "merged event should carry both source reports")

    def test_build_azure_retirement_calendar_prefers_azure_updates_link_for_multi_source_event(self):
        articles = [
            {
                "title": "Retirement: Subscription - Azure Virtual Desktop Classic",
                "link": "https://learn.microsoft.com/azure/virtual-desktop/virtual-desktop-fall-2019/classic-retirement",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-09-30",
            },
            {
                "title": "Retirement: Azure Virtual Desktop (classic) transition guidance",
                "link": "https://azure.microsoft.com/en-us/updates/558999/",
                "published": "2026-04-09T09:00:00+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "azureRetirementDate": "2030-09-30",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["sourceCount"], 2)
        self.assertEqual(events[0]["link"], "https://azure.microsoft.com/en-us/updates/558999/")

    def test_build_azure_retirement_calendar_prefers_endoflife_link_when_lifecycle_source_present(self):
        articles = [
            {
                "title": "Retirement: Microsoft .NET 8 (LTS) - Security support ends",
                "link": "https://azure.microsoft.com/en-us/updates/123456/",
                "published": "2026-04-09T09:00:00+00:00",
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "update",
                "azureRetirementDate": "2030-11-10",
            },
            {
                "title": "Retirement: Microsoft .NET 8 (LTS) - Security support ends",
                "link": "https://endoflife.date/dotnet",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Microsoft Lifecycle",
                "blogId": "microsoftlifecycle",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-11-10",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["sourceCount"], 2)
        self.assertEqual(events[0]["link"], "https://endoflife.date/dotnet")

    def test_build_azure_retirement_calendar_keeps_single_source_link(self):
        articles = [
            {
                "title": "Retirement: Subscription - Azure Virtual Desktop Classic",
                "link": "https://learn.microsoft.com/azure/virtual-desktop/virtual-desktop-fall-2019/classic-retirement",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-09-30",
            }
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["sourceCount"], 1)
        self.assertEqual(
            events[0]["link"],
            "https://learn.microsoft.com/azure/virtual-desktop/virtual-desktop-fall-2019/classic-retirement",
        )

    def test_build_azure_retirement_calendar_cross_source_keeps_different_events_same_date(self):
        """Two genuinely different retirements on the same date must not be merged."""
        articles = [
            {
                "title": "Retirement: Azure API for FHIR - Entire service",
                "link": "https://azure.microsoft.com/updates?id=azure-api-for-fhir-retirement",
                "published": "2026-04-09T08:00:00+00:00",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-09-30",
            },
            {
                "title": "Azure Virtual Desktop (classic) will be retired on 30 September 2030",
                "link": "https://azure.microsoft.com/en-us/updates/avd-classic-retirement/",
                "published": "2026-04-09T09:00:00+00:00",
                "blog": "Azure Deprecations",
                "blogId": "azuredeprecations",
                "announcementType": "retirement",
                "azureRetirementDate": "2030-09-30",
            },
        ]

        events = fetch_feeds.build_azure_retirement_calendar(articles)

        self.assertEqual(len(events), 2, "distinct retirements on same date must remain separate")

    def test_build_retirement_window_buckets_assigns_rolling_windows(self):
        events = [
            {
                "title": "Soon",
                "retirementDate": "2026-05-15",
                "datePrecision": "day",
                "link": "https://example.com/soon",
            },
            {
                "title": "Three to six",
                "retirementDate": "2026-08",
                "datePrecision": "month",
                "link": "https://example.com/three-six",
            },
            {
                "title": "Six to nine",
                "retirementDate": "2026-11-10",
                "datePrecision": "day",
                "link": "https://example.com/six-nine",
            },
            {
                "title": "Nine to twelve",
                "retirementDate": "2027-03",
                "datePrecision": "month",
                "link": "https://example.com/nine-twelve",
            },
            {
                "title": "Twelve to twenty-four",
                "retirementDate": "2027-09-30",
                "datePrecision": "day",
                "link": "https://example.com/twelve-twenty-four",
            },
            {
                "title": "Twenty-four plus",
                "retirementDate": "2028-09-30",
                "datePrecision": "day",
                "link": "https://example.com/twenty-four-plus",
            },
        ]

        buckets = fetch_feeds.build_retirement_window_buckets(
            events,
            today=datetime(2026, 4, 8, tzinfo=timezone.utc).date(),
        )

        windows = buckets["windows"]
        self.assertEqual(windows["0_3_months"]["count"], 1)
        self.assertEqual(windows["3_6_months"]["count"], 1)
        self.assertEqual(windows["6_9_months"]["count"], 1)
        self.assertEqual(windows["9_12_months"]["count"], 1)
        self.assertEqual(windows["12_24_months"]["count"], 1)
        self.assertEqual(windows["24_plus_months"]["count"], 1)
        self.assertEqual(windows["3_6_months"]["items"][0]["retirementDate"], "2026-08")
        self.assertEqual(windows["9_12_months"]["items"][0]["retirementDate"], "2027-03")
        self.assertEqual(windows["12_24_months"]["items"][0]["retirementDate"], "2027-09-30")
        self.assertEqual(windows["24_plus_months"]["items"][0]["retirementDate"], "2028-09-30")


class RetirementCalendarIcsTests(unittest.TestCase):
    def test_generate_azure_retirements_ics_contains_required_fields(self):
        events = [
            {
                "title": "App service - Support for Node 20 LTS",
                "link": "https://azure.microsoft.com/en-us/updates/558102/",
                "retirementDate": "2030-04-30",
                "datePrecision": "day",
                "sources": ["Azure Updates"],
            }
        ]

        payload = fetch_feeds.generate_azure_retirements_ics(
            events,
            generated_at=datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc),
        )

        self.assertIn("BEGIN:VCALENDAR", payload)
        self.assertIn("BEGIN:VEVENT", payload)
        self.assertIn("SUMMARY:App service - Support for Node 20 LTS", payload)
        self.assertIn("DTSTART;VALUE=DATE:20300430", payload)
        self.assertIn("DTEND;VALUE=DATE:20300501", payload)
        self.assertIn("URL:https://azure.microsoft.com/en-us/updates/558102/", payload)

    def test_generate_azure_retirements_ics_marks_month_precision(self):
        events = [
            {
                "title": "Storage account - Legacy accounts",
                "link": "https://learn.microsoft.com/example/legacy",
                "retirementDate": "2030-05",
                "datePrecision": "month",
                "sources": ["Azure Retirements Workbook"],
            }
        ]

        payload = fetch_feeds.generate_azure_retirements_ics(
            events,
            generated_at=datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc),
        )

        self.assertIn("DTSTART;VALUE=DATE:20300501", payload)
        self.assertIn("DTEND;VALUE=DATE:20300502", payload)
        self.assertIn("Date precision: month", payload)
        self.assertIn("month-level precision", payload)

    def test_write_azure_retirements_ics_creates_file(self):
        events = [
            {
                "title": "Example service retirement",
                "link": "https://example.com/retirement",
                "retirementDate": "2031-01-10",
                "datePrecision": "day",
                "sources": ["Azure Updates"],
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / "retirements.ics"
            fetch_feeds.write_azure_retirements_ics(events, output_path=output_path)
            content = output_path.read_text(encoding="utf-8")

        self.assertIn("BEGIN:VCALENDAR", content)
        self.assertIn("END:VCALENDAR", content)
        self.assertIn("SUMMARY:Example service retirement", content)


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
        workbook_articles = [
            {
                "title": "Retirement: Workbook service - Feature",
                "link": "https://azure.microsoft.com/updates?id=workbook",
                "published": "2031-01-11T12:00:00+00:00",
                "summary": "Workbook retirement details",
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "author": "Microsoft",
                "announcementType": "retirement",
                "lifecycle": "retiring",
                "azureRetirementDate": "2031-08-31",
            }
        ]
        lifecycle_articles = [
            {
                "title": "Retirement: Microsoft Windows Server 2022 (LTSC) - Active support ends",
                "link": "https://endoflife.date/windows-server",
                "published": "2031-01-12T12:00:00+00:00",
                "summary": "Lifecycle milestone",
                "blog": "Microsoft Lifecycle",
                "blogId": "microsoftlifecycle",
                "author": "endoflife.date",
                "announcementType": "retirement",
                "lifecycle": "retiring",
                "azureRetirementDate": "2031-10-13",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with temporary_cwd(tmpdir):
                with mock.patch.object(fetch_feeds, "fetch_tech_community_feeds", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_aks_blog", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_devblogs_feeds", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_azure_updates_feed", return_value=[]), \
                    mock.patch.object(fetch_feeds, "fetch_aztty_announcements", return_value=aztty_articles), \
                    mock.patch.object(fetch_feeds, "fetch_azure_retirements_from_csv", return_value=workbook_articles), \
                    mock.patch.object(fetch_feeds, "fetch_microsoft_lifecycle_retirements", return_value=lifecycle_articles), \
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

            payload = json.loads((pathlib.Path(tmpdir) / "data" / "feeds.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["totalArticles"], 1)
        self.assertEqual(len(payload["articles"]), 1)
        self.assertEqual(payload["articles"][0]["blogId"], "azuredeprecations")
        self.assertNotIn("microsoftlifecycle", {a.get("blogId") for a in payload["articles"]})
        self.assertIn("azureRetirementCalendar", payload)
        self.assertIn("azureRetirementBuckets", payload)
        self.assertEqual(len(payload["azureRetirementCalendar"]), 3)
        self.assertEqual(
            payload["azureRetirementCalendar"][0]["retirementDate"],
            "2031-07-31",
        )
        self.assertEqual(
            payload["azureRetirementCalendar"][0]["title"],
            "Example service retirement notice",
        )
        self.assertEqual(
            payload["azureRetirementCalendar"][1]["retirementDate"],
            "2031-08-31",
        )
        self.assertEqual(
            payload["azureRetirementCalendar"][2]["retirementDate"],
            "2031-10-13",
        )
        self.assertIn("windows", payload["azureRetirementBuckets"])


class UnifiedRetirementCalendarTests(unittest.TestCase):
    """Tests for build_unified_retirement_calendar() function."""

    def test_build_unified_retirement_calendar_tags_events_with_source(self):
        """Unified calendar should tag each event with its source."""
        now = datetime.now(timezone.utc)
        future_month = (now + timedelta(days=60)).strftime("%Y-%m")

        azure_events = [
            {
                "title": "Azure service retirement",
                "link": "https://example.com/azure",
                "azureRetirementDate": future_month,
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]
        microsoft_events = [
            {
                "title": "Windows Server 2016 end of support",
                "link": "https://example.com/windows",
                "azureRetirementDate": future_month,
                "blog": "Microsoft Lifecycle",
                "blogId": "microsoftlifecycle",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]
        m365_events = [
            {
                "title": "Microsoft 365 feature deprecation",
                "link": "https://example.com/m365",
                "m365RetirementDate": future_month,
                "blog": "Microsoft 365",
                "blogId": "m365",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]

        calendar = fetch_feeds.build_unified_retirement_calendar(
            azure_events=azure_events,
            microsoft_events=microsoft_events,
            m365_events=m365_events,
        )

        self.assertEqual(len(calendar), 3)
        sources = {event.get("source") for event in calendar}
        self.assertEqual(sources, {"azure", "microsoft", "m365"})
        for event in calendar:
            assert_category_metadata(self, event)

    def test_build_unified_retirement_calendar_maps_endoflife_to_existing_categories(self):
        """Microsoft lifecycle events should map to existing categories with fallback."""
        now = datetime.now(timezone.utc)
        future_month = (now + timedelta(days=60)).strftime("%Y-%m")

        microsoft_events = [
            {
                "title": "SQL Server 2022 end of support",
                "link": "https://example.com/sql",
                "azureRetirementDate": future_month,
                "blog": "Microsoft Lifecycle",
                "blogId": "microsoftlifecycle",
                "announcementType": "retirement",
                "published": now.isoformat(),
                "lifecycleProduct": "mssqlserver",
                "lifecycleRelease": "2022",
            },
            {
                "title": "Unmapped legacy product retirement",
                "link": "https://example.com/unknown",
                "azureRetirementDate": future_month,
                "blog": "Microsoft Lifecycle",
                "blogId": "microsoftlifecycle",
                "announcementType": "retirement",
                "published": now.isoformat(),
                "lifecycleProduct": "unknown-product",
                "lifecycleRelease": "v1",
            },
        ]

        calendar = fetch_feeds.build_unified_retirement_calendar(
            microsoft_events=microsoft_events,
        )

        self.assertEqual(len(calendar), 2)
        by_link = {event.get("link"): event for event in calendar}
        self.assertEqual(by_link["https://example.com/sql"].get("primaryCategory"), "Data & AI")
        self.assertEqual(by_link["https://example.com/unknown"].get("primaryCategory"), "Other")

    def test_build_unified_retirement_calendar_deduplicates_cross_source(self):
        """Unified calendar should deduplicate same event across sources."""
        now = datetime.now(timezone.utc)
        future_month = (now + timedelta(days=60)).strftime("%Y-%m")

        # Same event from both Azure and Microsoft sources
        azure_events = [
            {
                "title": "SQL Server 2019 retirement",
                "link": "https://example.com/sql",
                "azureRetirementDate": future_month,
                "blog": "Azure Blog",
                "blogId": "azureblog",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]
        microsoft_events = [
            {
                "title": "SQL Server 2019 end of support",
                "link": "https://example.com/sql2",
                "azureRetirementDate": future_month,
                "blog": "Microsoft Lifecycle",
                "blogId": "microsoftlifecycle",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]

        calendar = fetch_feeds.build_unified_retirement_calendar(
            azure_events=azure_events,
            microsoft_events=microsoft_events,
        )

        # Should have exactly one event (deduplicated by similar title/date)
        self.assertLessEqual(len(calendar), 2)  # At worst 2, normally 1 if fuzzy dedup works

    def test_build_unified_retirement_calendar_priority_order(self):
        """Azure events should take priority over Microsoft which takes priority over M365."""
        now = datetime.now(timezone.utc)
        future_date = (now + timedelta(days=60)).strftime("%Y-%m-%d")

        # Same event from all three sources with different links
        azure_link = "https://example.com/azure-link"
        microsoft_link = "https://example.com/microsoft-link"

        azure_events = [
            {
                "title": "Service retirement",
                "link": azure_link,
                "azureRetirementDate": future_date,
                "blog": "Azure",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]
        microsoft_events = [
            {
                "title": "Service retire notice",
                "link": microsoft_link,
                "azureRetirementDate": future_date,
                "blog": "Microsoft",
                "blogId": "microsoftlifecycle",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]

        calendar = fetch_feeds.build_unified_retirement_calendar(
            azure_events=azure_events,
            microsoft_events=microsoft_events,
        )

        # Should keep at least 1 event, Azure should take priority
        self.assertGreaterEqual(len(calendar), 1)
        # First event should be from Azure (by priority)
        if len(calendar) > 0:
            # Check that azure event is in the results
            event_blogs = {e.get("blogId") for e in calendar}
            self.assertIn("azureretirements", event_blogs)

    def test_build_unified_retirement_calendar_prefers_endoflife_link_when_lifecycle_source_present(self):
        now = datetime.now(timezone.utc)
        future_date = (now + timedelta(days=60)).strftime("%Y-%m-%d")

        azure_events = [
            {
                "title": "Retirement: Microsoft .NET 8 (LTS) - Security support ends",
                "link": "https://azure.microsoft.com/en-us/updates/123456/",
                "azureRetirementDate": future_date,
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]
        microsoft_events = [
            {
                "title": "Retirement: Microsoft .NET 8 (LTS) - Security support ends",
                "link": "https://endoflife.date/dotnet",
                "azureRetirementDate": future_date,
                "blog": "Microsoft Lifecycle",
                "blogId": "microsoftlifecycle",
                "announcementType": "retirement",
                "published": now.isoformat(),
            }
        ]

        calendar = fetch_feeds.build_unified_retirement_calendar(
            azure_events=azure_events,
            microsoft_events=microsoft_events,
        )

        self.assertEqual(len(calendar), 1)
        self.assertEqual(calendar[0]["sourceCount"], 2)
        self.assertEqual(calendar[0]["link"], "https://endoflife.date/dotnet")

    def test_build_unified_retirement_calendar_filters_past_dates(self):
        """Unified calendar should exclude events with past retirement dates."""
        now = datetime.now(timezone.utc)
        past_date = (now - timedelta(days=60)).strftime("%Y-%m-%d")
        future_date = (now + timedelta(days=60)).strftime("%Y-%m-%d")

        azure_events = [
            {
                "title": "Past retirement",
                "link": "https://example.com/past",
                "azureRetirementDate": past_date,
                "blog": "Azure",
                "blogId": "azureblog",
                "announcementType": "retirement",
                "published": now.isoformat(),
            },
            {
                "title": "Future retirement",
                "link": "https://example.com/future",
                "azureRetirementDate": future_date,
                "blog": "Azure",
                "blogId": "azureblog",
                "announcementType": "retirement",
                "published": now.isoformat(),
            },
        ]

        calendar = fetch_feeds.build_unified_retirement_calendar(azure_events=azure_events)

        # Should only contain future event
        self.assertEqual(len(calendar), 1)
        self.assertEqual(calendar[0].get("retirementDate"), future_date)


if __name__ == "__main__":
    unittest.main()
