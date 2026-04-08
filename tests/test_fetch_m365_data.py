#!/usr/bin/env python3
"""
Unit Tests for Microsoft 365 Data Feed Fetcher
Tests mirrors test_fetch_feeds.py patterns for consistency.
"""

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

import fetch_m365_data


class NormalizeUrlTests(unittest.TestCase):
    """Test URL normalization for M365 items."""
    
    def test_normalizes_deltapulse_dashboard_url(self):
        """DeltaPulse URLs should normalize consistently."""
        url = "https://deltapulse.app/item/MC1255714?utm_source=feed"
        normalized = fetch_m365_data.normalize_url(url)
        # Tracking param should be removed
        self.assertNotIn("utm_source", normalized)
        self.assertIn("item/MC1255714", normalized)
    
    def test_removes_www_prefix(self):
        """www. prefix should be removed."""
        url = "https://www.deltapulse.app/item/MC1255714"
        normalized = fetch_m365_data.normalize_url(url)
        self.assertEqual(normalized, fetch_m365_data.normalize_url("https://deltapulse.app/item/MC1255714"))
    
    def test_sorts_query_parameters(self):
        """Query parameters should be sorted for consistent dedup."""
        url1 = "https://deltapulse.app/dashboard?z=1&a=2"
        url2 = "https://deltapulse.app/dashboard?a=2&z=1"
        self.assertEqual(fetch_m365_data.normalize_url(url1), fetch_m365_data.normalize_url(url2))


class DedupeM365ArticlesTests(unittest.TestCase):
    """Test M365 article deduplication."""
    
    def test_dedupes_same_normalized_url(self):
        """Articles with same normalized URL should be deduplicated."""
        now = datetime.now(timezone.utc)
        published = (now - timedelta(days=1)).isoformat()
        
        articles = [
            {
                "title": "First version",
                "link": "https://deltapulse.app/item/MC123?utm_source=rss",
                "published": published,
            },
            {
                "title": "Different title same URL",
                "link": "https://deltapulse.app/item/MC123?gclid=abc",
                "published": published,
            },
            {
                "title": "Unique article",
                "link": "https://deltapulse.app/item/MC456",
                "published": published,
            },
        ]
        
        deduped = fetch_m365_data.dedupe_m365_articles(articles)
        self.assertEqual(len(deduped), 2)
        titles = [a["title"] for a in deduped]
        self.assertIn("First version", titles)
        self.assertIn("Unique article", titles)

    def test_keeps_distinct_message_center_ids(self):
        """Different message IDs should not be deduped together."""
        fresh = datetime.now(timezone.utc).isoformat()
        articles = [
            {
                "title": "Msg A",
                "link": "https://admin.microsoft.com/Adminportal/Home?#/MessageCenter/:/messages/MC100",
                "published": fresh,
                "m365Source": "message_center",
                "m365Id": "MC100",
            },
            {
                "title": "Msg B",
                "link": "https://admin.microsoft.com/Adminportal/Home?#/MessageCenter/:/messages/MC200",
                "published": fresh,
                "m365Source": "message_center",
                "m365Id": "MC200",
            },
        ]

        deduped = fetch_m365_data.dedupe_m365_articles(articles)
        self.assertEqual(len(deduped), 2)
    
    def test_discards_stale_articles(self):
        """Articles older than 30 days should be discarded."""
        now = datetime.now(timezone.utc)
        fresh = (now - timedelta(days=1)).isoformat()
        stale = (now - timedelta(days=31)).isoformat()

        articles = [
            {"title": "Recent", "link": "https://deltapulse.app/d1", "published": fresh},
            {"title": "Old", "link": "https://deltapulse.app/d2", "published": stale},
        ]

        deduped = fetch_m365_data.dedupe_m365_articles(articles)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["title"], "Recent")

    def test_major_change_kept_past_30_days(self):
        """Major change articles should survive past the 30-day cutoff (up to 90 days)."""
        now = datetime.now(timezone.utc)
        stale_for_normal = (now - timedelta(days=45)).isoformat()

        articles = [
            {
                "title": "Major Change",
                "link": "https://deltapulse.app/d3",
                "published": stale_for_normal,
                "m365IsMajorChange": True,
            },
            {
                "title": "Normal Old",
                "link": "https://deltapulse.app/d4",
                "published": stale_for_normal,
                "m365IsMajorChange": False,
            },
        ]

        deduped = fetch_m365_data.dedupe_m365_articles(articles)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["title"], "Major Change")

    def test_major_change_discarded_after_90_days(self):
        """Major change articles older than 90 days should still be discarded."""
        now = datetime.now(timezone.utc)
        very_old = (now - timedelta(days=91)).isoformat()

        articles = [
            {
                "title": "Ancient Major Change",
                "link": "https://deltapulse.app/d5",
                "published": very_old,
                "m365IsMajorChange": True,
            },
        ]

        deduped = fetch_m365_data.dedupe_m365_articles(articles)
        self.assertEqual(len(deduped), 0)


class ClassifyM365LifecycleTests(unittest.TestCase):
    """Test M365 item lifecycle classification."""
    
    def test_roadmap_in_development_status(self):
        """Roadmap items with 'in development' status should be classified."""
        item = {
            "source": "roadmap",
            "status": "In Development",
        }
        self.assertEqual(fetch_m365_data.classify_m365_lifecycle(item), "in_development")
    
    def test_roadmap_in_preview_status(self):
        """Roadmap items with 'in preview' status should be classified."""
        item = {
            "source": "roadmap",
            "status": "In Preview",
        }
        self.assertEqual(fetch_m365_data.classify_m365_lifecycle(item), "in_preview")
    
    def test_roadmap_prefers_releasephase_over_status(self):
        """classify_m365_lifecycle should prefer DeltaPulse releasePhase over status."""
        item = {
            "source": "roadmap",
            "status": "In Preview",       # would map to in_preview
            "releasePhase": "In Development",  # DeltaPulse value wins
        }
        self.assertEqual(fetch_m365_data.classify_m365_lifecycle(item), "in_development")

    def test_roadmap_m365status_not_set(self):
        """build_article_from_m365_item should not populate m365Status for roadmap items."""
        item = {
            "id": "12345",
            "title": "Roadmap feature",
            "source": "roadmap",
            "status": "In Development",
            "releasePhase": "In Development",
            "service": ["Teams"],
        }
        article = fetch_m365_data.build_article_from_m365_item(item)
        self.assertIsNone(article.get("m365Status"))

    def test_message_center_defaults_to_launched(self):
        """Message Center items should default to launched_ga."""
        item = {
            "source": "message_center",
            "severity": "normal",
        }
        self.assertEqual(fetch_m365_data.classify_m365_lifecycle(item), "launched_ga")


class BuildArticleFromM365ItemTests(unittest.TestCase):
    """Test conversion from M365 item to article schema."""
    
    def test_converts_message_center_item(self):
        """Message Center items should convert properly."""
        item = {
            "id": "MC1255714",
            "title": "New feature announcement",
            "source": "message_center",
            "publishedDate": "2026-03-19T04:39:14.000Z",
            "service": ["Teams", "SharePoint"],
            "category": "stayInformed",
            "url": "https://deltapulse.app/item/MC1255714",
        }
        
        article = fetch_m365_data.build_article_from_m365_item(item)
        
        self.assertEqual(article["title"], "New feature announcement")
        self.assertEqual(
            article["link"],
            "https://deltapulse.app/item/MC1255714",
        )
        self.assertEqual(article["source"], "m365")
        self.assertEqual(article["m365Service"], "Teams")
        self.assertEqual(article["m365Id"], "MC1255714")
        self.assertEqual(article["m365AllServices"], ["Teams", "SharePoint"])
        self.assertIsNotNone(article["lifecycle"])

    def test_overrides_deltapulse_message_center_link(self):
        """Message Center items should resolve to DeltaPulse URL when available."""
        item = {
            "id": "MC999999",
            "title": "Admin portal update",
            "source": "message_center",
            "publishedDate": "2026-03-19T04:39:14.000Z",
            "service": ["Teams"],
            "url": "https://deltapulse.app/item/MC999999",
            "detailsUrl": "https://admin.microsoft.com/Adminportal/Home?#/MessageCenter/:/messages/MC999999",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)

        self.assertEqual(
            article["link"],
            "https://deltapulse.app/item/MC999999",
        )

    def test_message_center_falls_back_to_item_page(self):
        """Message Center items without URL should fall back to DeltaPulse dashboard search."""
        item = {
            "id": "MC888888",
            "title": "Missing URL update",
            "source": "message_center",
            "publishedDate": "2026-03-19T04:39:14.000Z",
            "service": ["Teams"],
        }

        article = fetch_m365_data.build_article_from_m365_item(item)
        self.assertEqual(
            article["link"],
            "https://deltapulse.app/item/MC888888",
        )

    def test_roadmap_item_gets_deltapulse_url(self):
        """Roadmap items should link to the DeltaPulse card, not the M365 roadmap page."""
        item = {
            "id": "558435",
            "title": "Security Update Alerts",
            "source": "roadmap",
            "status": "In development",
            "service": ["Microsoft 365"],
            "url": "https://deltapulse.app/item/558435",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)

        self.assertEqual(
            article["link"],
            "https://deltapulse.app/item/558435",
        )

    def test_roadmap_item_falls_back_to_deltapulse_search(self):
        """Roadmap items without a URL should fall back to DeltaPulse dashboard search."""
        item = {
            "id": "558435",
            "title": "Security Update Alerts",
            "source": "roadmap",
            "status": "In development",
            "service": ["Microsoft 365"],
        }

        article = fetch_m365_data.build_article_from_m365_item(item)

        self.assertEqual(
            article["link"],
            "https://deltapulse.app/item/558435",
        )

    def test_roadmap_item_uses_release_date_for_target_date(self):
        """Roadmap items should preserve DeltaPulse releaseDate for expected release display."""
        item = {
            "id": "558679",
            "title": "Teams preview improvements",
            "source": "roadmap",
            "status": "In development",
            "service": ["Microsoft Teams"],
            "releaseDate": "June CY2026",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)

        self.assertEqual(article["m365TargetDate"], "June 2026")

    def test_published_date_falls_back_through_fields(self):
        """Items without publishedDate should try other date fields."""
        item = {
            "id": "558435",
            "title": "Roadmap item",
            "source": "roadmap",
            "status": "In development",
            "service": [],
            "lastModifiedDateTime": "2026-03-18T10:00:00.000Z",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)
        self.assertEqual(article["published"], "2026-03-18T10:00:00.000Z")

    def test_published_date_uses_created_date(self):
        """Roadmap items should use createdDate when datetime fields are absent."""
        item = {
            "id": "558435",
            "title": "Roadmap item",
            "source": "roadmap",
            "status": "In development",
            "service": [],
            "createdDate": "2026-03-18T00:00:00.000Z",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)
        self.assertEqual(article["published"], "2026-03-18T00:00:00.000Z")

    def test_extracts_explicit_retirement_date_from_text(self):
        item = {
            "id": "MC200001",
            "title": "Upcoming change: Retirement of feature X on July 31, 2026",
            "source": "message_center",
            "publishedDate": "2026-03-19T04:39:14.000Z",
            "service": ["Microsoft Teams"],
            "description": "We are retiring feature X on July 31, 2026.",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)

        self.assertEqual(article["m365RetirementDate"], "2026-07-31")
        self.assertEqual(article["m365RetirementDatePrecision"], "day")
        self.assertEqual(article["lifecycle"], "retiring")

    def test_does_not_infer_retirement_date_from_target_date_only(self):
        item = {
            "id": "MC200002",
            "title": "Feature rollout update",
            "source": "message_center",
            "publishedDate": "2026-03-19T04:39:14.000Z",
            "service": ["Microsoft Teams"],
            "targetDate": "July 2026",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)

        self.assertIsNone(article["m365RetirementDate"])


class CategorizeByProductTests(unittest.TestCase):
    """Test M365 product categorization."""
    
    def test_categorizes_by_product(self):
        """Articles should be categorized by M365 product."""
        articles = [
            {"title": "Teams update", "m365Service": "Teams"},
            {"title": "Excel feature", "m365Service": "Excel"},
            {"title": "SharePoint news", "m365Service": "SharePoint Online"},
        ]
        
        categorized = fetch_m365_data.categorize_by_product(articles)
        
        # Teams should be in Collaboration
        self.assertTrue(any(a["title"] == "Teams update" for cat in ["Collaboration"] for a in categorized.get(cat, [])))
        self.assertTrue(any(a["title"] == "Excel feature" for cat in ["Productivity"] for a in categorized.get(cat, [])))


class ChecksumMetadataTests(unittest.TestCase):
    """Test M365 checksum generation (same as Azure pattern)."""
    
    def test_build_checksums_payload(self):
        """Checksums should include expected fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            m365_file = pathlib.Path(tmpdir) / "m365_data.json"
            m365_file.write_text('{"totalArticles": 145}\n', encoding="utf-8")
            
            payload = fetch_m365_data.build_checksums_payload(
                [m365_file],
                generated_at="2026-03-19T00:00:00+00:00"
            )
            
            self.assertEqual(payload["generatedAt"], "2026-03-19T00:00:00+00:00")
            self.assertEqual(len(payload["artifacts"]), 1)
            self.assertEqual(payload["artifacts"][0]["algorithm"], "sha256")
            self.assertIn("value", payload["artifacts"][0])


class FailsafeTests(unittest.TestCase):
    """Test M365 publish failsafe logic."""
    
    def test_triggers_on_large_drop(self):
        """Failsafe should trigger on significant metric drop."""
        triggered, details = fetch_m365_data.evaluate_m365_failsafe(
            new_count=50,
            previous_count=200,  # 25% - below 60% threshold
        )
        self.assertTrue(triggered)
        self.assertIn("relative_trigger=True", details)
    
    def test_triggers_on_absolute_floor(self):
        """Failsafe should trigger if below absolute floor."""
        triggered, details = fetch_m365_data.evaluate_m365_failsafe(
            new_count=70,
            previous_count=150,  # Above 60% but below floor
        )
        self.assertTrue(triggered)
    
    def test_does_not_trigger_normal(self):
        """Failsafe should not trigger for normal variation."""
        triggered, details = fetch_m365_data.evaluate_m365_failsafe(
            new_count=150,
            previous_count=200,  # 75% - normal variation
        )
        self.assertFalse(triggered)


class BuildM365FeedTests(unittest.TestCase):
    """Test feed-level output fields."""

    def test_includes_m365_video_payload(self):
        """Feed payload should include m365Video metadata."""
        raw_items = [
            {
                "id": "MC1000",
                "title": "A message",
                "source": "message_center",
                "publishedDate": "2026-03-19T04:39:14.000Z",
                "service": ["Teams"],
                "url": "https://deltapulse.app/item/MC1000",
            }
        ]
        m365_video = {
            "title": "What's new in Microsoft 365 | March Updates",
            "url": "https://www.youtube.com/watch?v=HdO9NV8a9yE&t=83s",
            "published": "2026-03-01T00:00:00+00:00",
            "thumbnail": "https://i.ytimg.com/vi/HdO9NV8a9yE/hqdefault.jpg",
        }

        feed = fetch_m365_data.build_m365_feed(raw_items, m365_video)
        self.assertEqual(feed.get("m365Video"), m365_video)

    def test_outputs_m365_retirement_calendar_and_buckets(self):
        raw_items = [
            {
                "id": "MC3001",
                "title": "Retirement: capability A retires on June 30, 2026",
                "description": "This capability will retire on June 30, 2026.",
                "source": "message_center",
                "publishedDate": "2026-03-20T00:00:00.000Z",
                "service": ["Microsoft Teams"],
                "url": "https://deltapulse.app/item/MC3001",
            },
            {
                "id": "MC3002",
                "title": "Deprecation: capability B retires in October 2026",
                "description": "Capability B retirement is planned for October 2026.",
                "source": "message_center",
                "publishedDate": "2026-03-20T00:00:00.000Z",
                "service": ["SharePoint Online"],
                "url": "https://deltapulse.app/item/MC3002",
            },
        ]

        feed = fetch_m365_data.build_m365_feed(raw_items)

        self.assertIn("m365RetirementCalendar", feed)
        self.assertIn("m365RetirementBuckets", feed)
        self.assertEqual(len(feed["m365RetirementCalendar"]), 2)
        self.assertIn("windows", feed["m365RetirementBuckets"])
        self.assertGreaterEqual(feed["m365RetirementBuckets"]["windows"]["0_3_months"]["count"], 1)
    
    def test_does_not_trigger_without_baseline(self):
        """Failsafe should not trigger if no baseline."""
        triggered, details = fetch_m365_data.evaluate_m365_failsafe(
            new_count=50,
            previous_count=None,
        )
        self.assertFalse(triggered)


class M365ConcurrencyTests(unittest.TestCase):
    """Test concurrent enrichment behavior for M365 items."""

    def test_fetch_m365_items_enriches_unique_items_once(self):
        session = mock.Mock()

        new_items = [
            {"id": "RM100", "source": "roadmap", "title": "Roadmap A"},
            {"id": "RM100", "source": "roadmap", "title": "Roadmap A duplicate"},
            {"id": "MC200", "source": "message_center", "title": "Message B"},
        ]
        updated_items = []

        def enrich_side_effect(_session, item):
            if item.get("id") == "RM100":
                return {"status": "In Development"}
            return {"severity": "high"}

        with mock.patch.object(
            fetch_m365_data,
            "call_mcp_tool",
            side_effect=[new_items, updated_items],
        ), mock.patch.object(
            fetch_m365_data,
            "_enrich_m365_item",
            side_effect=enrich_side_effect,
        ) as enrich_mock:
            items = fetch_m365_data.fetch_m365_items(session)

        self.assertEqual(len(items), 3)
        self.assertEqual(enrich_mock.call_count, 2)
        self.assertEqual(items[0].get("status"), "In Development")
        self.assertEqual(items[2].get("severity"), "high")


class YouTubeVideoHelperTests(unittest.TestCase):
    """Test YouTube helper parity behavior."""

    def test_m365_series_title_match_is_specific(self):
        self.assertTrue(
            fetch_m365_data._is_m365_monthly_video_title(
                "What's new in Microsoft 365 | March 2026"
            )
        )
        self.assertTrue(
            fetch_m365_data._is_m365_monthly_video_title(
                "What’s new in Microsoft 365 | March 2026"
            )
        )
        self.assertFalse(
            fetch_m365_data._is_m365_monthly_video_title(
                "Microsoft 365 update roundup"
            )
        )

    def test_select_best_youtube_video_entry_falls_back_to_latest(self):
        entries = [
            {"title": "Latest random upload", "link": "https://www.youtube.com/watch?v=latest"},
            {"title": "Older random upload", "link": "https://www.youtube.com/watch?v=older"},
        ]

        best, used_fallback = fetch_m365_data._select_best_youtube_video_entry(
            entries,
            lambda _: 0,
        )

        self.assertTrue(used_fallback)
        self.assertEqual(best["link"], "https://www.youtube.com/watch?v=latest")

    def test_resolve_youtube_channel_id_from_seed_extracts_channel_id(self):
        session = mock.Mock()
        response = mock.Mock()
        response.text = '<script>{"channelId":"UCm365Channel123"}</script>'
        response.raise_for_status.return_value = None
        session.get.return_value = response

        channel_id = fetch_m365_data._resolve_youtube_channel_id_from_seed(
            session,
            "https://www.youtube.com/watch?v=HdO9NV8a9yE",
            (5, 20),
        )

        self.assertEqual(channel_id, "UCm365Channel123")

    def test_fetch_m365_video_does_not_use_unrelated_latest_upload(self):
        session = mock.Mock()

        seed_response = mock.Mock()
        seed_response.text = '<script>{"channelId":"UCm365Channel123"}</script>'
        seed_response.raise_for_status.return_value = None

        rss_response = mock.Mock()
        rss_response.content = b"<feed></feed>"
        rss_response.raise_for_status.return_value = None

        session.get.side_effect = [seed_response, rss_response]

        with mock.patch.object(fetch_m365_data.feedparser, "parse") as parse_mock:
            parse_mock.return_value = mock.Mock(
                entries=[
                    {
                        "title": "Random channel upload",
                        "link": "https://www.youtube.com/watch?v=random123",
                        "published": "2026-03-20T10:00:00+00:00",
                    }
                ]
            )

            result = fetch_m365_data.fetch_m365_video(session)

        self.assertEqual(result["url"], fetch_m365_data.M365_VIDEO_SEED_URL)


if __name__ == "__main__":
    unittest.main()
