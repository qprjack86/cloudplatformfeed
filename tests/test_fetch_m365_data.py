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
        url = "https://deltapulse.app/dashboard?message=MC1255714&utm_source=feed"
        normalized = fetch_m365_data.normalize_url(url)
        # Tracking param should be removed
        self.assertNotIn("utm_source", normalized)
        self.assertIn("message=MC1255714", normalized)
    
    def test_removes_www_prefix(self):
        """www. prefix should be removed."""
        url = "https://www.deltapulse.app/dashboard?message=MC1255714"
        normalized = fetch_m365_data.normalize_url(url)
        self.assertEqual(normalized, fetch_m365_data.normalize_url("https://deltapulse.app/dashboard?message=MC1255714"))
    
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
                "link": "https://deltapulse.app/dashboard?message=MC123&utm_source=rss",
                "published": published,
            },
            {
                "title": "Different title same URL",
                "link": "https://deltapulse.app/dashboard?message=MC123&gclid=abc",
                "published": published,
            },
            {
                "title": "Unique article",
                "link": "https://deltapulse.app/dashboard?message=MC456",
                "published": published,
            },
        ]
        
        deduped = fetch_m365_data.dedupe_m365_articles(articles)
        self.assertEqual(len(deduped), 2)
        titles = [a["title"] for a in deduped]
        self.assertIn("First version", titles)
        self.assertIn("Unique article", titles)
    
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
            "url": "https://deltapulse.app/dashboard?message=MC1255714",
        }
        
        article = fetch_m365_data.build_article_from_m365_item(item)
        
        self.assertEqual(article["title"], "New feature announcement")
        self.assertEqual(
            article["link"],
            "https://admin.microsoft.com/Adminportal/Home?#/MessageCenter/:/messages/MC1255714",
        )
        self.assertEqual(article["source"], "m365")
        self.assertEqual(article["m365Service"], "Teams")
        self.assertEqual(article["m365Id"], "MC1255714")
        self.assertEqual(article["m365AllServices"], ["Teams", "SharePoint"])
        self.assertIsNotNone(article["lifecycle"])

    def test_preserves_direct_admin_link_when_provided(self):
        """Direct admin links should be used as-is when present."""
        item = {
            "id": "MC999999",
            "title": "Admin portal update",
            "source": "message_center",
            "publishedDate": "2026-03-19T04:39:14.000Z",
            "service": ["Teams"],
            "url": "https://deltapulse.app/dashboard?message=MC999999",
            "detailsUrl": "https://admin.microsoft.com/Adminportal/Home?#/MessageCenter/:/messages/MC999999",
        }

        article = fetch_m365_data.build_article_from_m365_item(item)

        self.assertEqual(
            article["link"],
            "https://admin.microsoft.com/Adminportal/Home?#/MessageCenter/:/messages/MC999999",
        )


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
    
    def test_does_not_trigger_without_baseline(self):
        """Failsafe should not trigger if no baseline."""
        triggered, details = fetch_m365_data.evaluate_m365_failsafe(
            new_count=50,
            previous_count=None,
        )
        self.assertFalse(triggered)


if __name__ == "__main__":
    unittest.main()
