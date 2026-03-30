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


if __name__ == "__main__":
    unittest.main()
