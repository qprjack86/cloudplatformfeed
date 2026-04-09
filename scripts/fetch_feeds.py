#!/usr/bin/env python3
"""
Microsoft Cloud Platform Feed - RSS Feed Fetcher
Fetches articles from Azure and Microsoft 365 blog RSS feeds and generates a JSON data file.
"""

import feedparser
import csv
import json
import os
import re
import concurrent.futures
import requests
from pathlib import Path
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.parse import urlsplit
from xml.dom.minidom import Document

from feed_common import (
    canonicalize_url,
    create_http_session as shared_create_http_session,
    build_checksums_payload as shared_build_checksums_payload,
    write_checksums_file as shared_write_checksums_file,
    evaluate_publish_failsafe as shared_evaluate_publish_failsafe,
    extract_youtube_video_id as shared_extract_youtube_video_id,
    build_youtube_thumbnail_from_video_url as shared_build_youtube_thumbnail_from_video_url,
    load_previous_article_count as shared_load_previous_article_count,
    load_site_config as shared_load_site_config,
    normalize_host as shared_normalize_host,
    resolve_youtube_channel_id_from_seed as shared_resolve_youtube_channel_id_from_seed,
    select_best_youtube_video_entry as shared_select_best_youtube_video_entry,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_CONFIG_PATH = REPO_ROOT / "config" / "site.json"


def _normalize_site_host(value):
    """Normalize a configured host value for canonical URL checks."""
    return shared_normalize_host(value)


def load_site_config(path=SITE_CONFIG_PATH):
    """Load and validate canonical site settings from config/site.json."""
    return shared_load_site_config(path)


SITE_CONFIG = load_site_config()
CANONICAL_SITE_HOST = SITE_CONFIG["canonicalHost"]
CANONICAL_SITE_URL = SITE_CONFIG["canonicalUrl"]

# Blog definitions: board_id -> display name
BLOGS = {
    "analyticsonazure": "Analytics on Azure",
    "appsonazureblog": "Apps on Azure",
    "azurearcblog": "Azure Arc",
    "azurearchitectureblog": "Azure Architecture",
    "azurecommunicationservicesblog": "Communication Services",
    "azurecompute": "Azure Compute",
    "azureconfidentialcomputingblog": "Confidential Computing",
    "azure-databricks": "Azure Databricks",
    "azure-events": "Azure Events",
    "azuregovernanceandmanagementblog": "Governance & Management",
    "azure-customer-innovation-blog": "Customer Innovation",
    "azurehighperformancecomputingblog": "High Performance Computing",
    "azureinfrastructureblog": "Azure Infrastructure",
    "integrationsonazureblog": "Integrations on Azure",
    "azuremapsblog": "Azure Maps",
    "azuremigrationblog": "Azure Migration",
    "azurenetworkingblog": "Azure Networking",
    "azurenetworksecurityblog": "Azure Network Security",
    "azureobservabilityblog": "Azure Observability",
    "azurepaasblog": "Azure PaaS",
    "azurestackblog": "Azure Stack",
    "azurestorageblog": "Azure Storage",
    "finopsblog": "FinOps",
    "azuretoolsblog": "Azure Tools",
    "azurevirtualdesktopblog": "Azure Virtual Desktop",
    "linuxandopensourceblog": "Linux & Open Source",
    "messagingonazureblog": "Messaging on Azure",
    "telecommunications-industry-blog": "Telecommunications",
    "azuredevcommunityblog": "Azure Dev Community",
    "oracleonazureblog": "Oracle on Azure",
    "microsoft-planetary-computer-blog": "Planetary Computer",
    "microsoftsentinelblog": "Microsoft Sentinel",
    "microsoftdefendercloudblog": "Microsoft Defender for Cloud",
    "azureadvancedthreatprotection": "Azure Advanced Threat Protection",
}

TC_RSS_URL = (
    "https://techcommunity.microsoft.com/t5/s/gxcuf89792/rss/board?board.id={board}"
)
AKS_BLOG_FEED = "https://blog.aks.azure.com/rss.xml"
AZURE_UPDATES_FEED = "https://www.microsoft.com/releasecommunications/api/v2/azure/rss"
AZURE_UPDATES_API = "https://www.microsoft.com/releasecommunications/api/v2/azure"
AZTTY_DEPRECATIONS_FEED = "https://aztty.azurewebsites.net/rss/deprecations"
AZTTY_UPDATES_FEED = "https://aztty.azurewebsites.net/rss/updates"
AZURE_RETIREMENTS_EXPORT_PATH = REPO_ROOT / "data" / "export_data.csv"
YOUTUBE_RSS_BASE = "https://www.youtube.com/feeds/videos.xml"
SAVILL_VIDEO_SEED_URL = "https://www.youtube.com/watch?v=17uHDPjdkto"
SUMMARY_WINDOW_DAYS = 7
MAX_ITEMS_PER_SECTION = 5
MAX_UNCLASSIFIED_FOR_AI = 20
LIFECYCLE_SECTIONS = {
    "in_preview": "In preview",
    "launched_ga": "Launched / Generally Available",
    "retiring": "Retiring",
    "in_development": "In development",
}
BULLET_PREFIX = "  \u2022 "
SECTION_HEADING_PREFIX = "- "
FALLBACK_BULLET = "none noted in selected window"
FEED_REQUEST_TIMEOUT = (5, 20)
FEED_RETRY_TOTAL = 2
FEED_BACKOFF_FACTOR = 1
FEED_USER_AGENT = f"AzureFeedBot/1.0 (+{CANONICAL_SITE_URL})"
FETCH_MAX_WORKERS = 4
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "ocid",
    "spm",
    "trk",
    "wt.mc_id",
}
DEFAULT_PORTS = {"http": 80, "https": 443}
PUBLIC_SUMMARY_REASONS = {
    "no_dated_articles",
    "no_articles_in_window",
    "missing_azure_openai_config",
    "azure_openai_failed",
}
FAILSAFE_MIN_ARTICLES = 80
FAILSAFE_MIN_RATIO = 0.60
RUN_METRICS_ENV_VAR = "AZUREFEED_RUN_METRICS_PATH"


CHECKSUM_ARTIFACTS = [
    Path("data") / "feeds.json",
    Path("data") / "feed.xml",
]
CHECKSUM_OUTPUT_PATH = Path("data") / "checksums.json"


def _artifact_checksum_record(path, generated_at):
    """Return checksum metadata for an existing artifact file."""
    return shared_build_checksums_payload([path], generated_at)["artifacts"][0]


def build_checksums_payload(paths, generated_at=None):
    """Build checksum metadata for published artifacts."""
    return shared_build_checksums_payload(paths, generated_at=generated_at)


def write_checksums_file(paths=None, output_path=CHECKSUM_OUTPUT_PATH, generated_at=None):
    """Write checksum metadata after published artifacts are finalized."""
    artifact_paths = paths or CHECKSUM_ARTIFACTS
    return shared_write_checksums_file(
        artifact_paths,
        output_path,
        generated_at=generated_at,
        logger=print,
    )


# DevBlogs definitions: slug -> (display name, feed URL)
DEVBLOGS = {
    "allthingsazure": ("All Things Azure", "https://devblogs.microsoft.com/all-things-azure/feed/"),
    "msdevblog": ("Microsoft Developers Blog", "https://developer.microsoft.com/blog/feed/"),
    "visualstudio": ("Visual Studio Blog", "https://devblogs.microsoft.com/visualstudio/feed/"),
    "vscodeblog": ("VS Code Blog", "https://devblogs.microsoft.com/vscode-blog/feed/"),
    "developfromthecloud": ("Develop from the Cloud", "https://devblogs.microsoft.com/develop-from-the-cloud/feed/"),
    "azuredevops": ("Azure DevOps Blog", "https://devblogs.microsoft.com/devops/feed/"),
    "iseblog": ("ISE Developer Blog", "https://devblogs.microsoft.com/ise/feed/"),
    "azuresdkblog": ("Azure SDK Blog", "https://devblogs.microsoft.com/azure-sdk/feed/"),
    "commandline": ("Windows Command Line", "https://devblogs.microsoft.com/commandline/feed/"),
    "aspireblog": ("Aspire Blog", "https://devblogs.microsoft.com/aspire/feed/"),
    "foundryblog": ("Microsoft Foundry Blog", "https://devblogs.microsoft.com/foundry/feed/"),
    "cosmosdbblog": ("Azure Cosmos DB Blog", "https://devblogs.microsoft.com/cosmosdb/feed/"),
    "azuresqlblog": ("Azure SQL Dev Corner", "https://devblogs.microsoft.com/azure-sql/feed/"),
}


def normalize_host(hostname):
    """Normalize hostnames used for feed allowlisting and URL dedupe."""
    return shared_normalize_host(hostname)


def build_allowed_feed_hosts():
    """Return the set of remote hosts that are allowed for feed retrieval."""
    source_urls = [
        TC_RSS_URL.format(board="azurecompute"),
        AKS_BLOG_FEED,
        AZURE_UPDATES_FEED,
        AZURE_UPDATES_API,
        AZTTY_DEPRECATIONS_FEED,
        AZTTY_UPDATES_FEED,
        YOUTUBE_RSS_BASE,
        SAVILL_VIDEO_SEED_URL,
    ]
    source_urls.extend(feed_url for _, feed_url in DEVBLOGS.values())

    hosts = set()
    for url in source_urls:
        host = normalize_host(urlsplit(url).hostname)
        if host:
            hosts.add(host)
    return hosts


ALLOWED_FEED_HOSTS = build_allowed_feed_hosts()


def create_http_session():
    """Create a session with bounded retries for transient feed failures."""
    return shared_create_http_session(
        retry_total=FEED_RETRY_TOTAL,
        backoff_factor=FEED_BACKOFF_FACTOR,
        user_agent=FEED_USER_AGENT,
        allowed_methods=("GET",),
        raise_on_status=False,
    )


HTTP_SESSION = create_http_session()


def validate_feed_url(url):
    """Reject unexpected feed URLs before attempting any network request."""
    parsed = urlsplit((url or "").strip())
    host = normalize_host(parsed.hostname)

    if parsed.scheme != "https":
        raise ValueError(f"Feed URL must use https: {url}")
    if not host or host not in ALLOWED_FEED_HOSTS:
        raise ValueError(f"Feed URL host is not allowlisted: {url}")

    return parsed


def fetch_feed(url):
    """Fetch and parse a remote feed using explicit transport controls."""
    validate_feed_url(url)
    response = HTTP_SESSION.get(url, timeout=FEED_REQUEST_TIMEOUT)
    response.raise_for_status()
    return feedparser.parse(response.content)


def clean_html(text):
    """Remove HTML tags and clean up text."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", "", text)
    clean = unescape(clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def truncate(text, max_length=300):
    """Truncate text to max_length, ending at a word boundary."""
    if len(text) <= max_length:
        return text
    truncated = text[:max_length].rsplit(" ", 1)[0]
    return truncated + "..."


def get_recent_publishing_days(articles, max_days):
    """Return the calendar days within max_days of the most recent article."""
    published_days_all = sorted(
        {
            article.get("published", "")[:10]
            for article in articles
            if re.match(r"\d{4}-\d{2}-\d{2}", article.get("published", ""))
        },
        reverse=True,
    )
    if not published_days_all:
        return []

    latest_day_str = published_days_all[0]
    latest_date = datetime.fromisoformat(latest_day_str).replace(tzinfo=timezone.utc)
    cutoff_date = latest_date - timedelta(days=max_days)

    return [
        day_str for day_str in published_days_all
        if datetime.fromisoformat(day_str).replace(tzinfo=timezone.utc) > cutoff_date
    ]


def get_articles_for_publishing_days(articles, publishing_days):
    """Return articles whose published date falls within the selected publishing days."""
    publishing_days_set = set(publishing_days)
    return [
        article
        for article in articles
        if article.get("published", "")[:10] in publishing_days_set
    ]


def _normalize_for_match(text):
    """Normalize text for fuzzy title matching."""
    value = (text or "").lower()
    value = re.sub(r"\[[^\]]+\]", " ", value)
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_article_url(url):
    """Canonicalize article URLs for more reliable deduplication."""
    return canonicalize_url(
        url,
        tracking_query_prefixes=TRACKING_QUERY_PREFIXES,
        tracking_query_keys=TRACKING_QUERY_KEYS,
        default_ports=DEFAULT_PORTS,
    )


def parse_iso_datetime(value):
    """Parse an ISO datetime string into UTC."""
    if not value:
        return None

    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        # Some feeds emit >6 fractional second digits (for example .1234567),
        # which datetime.fromisoformat (py3.9) cannot parse directly.
        normalized = value.replace("Z", "+00:00")
        trimmed = re.sub(r"(\.\d{6})\d+([+-]\d{2}:\d{2})$", r"\1\2", normalized)
        if trimmed != normalized:
            try:
                dt = datetime.fromisoformat(trimmed)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except ValueError:
                return None
        return None


def article_is_recent(article, cutoff_dt):
    """Return whether the article is within the retention window."""
    published_dt = parse_iso_datetime(article.get("published", ""))
    return published_dt is not None and published_dt >= cutoff_dt


def dedupe_articles(articles):
    """Drop stale or duplicate articles using canonical links and title/day matching."""
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=30)
    seen_links = set()
    seen_title_days = set()
    unique_articles = []

    for article in articles:
        if not article_is_recent(article, cutoff_dt):
            retirement_date = article.get("azureRetirementDate")
            if not _is_retirement_date_future(retirement_date):
                print(f"Discarding stale/undated article: {article.get('title', 'Untitled')}")
                continue

        canonical_link = normalize_article_url(article.get("link", ""))
        title_key = None
        normalized_title = _normalize_for_match(article.get("title", ""))
        published_day = article.get("published", "")[:10]
        if normalized_title and published_day and len(normalized_title) >= 20:
            title_key = (normalized_title, published_day)

        duplicate_reason = None
        if canonical_link and canonical_link in seen_links:
            duplicate_reason = "canonical_url"
        elif title_key and title_key in seen_title_days:
            duplicate_reason = "normalized_title_day"

        if duplicate_reason:
            print(
                f"Deduplicated article via {duplicate_reason}: "
                f"{article.get('title', 'Untitled')}"
            )
            continue

        if canonical_link:
            seen_links.add(canonical_link)
        if title_key:
            seen_title_days.add(title_key)
        unique_articles.append(article)

    return unique_articles


def attach_links_to_summary(summary_text, summary_articles):
    """Attach article links to bullet lines when model output omits markdown links."""
    if not summary_text:
        return summary_text

    # Recover from malformed heading links like:
    # - [Launched / Generally Available:](https://...)
    summary_text = re.sub(
        r"(?m)^- \[([^\]]+:)\]\(https?://[^\s)]+\)\s*$",
        r"- \1",
        summary_text,
    )

    candidates = []
    for article in summary_articles:
        title = (article.get("title") or "").strip()
        link = (article.get("link") or "").strip()
        if title and link:
            candidates.append(
                {
                    "title": title,
                    "norm": _normalize_for_match(title),
                    "link": link,
                }
            )

    markdown_link_re = re.compile(r"\[[^\]]+\]\((https?://[^\s)]+)\)")
    bullet_re = re.compile(r"^(\s+[•\-*]\s+)(.+)$")

    out_lines = []
    for line in summary_text.splitlines():
        bullet = bullet_re.match(line)
        if not bullet:
            out_lines.append(line)
            continue

        prefix, content = bullet.groups()
        stripped = content.strip()
        if not stripped or "none noted in selected window" in stripped.lower():
            out_lines.append(line)
            continue
        if markdown_link_re.search(stripped):
            out_lines.append(line)
            continue

        best = None
        probe = _normalize_for_match(stripped)
        for candidate in candidates:
            score = SequenceMatcher(None, probe, candidate["norm"]).ratio()
            if probe and (probe in candidate["norm"] or candidate["norm"] in probe):
                score += 0.2
            if best is None or score > best[0]:
                best = (score, candidate)

        if best and best[0] >= 0.45:
            out_lines.append(
                f"{prefix}[{stripped}]({best[1]['link']})"
            )
        else:
            out_lines.append(line)

    return "\n".join(out_lines)


def classify_lifecycle(article):
    """Classify an article into a lifecycle bucket based on title patterns.

    Returns 'in_preview', 'launched_ga', 'retiring', 'in_development', or None when
    no deterministic signal is present.
    """
    title = (article.get("title") or "").lower()
    if re.search(r"retirement|deprecated|deprecat|retir", title):
        return "retiring"
    if re.search(r"\[in development\]|in development|coming soon", title):
        return "in_development"
    if re.search(r"\[.*?preview\]|public preview|private preview|\bin preview\b|now in.*?preview", title):
        return "in_preview"
    if re.search(
        r"\[.*?generally available\]|\[ga\]|generally available|general availability"
        r"|now available\b|is now available",
        title,
    ):
        return "launched_ga"
    return None


def classify_with_ai(candidates, client, deployment):
    """Ask AI to classify and label articles lacking deterministic lifecycle signals.

    Returns a list of dicts with keys: id, bucket, label.  On any failure
    returns an empty list so the caller can proceed without AI results.
    """
    if not candidates:
        return []

    records = [
        {
            "id": str(i),
            "title": a.get("title", ""),
            "blogId": a.get("blogId", ""),
            "summary_snippet": (a.get("summary") or "")[:80],
        }
        for i, a in enumerate(candidates)
    ]
    system_msg = (
        "You are an Azure release classifier. "
        "For each item assign a bucket and write a concise one-line display label. "
        'Bucket must be exactly one of: in_preview, launched_ga, retiring, in_development, other. '
        'Return ONLY a JSON object with key "items" containing an array. '
        "Each element must have: id (string), bucket (string), label (string). "
        "No explanation, no markdown fences, only the JSON object."
    )
    user_msg = json.dumps({"items": records}, ensure_ascii=False)

    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_completion_tokens=300,
        )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
        parsed = json.loads(raw)
        valid_buckets = {"in_preview", "launched_ga", "retiring", "in_development", "other"}
        results = []
        for item in parsed.get("items", []):
            bucket = item.get("bucket", "")
            if bucket not in valid_buckets:
                continue
            results.append({
                "id": str(item.get("id", "")),
                "bucket": bucket,
                "label": (item.get("label") or "").strip(),
            })
        return results
    except Exception as e:
        print(f"  AI classifier failed: {e}")
        return []


def render_summary_markdown(buckets):
    """Render lifecycle summary markdown from pre-built bucket data.

    buckets: {"in_preview": [{"label": str, "link": str}], ...}
    """
    lines = []
    for key, heading in LIFECYCLE_SECTIONS.items():
        lines.append(f"{SECTION_HEADING_PREFIX}{heading}:")
        items = buckets.get(key, [])[:MAX_ITEMS_PER_SECTION]
        if items:
            for item in items:
                label = item.get("label") or ""
                link = item.get("link") or ""
                if label and link:
                    lines.append(f"{BULLET_PREFIX}[{label}]({link})")
                elif label:
                    lines.append(f"{BULLET_PREFIX}{label}")
        else:
            lines.append(f"{BULLET_PREFIX}{FALLBACK_BULLET}")
        lines.append("")
    return "\n".join(lines).rstrip()


def parse_date(entry):
    """Parse date from feed entry, return ISO format string."""
    for field in ["published_parsed", "updated_parsed"]:
        parsed = entry.get(field)
        if parsed:
            try:
                dt = datetime(*parsed[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except (ValueError, TypeError):
                continue

    for field in ["published", "updated"]:
        date_str = entry.get(field, "")
        if date_str:
            try:
                dt = parsedate_to_datetime(date_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
            except (TypeError, ValueError, IndexError, OverflowError):
                continue

    return datetime.now(timezone.utc).isoformat()


def _extract_youtube_video_id(url):
    """Extract a YouTube video id from watch or youtu.be links."""
    return shared_extract_youtube_video_id(url)


def _build_youtube_thumbnail_from_video_url(url):
    """Build a deterministic thumbnail URL from a YouTube video link."""
    return shared_build_youtube_thumbnail_from_video_url(url)


def _resolve_youtube_channel_id_from_seed(session, seed_url, timeout):
    """Resolve a YouTube channel id by reading the seed video page payload."""
    return shared_resolve_youtube_channel_id_from_seed(session, seed_url, timeout)


def _select_best_youtube_video_entry(entries, match_score_fn):
    """Select highest scoring entry; fall back to latest upload when no match."""
    return shared_select_best_youtube_video_entry(entries, match_score_fn)


def _entries_to_articles(entries, blog_name, blog_id):
    """Convert feed entries into article payloads."""
    articles = []
    for entry in entries:
        summary = clean_html(entry.get("summary", ""))
        articles.append(
            {
                "title": clean_html(entry.get("title", "Untitled")),
                "link": entry.get("link", ""),
                "published": parse_date(entry),
                "summary": truncate(summary),
                "blog": blog_name,
                "blogId": blog_id,
                "author": entry.get("author", "Microsoft"),
            }
        )
    return articles


def _fetch_named_feed(blog_name, blog_id, feed_url):
    """Fetch a single blog feed and return parsed article records."""
    feed = fetch_feed(feed_url)

    if feed.bozo and not feed.entries:
        print(f"  Warning: Could not parse feed for {blog_name}")
        return []

    articles = _entries_to_articles(feed.entries, blog_name, blog_id)
    print(f"  Found {len(articles)} articles")
    return articles


def fetch_tech_community_feeds():
    """Fetch articles from Tech Community blogs."""
    articles = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_MAX_WORKERS) as executor:
        future_to_blog = {}
        for board_id, blog_name in BLOGS.items():
            print(f"Fetching: {blog_name} ({board_id})...")
            feed_url = TC_RSS_URL.format(board=board_id)
            future = executor.submit(_fetch_named_feed, blog_name, board_id, feed_url)
            future_to_blog[future] = blog_name

        for future in concurrent.futures.as_completed(future_to_blog):
            blog_name = future_to_blog[future]
            try:
                articles.extend(future.result())
            except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
                print(f"  Error fetching {blog_name}: {exc}")

    return articles


def fetch_aks_blog():
    """Fetch articles from the AKS blog."""
    articles = []
    print("Fetching: AKS Blog...")

    try:
        feed = fetch_feed(AKS_BLOG_FEED)

        if feed.bozo and not feed.entries:
            print("  Warning: Could not parse AKS blog feed")
            return articles

        count = 0
        for entry in feed.entries:
            summary = clean_html(entry.get("summary", ""))
            articles.append(
                {
                    "title": clean_html(entry.get("title", "Untitled")),
                    "link": entry.get("link", ""),
                    "published": parse_date(entry),
                    "summary": truncate(summary),
                    "blog": "AKS Blog",
                    "blogId": "aksblog",
                    "author": entry.get("author", "Microsoft"),
                }
            )
            count += 1

        print(f"  Found {count} articles")

    except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
        print(f"  Error fetching AKS blog: {exc}")

    return articles


def fetch_devblogs_feeds():
    """Fetch articles from Microsoft DevBlogs."""
    articles = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_MAX_WORKERS) as executor:
        future_to_blog = {}
        for blog_id, (blog_name, feed_url) in DEVBLOGS.items():
            print(f"Fetching: {blog_name}...")
            future = executor.submit(_fetch_named_feed, blog_name, blog_id, feed_url)
            future_to_blog[future] = blog_name

        for future in concurrent.futures.as_completed(future_to_blog):
            blog_name = future_to_blog[future]
            try:
                articles.extend(future.result())
            except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
                print(f"  Error fetching {blog_name}: {exc}")

    return articles


def fetch_savill_video():
    """Fetch John Savill's latest Azure Infrastructure Update video from YouTube RSS."""
    print("Fetching: John Savill YouTube channel...")
    fallback = {
        "title": "Latest Azure Infrastructure Update",
        "url": SAVILL_VIDEO_SEED_URL,
        "published": "",
        "thumbnail": _build_youtube_thumbnail_from_video_url(SAVILL_VIDEO_SEED_URL),
    }

    try:
        channel_id = _resolve_youtube_channel_id_from_seed(
            HTTP_SESSION,
            SAVILL_VIDEO_SEED_URL,
            FEED_REQUEST_TIMEOUT,
        )
        if not channel_id:
            print("  Warning: Could not resolve Savill YouTube channel id from seed video")
            return fallback

        feed = fetch_feed(f"{YOUTUBE_RSS_BASE}?channel_id={channel_id}")
        if not feed.entries:
            print("  Warning: No entries in Savill YouTube feed")
            return fallback

        def match_score(entry):
            t = entry.get("title", "").lower()
            if "azure infrastructure update" in t:
                return 3
            if "azure" in t and "infrastructure" in t:
                return 2
            if "azure" in t and "update" in t:
                return 1
            return 0

        best, used_fallback = _select_best_youtube_video_entry(feed.entries, match_score)
        if used_fallback:
            print("  Warning: No strong Savill title match found; using latest upload")

        link = best.get("link", "")

        # Extract thumbnail: prefer media:thumbnail element, then deterministic ytimg fallback.
        thumbnail = ""
        media_thumbs = getattr(best, "media_thumbnail", None) or best.get("media_thumbnail", [])
        if media_thumbs:
            thumbnail = media_thumbs[0].get("url", "")
        if not thumbnail:
            thumbnail = _build_youtube_thumbnail_from_video_url(link)

        result = {
            "title": clean_html(best.get("title", "")) or fallback["title"],
            "url": link or fallback["url"],
            "published": parse_date(best) if best else fallback["published"],
            "thumbnail": thumbnail or fallback["thumbnail"],
        }
        print(f"  Found: {result['title'][:70]}")
        return result
    except (
        requests.exceptions.RequestException,
        ValueError,
        TypeError,
        KeyError,
        IndexError,
    ) as exc:
        print(f"  Error fetching Savill YouTube: {exc}")
        return fallback


AZURE_UPDATES_MAX_PAGES = 10
AZURE_UPDATES_RETIREMENT_ENRICH_MAX = 20
WORKBOOK_RETIREMENT_ENRICH_MAX = 60
RETIREMENT_MONTH_PATTERN = (
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
)
RETIREMENT_CONTEXT_PATTERN = re.compile(
    r"retir|will end|end of support|end-of-support|end of life|eol|deprecated|deprecat|sunset|stop supporting",
    re.IGNORECASE,
)
RETIREMENT_DATE_PATTERNS = (
    (
        re.compile(
            rf"\b(?P<month>{RETIREMENT_MONTH_PATTERN})\s+"
            r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*,?\s*(?P<year>\d{4})\b",
            re.IGNORECASE,
        ),
        "day",
    ),
    (
        re.compile(
            rf"\b(?P<day>\d{{1,2}})(?:st|nd|rd|th)?\s+"
            rf"(?P<month>{RETIREMENT_MONTH_PATTERN})\s+(?P<year>\d{{4}})\b",
            re.IGNORECASE,
        ),
        "day",
    ),
    (
        re.compile(
            rf"\b(?P<month>{RETIREMENT_MONTH_PATTERN})\s+(?P<year>\d{{4}})\b",
            re.IGNORECASE,
        ),
        "month",
    ),
)
RETIREMENT_MONTH_TO_INT = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _retirement_date_precision(value):
    """Return normalized precision label for serialized retirement dates."""
    raw = str(value or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return "day"
    if re.match(r"^\d{4}-\d{2}$", raw):
        return "month"
    return None


def _is_retirement_date_future(value, today=None):
    """Return True when a retirement date is in the current/future window."""
    precision = _retirement_date_precision(value)
    if not precision:
        return False

    reference = today or datetime.now(timezone.utc).date()
    raw = str(value or "").strip()
    if precision == "month":
        year, month = raw.split("-")
        return (int(year), int(month)) >= (reference.year, reference.month)

    sort_dt = _parse_retirement_calendar_sort_date(raw)
    return bool(sort_dt and sort_dt.date() >= reference)


def _extract_azure_update_id_from_url(url):
    """Extract Azure Updates identity token from canonical path or id= query variants."""
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlsplit(raw)
    query_match = re.search(r"(?:^|&)id=([^&]+)", parsed.query)
    if query_match:
        return query_match.group(1).strip().lower()

    path_segments = [seg for seg in (parsed.path or "").split("/") if seg]
    try:
        updates_index = path_segments.index("updates")
    except ValueError:
        return ""

    candidate_index = updates_index + 1
    if candidate_index >= len(path_segments):
        return ""
    candidate = path_segments[candidate_index]
    if candidate.lower() == "v2":
        candidate_index += 1
        if candidate_index >= len(path_segments):
            return ""
        candidate = path_segments[candidate_index]

    candidate = candidate.strip().lower()
    if candidate:
        return candidate
    return ""


def _extract_azure_update_retirement_date_from_page(url):
    """Fetch an Azure update page and extract retirement date from full page text."""
    raw = str(url or "").strip()
    if not raw:
        return None

    parsed = urlsplit(raw)
    host = normalize_host(parsed.hostname)
    if parsed.scheme != "https" or host != "azure.microsoft.com":
        return None
    if "/updates" not in (parsed.path or ""):
        return None

    response = HTTP_SESSION.get(raw, timeout=FEED_REQUEST_TIMEOUT)
    response.raise_for_status()
    page_text = clean_html(response.text)
    return _extract_azure_retirement_date("", page_text)


def _extract_azure_update_retirement_date_by_id(update_id):
    """Fetch one Azure Updates item by id and extract retirement date from API fields/text."""
    token = str(update_id or "").strip()
    if not token:
        return None

    url = f"{AZURE_UPDATES_API}?$filter=id%20eq%20'{token}'"
    response = HTTP_SESSION.get(url, timeout=FEED_REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    items = payload.get("value", []) if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        return None

    for item in items:
        if str(item.get("id", "") or "").strip() != token:
            continue
        article = _parse_azure_update_item(item)
        if article:
            return article.get("azureRetirementDate")
    return None


def _retirement_date_rank_key(value):
    """Rank retirement dates so day precision is preferred over month precision."""
    precision = _retirement_date_precision(value)
    sort_dt = _parse_retirement_calendar_sort_date(value)
    return (
        1 if precision == "day" else 0,
        sort_dt or datetime.min.replace(tzinfo=timezone.utc),
    )


def _prefer_retirement_date(candidate_value, current_value):
    """Return True when candidate retirement date should replace current value."""
    if not candidate_value:
        return False
    if not current_value:
        return True
    return _retirement_date_rank_key(candidate_value) > _retirement_date_rank_key(current_value)


def _retirement_event_rank_key(event):
    """Rank merged retirement events by precision/source quality."""
    retirement_date = event.get("retirementDate", "")
    precision = event.get("datePrecision") or _retirement_date_precision(retirement_date)
    sort_dt = _parse_retirement_calendar_sort_date(retirement_date)
    published_dt = parse_iso_datetime(event.get("published", ""))
    return (
        1 if precision == "day" else 0,
        1 if event.get("blogId") == "azureupdates" else 0,
        sort_dt or datetime.min.replace(tzinfo=timezone.utc),
        published_dt or datetime.min.replace(tzinfo=timezone.utc),
    )


def _azure_retirement_identity_key(title, link):
    """Build a cross-source identity key so day/month variants collapse together."""
    normalized_title = _normalize_calendar_title_for_dedupe(title)
    if normalized_title:
        return f"title:{normalized_title}"
    update_id = _extract_azure_update_id_from_url(link)
    if update_id:
        return f"update-id:{update_id}"
    return ""


def _azure_runtime_retirement_alias_key(title, retirement_date):
    """Return a stable alias key for runtime retirements that use variant wording."""
    parsed_date = _parse_retirement_calendar_sort_date(retirement_date)
    if not parsed_date:
        return ""

    display_title = _display_calendar_title(title)
    parts = [part.strip() for part in display_title.split(" - ", 1)]
    if len(parts) != 2:
        return ""

    service_name, feature_name = parts
    if not service_name or not feature_name:
        return ""

    normalized_feature = _normalize_for_match(feature_name)
    runtime_match = re.search(r"\b(node(?:\s*js)?|python|php|\.net|dotnet|java)\s*(\d+)\b", normalized_feature)
    if not runtime_match:
        return ""

    runtime = runtime_match.group(1).replace(" ", "")
    if runtime in {".net", "dotnet"}:
        runtime = "dotnet"
    if runtime.startswith("node"):
        runtime = "node"

    major_version = runtime_match.group(2)
    normalized_service = _normalize_for_match(service_name)
    if not normalized_service:
        return ""

    return f"runtime:{normalized_service}:{runtime}:{major_version}:{retirement_date}"


def _classify_azure_update_lifecycle(status_raw, title_raw, update_type_raw=""):
    """Derive lifecycle bucket from Azure Updates API status/title signals."""
    status = (status_raw or "").lower()
    title = (title_raw or "").lower()
    update_type = (update_type_raw or "").lower()
    text = f"{status} {update_type} {title}".strip()

    if re.search(r"retir|deprecat|sunset|end of support", text):
        return "retiring"
    if re.search(r"in development|coming soon|develop", text):
        return "in_development"
    if re.search(r"preview", text):
        return "in_preview"
    if re.search(r"launch|generally available|\bga\b|now available|available", text):
        return "launched_ga"
    return None


def _normalize_retirement_date_candidate(match, precision):
    """Normalize a regex match into a sortable retirement date candidate."""
    groups = match.groupdict()
    month = RETIREMENT_MONTH_TO_INT.get((groups.get("month") or "")[:3].lower())
    if not month:
        return None

    try:
        year = int(groups.get("year", "0"))
    except ValueError:
        return None

    day_sort = 0
    if precision == "day":
        try:
            day_sort = int(groups.get("day", "0"))
            datetime(year, month, day_sort)
        except (TypeError, ValueError):
            return None
        value = f"{year:04d}-{month:02d}-{day_sort:02d}"
    else:
        value = f"{year:04d}-{month:02d}"

    return {
        "value": value,
        "year": year,
        "month": month,
        "day_sort": day_sort,
        "precision": precision,
    }


def _normalize_structured_retirement_date(value):
    """Normalize structured API retirement dates to YYYY-MM or YYYY-MM-DD."""
    raw = clean_html(str(value or "").strip())
    if not raw:
        return None

    match_day = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if match_day:
        return raw

    match_month = re.match(r"^(\d{4})-(\d{2})$", raw)
    if match_month:
        return raw

    dt = parse_iso_datetime(raw)
    if dt:
        return dt.date().isoformat()

    return None


def _extract_azure_retirement_date(title_raw, summary_raw):
    """Extract a best-effort retirement date from Azure update title/summary text."""
    today = datetime.now(timezone.utc).date()
    candidates = []

    for source_name, source_text in (
        ("title", (title_raw or "").strip()),
        ("summary", (summary_raw or "").strip()),
    ):
        if not source_text:
            continue

        context_spans = [m.span() for m in RETIREMENT_CONTEXT_PATTERN.finditer(source_text)]
        for pattern, precision in RETIREMENT_DATE_PATTERNS:
            for match in pattern.finditer(source_text):
                candidate = _normalize_retirement_date_candidate(match, precision)
                if not candidate:
                    continue

                span_start, span_end = match.span()
                near_context = False
                for ctx_start, ctx_end in context_spans:
                    if ctx_start <= span_end and ctx_end >= span_start:
                        near_context = True
                        break
                    # Prefer dates directly attached to retirement wording (same phrase).
                    if ctx_end <= span_start and (span_start - ctx_end) <= 48:
                        near_context = True
                        break

                if candidate["precision"] == "month":
                    is_future = (candidate["year"], candidate["month"]) >= (
                        today.year,
                        today.month,
                    )
                else:
                    is_future = datetime(
                        candidate["year"],
                        candidate["month"],
                        candidate["day_sort"],
                    ).date() >= today

                candidate["near_context"] = near_context
                candidate["is_future"] = is_future
                candidate["source_priority"] = 1 if source_name == "title" else 0
                candidate["span_start"] = span_start
                candidates.append(candidate)

    if not candidates:
        return None

    best = max(
        candidates,
        key=lambda c: (
            1 if c["near_context"] else 0,
            1 if c["is_future"] else 0,
            c["year"],
            c["month"],
            c["day_sort"],
            c["source_priority"],
            -c["span_start"],
        ),
    )
    return best["value"]


def _parse_azure_update_published(item):
    """Extract the best published timestamp available in an Azure Updates API item."""
    for key in (
        "created",
        "modified",
        "lastModified",
        "publishedDate",
        "publishDate",
        "announcementDate",
    ):
        value = str(item.get(key, "") or "").strip()
        dt = parse_iso_datetime(value)
        if dt:
            return dt.isoformat()
    return None


def _parse_azure_update_item(item):
    """Parse and normalize one Azure Updates API item.

    Returns None when the item has no parseable published timestamp.
    """
    if not isinstance(item, dict):
        return None

    published = _parse_azure_update_published(item)
    if not published:
        return None

    title = clean_html(item.get("title", "Untitled"))
    summary_full = clean_html(item.get("description") or item.get("summary") or "")
    status_raw = clean_html(str(item.get("status", "") or "").strip())
    update_type_raw = clean_html(
        str(
            item.get("type")
            or item.get("notificationType")
            or item.get("announcementType")
            or ""
        ).strip()
    )
    lifecycle = _classify_azure_update_lifecycle(status_raw, title, update_type_raw)
    preview_date = clean_html(str(item.get("previewAvailabilityDate") or "").strip())
    ga_date = clean_html(str(item.get("generalAvailabilityDate") or "").strip())
    target_date_raw = clean_html(str(item.get("targetDate") or "").strip())
    legacy_target_date = clean_html(
        str(
            item.get("generalAvailabilityDate")
            or item.get("previewAvailabilityDate")
            or item.get("targetDate")
            or ""
        ).strip()
    )

    item_id = str(item.get("id", "") or "").strip()
    link = (
        f"https://azure.microsoft.com/en-us/updates/{item_id}/"
        if item_id
        else str(item.get("link", "") or "").strip()
    )

    article = {
        "title": title,
        "link": link,
        "published": published,
        "summary": truncate(summary_full),
        "blog": "Azure Updates",
        "blogId": "azureupdates",
        "author": clean_html(str(item.get("author", "") or "").strip()) or "Microsoft",
    }
    if lifecycle:
        article["lifecycle"] = lifecycle
    if preview_date:
        article["azurePreviewDate"] = preview_date
    if ga_date:
        article["azureGeneralAvailabilityDate"] = ga_date
    if legacy_target_date:
        article["azureTargetDate"] = legacy_target_date
    if lifecycle == "retiring":
        retirement_date = _normalize_structured_retirement_date(target_date_raw)
        extracted_retirement_date = _extract_azure_retirement_date(title, summary_full)
        if _prefer_retirement_date(extracted_retirement_date, retirement_date):
            retirement_date = extracted_retirement_date
        if retirement_date:
            article["azureRetirementDate"] = retirement_date
    if status_raw:
        article["azureStatus"] = status_raw
    if update_type_raw:
        article["azureUpdateType"] = update_type_raw
    return article


def fetch_azure_updates_via_api():
    """Fetch Azure Updates via JSON API and keep only valid dated items in window."""
    print("Fetching: Azure Updates API...")
    articles = []
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=30)
    enrich_budget = AZURE_UPDATES_RETIREMENT_ENRICH_MAX
    url = AZURE_UPDATES_API + "?$orderby=created%20desc"
    page = 0

    while url and page < AZURE_UPDATES_MAX_PAGES:
        page += 1
        if page == 1:
            validate_feed_url(AZURE_UPDATES_API)

        response = HTTP_SESSION.get(url, timeout=FEED_REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("value", []) if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            break

        all_before_cutoff = True
        for item in items:
            article = _parse_azure_update_item(item)
            if not article:
                continue

            if (
                enrich_budget > 0
                and article.get("lifecycle") == "retiring"
                and "azureTargetDate" not in article
                and article.get("blogId") == "azureupdates"
            ):
                current_retirement_date = article.get("azureRetirementDate", "")
                current_precision = _retirement_date_precision(current_retirement_date)
                if current_precision in (None, "month"):
                    try:
                        enriched_retirement_date = _extract_azure_update_retirement_date_from_page(
                            article.get("link", "")
                        )
                    except (
                        requests.exceptions.RequestException,
                        ValueError,
                        TypeError,
                        RuntimeError,
                    ):
                        enriched_retirement_date = None
                    if _prefer_retirement_date(enriched_retirement_date, current_retirement_date):
                        article["azureRetirementDate"] = enriched_retirement_date
                    enrich_budget -= 1

            published_dt = parse_iso_datetime(article.get("published"))
            if not published_dt:
                continue
            retirement_date = article.get("azureRetirementDate")
            keep_for_future_retirement = _is_retirement_date_future(retirement_date)
            if published_dt >= cutoff_dt or keep_for_future_retirement:
                all_before_cutoff = False
                articles.append(article)

        next_link = payload.get("@odata.nextLink", "") if isinstance(payload, dict) else ""
        if all_before_cutoff or not next_link:
            break
        url = next_link

    print(f"  Found {len(articles)} valid API articles across {page} page(s)")
    return articles


def fetch_azure_updates_via_rss():
    """Fetch Azure Updates from RSS as compatibility fallback."""
    articles = []
    print("Fetching: Azure Updates RSS fallback...")

    feed = fetch_feed(AZURE_UPDATES_FEED)

    if feed.bozo and not feed.entries:
        print("  Warning: Could not parse Azure Updates RSS feed")
        return articles

    count = 0
    for entry in feed.entries:
        summary = clean_html(entry.get("summary", ""))
        articles.append(
            {
                "title": clean_html(entry.get("title", "Untitled")),
                "link": entry.get("link", ""),
                "published": parse_date(entry),
                "summary": truncate(summary),
                "blog": "Azure Updates",
                "blogId": "azureupdates",
                "author": entry.get("author", "Microsoft"),
            }
        )
        count += 1

    print(f"  Found {count} RSS articles")
    return articles


def fetch_azure_updates_feed():
    """Fetch Azure Updates with API primary and RSS fallback."""
    try:
        api_articles = fetch_azure_updates_via_api()
        if api_articles:
            return api_articles
        print("  API returned zero valid dated items; falling back to RSS")
    except (
        requests.exceptions.RequestException,
        ValueError,
        TypeError,
        json.JSONDecodeError,
        RuntimeError,
    ) as exc:
        print(f"  Error fetching Azure Updates API: {exc}")
        print("  Falling back to RSS feed")

    try:
        return fetch_azure_updates_via_rss()
    except (
        requests.exceptions.RequestException,
        ValueError,
        TypeError,
        json.JSONDecodeError,
        RuntimeError,
    ) as exc:
        print(f"  Error fetching Azure Updates RSS fallback: {exc}")
        return []


def _classify_aztty_lifecycle(title_raw, summary_raw):
    """Infer lifecycle from aztty deprecations/updates title and summary text."""
    text = f"{title_raw or ''} {summary_raw or ''}".lower()
    if re.search(r"retir|deprecat|sunset|end of support|end of life|eol", text):
        return "retiring"
    if re.search(r"in development|coming soon|develop", text):
        return "in_development"
    if re.search(r"preview", text):
        return "in_preview"
    if re.search(r"launch|generally available|\bga\b|now available|available", text):
        return "launched_ga"
    return None


def fetch_aztty_feed(feed_url, blog_name, blog_id, announcement_type):
    """Fetch aztty RSS feed entries and normalize to common article schema."""
    articles = []
    print(f"Fetching: {blog_name}...")
    feed = fetch_feed(feed_url)

    if feed.bozo and not feed.entries:
        print(f"  Warning: Could not parse {blog_name}")
        return articles

    for entry in feed.entries:
        title = clean_html(entry.get("title", "Untitled"))
        summary_full = clean_html(entry.get("summary", ""))
        lifecycle = _classify_aztty_lifecycle(title, summary_full)
        article = {
            "title": title,
            "link": entry.get("link", ""),
            "published": parse_date(entry),
            "summary": truncate(summary_full),
            "blog": blog_name,
            "blogId": blog_id,
            "author": entry.get("author", "Microsoft"),
            "announcementType": announcement_type,
        }
        if lifecycle:
            article["lifecycle"] = lifecycle
        if lifecycle == "retiring":
            retirement_date = _extract_azure_retirement_date(title, summary_full)
            if retirement_date:
                article["azureRetirementDate"] = retirement_date
        articles.append(article)

    print(f"  Found {len(articles)} RSS articles")
    return articles


def fetch_aztty_announcements():
    """Fetch deprecation and update announcements from aztty RSS feeds."""
    articles = []
    try:
        articles.extend(
            fetch_aztty_feed(
                AZTTY_DEPRECATIONS_FEED,
                "Azure Deprecations (aztty)",
                "azuredeprecations",
                "deprecation",
            )
        )
    except Exception as e:
        print(f"  Error fetching aztty deprecations feed: {e}")

    try:
        articles.extend(
            fetch_aztty_feed(
                AZTTY_UPDATES_FEED,
                "Azure Updates (aztty)",
                "azttyupdates",
                "update",
            )
        )
    except Exception as e:
        print(f"  Error fetching aztty updates feed: {e}")

    return articles


def _get_first_row_value(row, keys):
    """Return the first non-empty CSV value found for any candidate key."""
    for key in keys:
        value = clean_html(str(row.get(key, "") or "")).strip()
        if value:
            return value
    return ""


def _is_azure_updates_link(url):
    """Return True when a URL points to an Azure Updates article page."""
    raw = str(url or "").strip()
    if not raw:
        return False

    parsed = urlsplit(raw)
    return (
        parsed.scheme == "https"
        and normalize_host(parsed.hostname) == "azure.microsoft.com"
        and "/updates" in (parsed.path or "")
    )


def _build_workbook_retirement_cache_key(link):
    """Build a stable cache key for workbook link enrichment."""
    update_id = _extract_azure_update_id_from_url(link)
    if update_id:
        return f"update-id:{update_id}"

    parsed = urlsplit(str(link or "").strip())
    host = normalize_host(parsed.hostname)
    path = (parsed.path or "").rstrip("/").lower()
    if not host and not path:
        return ""
    return f"url:{host}{path}"


def _resolve_workbook_retirement_date(csv_retirement_date, link, cache, enrich_budget):
    """Resolve workbook retirement date, preferring linked article date on conflict."""
    if not _is_azure_updates_link(link):
        return csv_retirement_date, False, enrich_budget

    update_id = _extract_azure_update_id_from_url(link)
    cache_key = _build_workbook_retirement_cache_key(link)
    linked_retirement_date = None
    has_cached_value = cache_key in cache if cache_key else False
    if has_cached_value:
        linked_retirement_date = cache.get(cache_key)

    if not has_cached_value:
        if enrich_budget <= 0:
            return csv_retirement_date, False, enrich_budget
        try:
            if update_id:
                linked_retirement_date = _extract_azure_update_retirement_date_by_id(update_id)
            if not linked_retirement_date:
                linked_retirement_date = _extract_azure_update_retirement_date_from_page(link)
        except (
            requests.exceptions.RequestException,
            ValueError,
            TypeError,
            RuntimeError,
        ):
            linked_retirement_date = None
        enrich_budget -= 1
        if cache_key:
            cache[cache_key] = linked_retirement_date

    if linked_retirement_date and linked_retirement_date != csv_retirement_date:
        return linked_retirement_date, True, enrich_budget

    return csv_retirement_date, False, enrich_budget


def _normalize_csv_retirement_date(raw_value):
    """Normalize workbook retirement dates to YYYY-MM or YYYY-MM-DD."""
    candidate = clean_html(str(raw_value or "")).strip()
    if not candidate:
        return None

    normalized = _normalize_structured_retirement_date(candidate)
    if normalized:
        return normalized

    for pattern in ("%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(candidate, pattern)
            return dt.date().isoformat()
        except ValueError:
            continue

    extracted = _extract_azure_retirement_date("", candidate)
    if extracted:
        return extracted

    return None


def _parse_impacted_services_flag(raw_value):
    """Parse workbook impacted-services indicator into boolean metadata."""
    value = clean_html(str(raw_value or "")).strip().lower()
    if value in {"yes", "y", "true", "1"}:
        return True
    if value in {"no", "n", "false", "0"}:
        return False
    return None


def fetch_azure_retirements_from_csv(csv_path=AZURE_RETIREMENTS_EXPORT_PATH):
    """Load Azure retirement rows from workbook CSV export and map to article schema."""
    path = Path(csv_path)
    if not path.exists():
        print(f"CSV retirement source not found at {path}; skipping")
        return []

    print(f"Fetching: Azure Retirements workbook CSV ({path})...")
    articles = []
    now_iso = datetime.now(timezone.utc).isoformat()
    enrich_cache = {}
    enrich_budget = WORKBOOK_RETIREMENT_ENRICH_MAX
    override_count = 0

    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if not reader.fieldnames:
            print("  Warning: workbook CSV has no header row")
            return []

        for row_number, row in enumerate(reader, start=2):
            service_name = _get_first_row_value(row, ["Service Name", "serviceName", "name", "service"])
            retiring_feature = _get_first_row_value(
                row,
                ["Retiring Feature", "retiringFeature", "feature", "title"],
            )
            retirement_date = _normalize_csv_retirement_date(
                _get_first_row_value(row, ["Retirement Date", "retirementDate", "date"])
            )

            if not retirement_date:
                print(f"  Skipping workbook row {row_number}: invalid retirement date")
                continue

            identity_bits = [bit for bit in (service_name, retiring_feature) if bit]
            if identity_bits:
                title = f"Retirement: {' - '.join(identity_bits)}"
            else:
                title = f"Retirement: Azure service update ({row_number})"

            link = _get_first_row_value(row, ["Actions", "actions", "link", "url"])
            csv_retirement_date = retirement_date
            retirement_date, was_overridden, enrich_budget = _resolve_workbook_retirement_date(
                csv_retirement_date,
                link,
                enrich_cache,
                enrich_budget,
            )
            if was_overridden:
                override_count += 1
            impacted_raw = _get_first_row_value(
                row,
                [
                    "Is Available under the Impacted Services?",
                    "isAvailableUnderImpactedServices",
                    "impactedServices",
                ],
            )
            impacted_flag = _parse_impacted_services_flag(impacted_raw)

            summary_parts = []
            if retiring_feature:
                summary_parts.append(retiring_feature)
            if service_name:
                summary_parts.append(f"Service: {service_name}")
            if impacted_raw:
                summary_parts.append(f"Impacted services: {impacted_raw}")

            article = {
                "title": title,
                "link": link,
                "published": now_iso,
                "summary": truncate(". ".join(summary_parts) or "Workbook retirement entry"),
                "blog": "Azure Retirements Workbook",
                "blogId": "azureretirements",
                "author": "Microsoft",
                "announcementType": "retirement",
                "lifecycle": "retiring",
                "azureRetirementDate": retirement_date,
            }
            if was_overridden:
                article["azureRetirementDateCsv"] = csv_retirement_date
                article["azureRetirementDateSource"] = "linked_page"
            else:
                article["azureRetirementDateSource"] = "csv"
            if impacted_flag is not None:
                article["impactedServicesAvailable"] = impacted_flag
            if impacted_raw:
                article["impactedServicesRaw"] = impacted_raw

            articles.append(article)

    print(f"  Loaded {len(articles)} retirement rows from workbook CSV")
    if override_count:
        print(f"  Overrode {override_count} workbook retirement dates using linked Azure Updates pages")
    return articles


def _parse_retirement_calendar_sort_date(value):
    """Parse YYYY-MM or YYYY-MM-DD values into a datetime for sorting/calendar use."""
    raw = str(value or "").strip()
    if not raw:
        return None
    match_day = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if match_day:
        return datetime(
            int(match_day.group(1)),
            int(match_day.group(2)),
            int(match_day.group(3)),
            tzinfo=timezone.utc,
        )
    match_month = re.match(r"^(\d{4})-(\d{2})$", raw)
    if match_month:
        return datetime(
            int(match_month.group(1)),
            int(match_month.group(2)),
            1,
            tzinfo=timezone.utc,
        )
    return None


def _normalize_calendar_title_for_dedupe(title):
    """Normalize retirement titles so the same announcement across sources dedupes."""
    value = clean_html(title or "").strip()
    if not value:
        return ""
    value = re.sub(r"^\s*(retirement|deprecation|update)\s*:\s*", "", value, flags=re.IGNORECASE)
    return _normalize_for_match(value)


def _display_calendar_title(title):
    """Normalize display titles by removing repetitive lead-in prefixes."""
    value = clean_html(title or "").strip()
    if not value:
        return "Untitled"
    value = re.sub(r"^\s*(retirement|deprecation|update)\s*:\s*", "", value, flags=re.IGNORECASE)
    return value.strip() or "Untitled"


def build_azure_retirement_calendar(articles, max_items=120):
    """Build a deduplicated, date-sorted list of upcoming retirement announcements."""
    today = datetime.now(timezone.utc).date()
    events_by_key = {}

    for article in articles:
        retirement_date = article.get("azureRetirementDate")
        if not retirement_date:
            continue

        sort_dt = _parse_retirement_calendar_sort_date(retirement_date)
        if not sort_dt:
            continue

        if not _is_retirement_date_future(retirement_date, today=today):
            continue

        title = article.get("title", "Untitled")
        link = article.get("link", "")
        dedupe_key = _azure_retirement_identity_key(title, link)
        update_id = _extract_azure_update_id_from_url(link)
        update_id_key = f"update-id:{update_id}" if update_id else ""
        runtime_alias_key = _azure_runtime_retirement_alias_key(title, retirement_date)
        if dedupe_key.endswith(":"):
            dedupe_key = f"fallback:{_normalize_for_match(title)}"

        precision = "day" if re.match(r"^\d{4}-\d{2}-\d{2}$", retirement_date) else "month"
        source_label = article.get("blog", "") or article.get("blogId", "") or "Source"
        source_report = {
            "blog": article.get("blog", ""),
            "blogId": article.get("blogId", ""),
            "announcementType": article.get("announcementType", ""),
            "link": link,
        }

        existing = events_by_key.get(dedupe_key)
        if not existing and update_id_key:
            existing = events_by_key.get(update_id_key)
        if not existing and runtime_alias_key:
            existing = events_by_key.get(runtime_alias_key)
        if existing:
            existing["sourceReports"].append(source_report)
            if article.get("published", "") > existing.get("published", ""):
                existing["published"] = article.get("published", "")

            replacement = {
                "title": _display_calendar_title(title),
                "link": link,
                "retirementDate": retirement_date,
                "datePrecision": precision,
                "published": article.get("published", ""),
                "blog": article.get("blog", ""),
                "blogId": article.get("blogId", ""),
                "announcementType": article.get("announcementType", ""),
            }
            if _retirement_event_rank_key(replacement) > _retirement_event_rank_key(existing):
                existing["title"] = replacement["title"]
                existing["link"] = replacement["link"] or existing.get("link", "")
                existing["retirementDate"] = replacement["retirementDate"]
                existing["datePrecision"] = replacement["datePrecision"]
                existing["published"] = replacement["published"]
                existing["blog"] = replacement["blog"]
                existing["blogId"] = replacement["blogId"]
                existing["announcementType"] = replacement["announcementType"]

            if existing.get("blogId") != "azureupdates" and article.get("blogId") == "azureupdates":
                existing["blog"] = article.get("blog", "")
                existing["blogId"] = article.get("blogId", "")
                existing["announcementType"] = article.get("announcementType", "")
                existing["link"] = link or existing.get("link", "")
            elif not existing.get("link") and link:
                existing["link"] = link
            existing["sources"] = sorted(
                {
                    src for src in existing.get("sources", []) + [source_label]
                    if src
                }
            )
            events_by_key[dedupe_key] = existing
            if update_id_key:
                events_by_key[update_id_key] = existing
            if runtime_alias_key:
                events_by_key[runtime_alias_key] = existing
            continue

        event = {
            "title": _display_calendar_title(title),
            "link": link,
            "retirementDate": retirement_date,
            "datePrecision": precision,
            "published": article.get("published", ""),
            "blog": article.get("blog", ""),
            "blogId": article.get("blogId", ""),
            "announcementType": article.get("announcementType", ""),
            "sources": [source_label] if source_label else [],
            "sourceReports": [source_report],
        }
        events_by_key[dedupe_key] = event
        if update_id_key:
            events_by_key[update_id_key] = event
        if runtime_alias_key:
            events_by_key[runtime_alias_key] = event

    events = []
    seen_ids = set()
    for event in events_by_key.values():
        event_identity = id(event)
        if event_identity in seen_ids:
            continue
        seen_ids.add(event_identity)
        events.append(event)
    for event in events:
        event["sourceCount"] = len(event.get("sourceReports", []))

    events.sort(
        key=lambda event: (
            _parse_retirement_calendar_sort_date(event.get("retirementDate"))
            or datetime.max.replace(tzinfo=timezone.utc),
            event.get("title", "").lower(),
        )
    )
    return events[:max_items]


def build_retirement_window_buckets(events, today=None, preview_limit=8):
    """Build rolling retirement windows, including long-range future buckets."""
    reference = today or datetime.now(timezone.utc).date()
    window_defs = (
        ("0_3_months", 0, 3),
        ("3_6_months", 3, 6),
        ("6_9_months", 6, 9),
        ("9_12_months", 9, 12),
        ("12_24_months", 12, 24),
        ("24_plus_months", 24, 10_000),
    )
    buckets = {
        key: {
            "label": key.replace("_", "-"),
            "startMonthOffset": start,
            "endMonthOffset": end,
            "count": 0,
            "items": [],
        }
        for key, start, end in window_defs
    }

    for event in events or []:
        retirement_date = event.get("retirementDate", "")
        sort_dt = _parse_retirement_calendar_sort_date(retirement_date)
        if not sort_dt:
            continue

        month_offset = (sort_dt.year - reference.year) * 12 + (sort_dt.month - reference.month)
        if month_offset < 0:
            continue

        for key, start, end in window_defs:
            if start <= month_offset < end:
                bucket = buckets[key]
                bucket["count"] += 1
                if len(bucket["items"]) < preview_limit:
                    bucket["items"].append(
                        {
                            "title": event.get("title", "Untitled"),
                            "link": event.get("link", ""),
                            "retirementDate": retirement_date,
                            "datePrecision": event.get("datePrecision")
                            or _retirement_date_precision(retirement_date)
                            or "month",
                        }
                    )
                break

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "referenceMonth": f"{reference.year:04d}-{reference.month:02d}",
        "windows": buckets,
    }


def generate_rss_feed(articles):
    """Generate an RSS feed XML file from the aggregated articles."""
    doc = Document()
    rss = doc.createElement("rss")
    rss.setAttribute("version", "2.0")
    rss.setAttribute("xmlns:dc", "http://purl.org/dc/elements/1.1/")
    doc.appendChild(rss)

    channel = doc.createElement("channel")
    rss.appendChild(channel)

    def append_text(parent, name, text, use_cdata=False):
        element = doc.createElement(name)
        parent.appendChild(element)
        value = "" if text is None else str(text)
        if use_cdata and "]]>" not in value:
            element.appendChild(doc.createCDATASection(value))
        else:
            element.appendChild(doc.createTextNode(value))

    append_text(channel, "title", "Microsoft Cloud Platform Feed")
    append_text(channel, "link", CANONICAL_SITE_URL)
    append_text(
        channel,
        "description",
        "Aggregated daily updates from Azure and Microsoft 365 blogs",
    )
    append_text(
        channel,
        "lastBuildDate",
        datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT"),
    )
    append_text(channel, "generator", "Microsoft Cloud Platform Feed")
    append_text(channel, "language", "en")

    for article in articles[:50]:
        item = doc.createElement("item")
        channel.appendChild(item)

        append_text(item, "title", article["title"], use_cdata=True)
        append_text(item, "link", article["link"])
        append_text(item, "guid", article["link"])
        append_text(item, "description", article["summary"], use_cdata=True)
        append_text(item, "dc:creator", article["author"], use_cdata=True)
        try:
            dt = datetime.fromisoformat(article["published"])
            append_text(item, "pubDate", dt.strftime("%a, %d %b %Y %H:%M:%S GMT"))
        except (ValueError, TypeError):
            pass
        append_text(item, "category", article["blog"], use_cdata=True)

    xml_str = doc.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")
    output_path = os.path.join("data", "feed.xml")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"RSS feed saved to {output_path}")


def generate_ai_summary(articles):
    """Generate an AI summary using Azure OpenAI over recent publishing days."""
    summary_days = get_recent_publishing_days(articles, SUMMARY_WINDOW_DAYS)
    if not summary_days:
        print("No dated articles available, skipping AI summary")
        return {
            "status": "unavailable",
            "reason": "no_dated_articles",
            "windowDays": SUMMARY_WINDOW_DAYS,
            "publishingDays": [],
        }

    day_articles = get_articles_for_publishing_days(articles, summary_days)
    if not day_articles:
        print("No articles found in configured publishing-day window, skipping AI summary")
        return {
            "status": "unavailable",
            "reason": "no_articles_in_window",
            "windowDays": SUMMARY_WINDOW_DAYS,
            "publishingDays": summary_days,
        }

    api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "")
    deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "")

    required = {
        "AZURE_OPENAI_API_KEY": api_key,
        "AZURE_OPENAI_ENDPOINT": endpoint,
        "AZURE_OPENAI_API_VERSION": api_version,
        "AZURE_OPENAI_DEPLOYMENT": deployment,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        print(
            "Missing Azure OpenAI config ("
            + ", ".join(missing)
            + "), skipping AI summary"
        )
        # Preserve the last known good summary so a local run without credentials
        # never erases the summary that CI previously generated.
        existing_output = os.path.join("data", "feeds.json")
        try:
            with open(existing_output, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if existing.get("summaryStatus") == "available" and existing.get("summary"):
                # Backfill links if an older summary was plain text only.
                preserved_summary = attach_links_to_summary(
                    existing["summary"], day_articles
                )
                print("  Preserving existing AI summary from previous CI run")
                return {
                    "status": "available",
                    "summary": preserved_summary,
                    "source": existing.get("summarySource", "azure-openai"),
                    "windowDays": existing.get("summaryWindowDays", SUMMARY_WINDOW_DAYS),
                    "publishingDays": existing.get("summaryPublishingDays", []),
                    "articleCount": existing.get("summaryArticleCount", 0),
                }
        except (OSError, json.JSONDecodeError, KeyError):
            pass
        return {
            "status": "unavailable",
            "reason": "missing_azure_openai_config",
            "windowDays": SUMMARY_WINDOW_DAYS,
            "publishingDays": summary_days,
        }

    try:
        from openai import AzureOpenAI

        azure_updates_articles = [a for a in day_articles if a.get("blogId") == "azureupdates"]
        other_articles = [a for a in day_articles if a.get("blogId") != "azureupdates"]
        if not azure_updates_articles:
            print("No Azure Updates entries found in window; using all articles for classification")
        ordered_articles = azure_updates_articles + other_articles

        # Phase 1: rule-based lifecycle classification
        code_buckets = {"in_preview": [], "launched_ga": [], "retiring": [], "in_development": []}
        unclassified = []
        for article in ordered_articles:
            bucket = classify_lifecycle(article)
            if bucket and len(code_buckets[bucket]) < MAX_ITEMS_PER_SECTION:
                code_buckets[bucket].append(article)
            else:
                unclassified.append(article)

        code_classified_count = sum(len(v) for v in code_buckets.values())
        print(f"  Rule-based classification: {code_classified_count} items bucketed, {len(unclassified)} unclassified")

        # Phase 2: AI classification for items without deterministic signals
        unclassified_au = [a for a in unclassified if a.get("blogId") == "azureupdates"]
        unclassified_other = [a for a in unclassified if a.get("blogId") != "azureupdates"]
        unclassified_pool = (unclassified_au + unclassified_other)[:MAX_UNCLASSIFIED_FOR_AI]

        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version,
        )
        ai_results = classify_with_ai(unclassified_pool, client, deployment)
        ai_result_by_id = {r["id"]: r for r in ai_results}

        # Phase 3: merge results and render markdown entirely in code
        final_buckets = {"in_preview": [], "launched_ga": [], "retiring": [], "in_development": []}
        for bucket_key, bucket_articles in code_buckets.items():
            for article in bucket_articles:
                label = re.sub(r"^\s*\[[^\]]+\]\s*", "", article.get("title", "")).strip()
                final_buckets[bucket_key].append({"label": label, "link": article.get("link", "")})

        for i, article in enumerate(unclassified_pool):
            ai = ai_result_by_id.get(str(i))
            if not ai or ai["bucket"] == "other":
                continue
            bucket_key = ai["bucket"]
            if len(final_buckets[bucket_key]) >= MAX_ITEMS_PER_SECTION:
                continue
            label = ai["label"] or re.sub(r"^\s*\[[^\]]+\]\s*", "", article.get("title", "")).strip()
            final_buckets[bucket_key].append({"label": label, "link": article.get("link", "")})

        summary_article_count = sum(len(v) for v in final_buckets.values())
        summary = render_summary_markdown(final_buckets)
        print(f"AI summary generated: {summary[:100]}...")
        return {
            "status": "available",
            "summary": summary,
            "source": "azure-openai",
            "windowDays": len(summary_days),
            "publishingDays": summary_days,
            "articleCount": summary_article_count,
        }

    except Exception as e:
        error_msg = str(e)
        print(
            "AI summary failed "
            "(check Azure OpenAI auth, AZURE_OPENAI_API_VERSION, "
            f"and AZURE_OPENAI_DEPLOYMENT): {error_msg}"
        )
        return {
            "status": "unavailable",
            "reason": "azure_openai_failed",
            "windowDays": SUMMARY_WINDOW_DAYS,
            "publishingDays": summary_days,
        }


def load_previous_article_count(path):
    """Return prior article count from feeds.json, or None when unavailable."""
    return shared_load_previous_article_count(path, logger=print)


def evaluate_publish_failsafe(
    new_count,
    previous_count,
    min_articles=FAILSAFE_MIN_ARTICLES,
    min_ratio=FAILSAFE_MIN_RATIO,
):
    """Return (triggered, details) for publish fail-safe guard logic."""
    return shared_evaluate_publish_failsafe(
        new_count,
        previous_count,
        min_articles=min_articles,
        min_ratio=min_ratio,
    )


def _build_retirement_run_metrics(retirement_calendar, retirement_buckets):
    """Return retirement coverage metrics for CI observability."""
    events = retirement_calendar or []
    windows = (retirement_buckets or {}).get("windows", {})

    unique_sources = set()
    for event in events:
        source_list = event.get("sources") or []
        if source_list:
            unique_sources.update(src for src in source_list if src)
        else:
            fallback = event.get("blog")
            if fallback:
                unique_sources.add(fallback)

    return {
        "retirementTotalCount": len(events),
        "retirementSourceCount": len(unique_sources),
        "retirementWindowCounts": {
            key: int((value or {}).get("count", 0))
            for key, value in windows.items()
        },
    }


def build_run_metrics(
    raw_article_count,
    unique_article_count,
    previous_article_count,
    failsafe_triggered,
    failsafe_details,
    published,
    summary_payload,
    savill_video,
    retirement_calendar=None,
    retirement_buckets=None,
):
    """Build the core observability payload for a fetch run."""
    summary_data = summary_payload or {}
    metrics = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "rawArticleCount": raw_article_count,
        "uniqueArticleCount": unique_article_count,
        "previousArticleCount": previous_article_count,
        "failsafeTriggered": failsafe_triggered,
        "failsafeDetails": failsafe_details,
        "published": published,
        "summaryStatus": summary_data.get("status"),
        "summaryReason": summary_data.get("reason"),
        "summaryArticleCount": summary_data.get("articleCount"),
        "savillVideoFound": bool(savill_video),
    }
    metrics.update(
        _build_retirement_run_metrics(retirement_calendar, retirement_buckets)
    )
    return metrics


def write_run_metrics(metrics, output_path=None):
    """Write run metrics to the configured path without failing the run."""
    path = output_path
    if path is None:
        path = os.environ.get(RUN_METRICS_ENV_VAR, "")
    path = (path or "").strip()
    if not path:
        return False

    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)
        print(f"Run metrics saved to {path}")
        return True
    except (OSError, TypeError, ValueError) as e:
        print(f"Warning: failed to write run metrics to {path}: {e}")
        return False


def main():
    print("=" * 60)
    print("Microsoft Cloud Platform Feed - Fetching RSS Feeds")
    print("=" * 60)

    all_articles = []
    all_articles.extend(fetch_tech_community_feeds())
    all_articles.extend(fetch_aks_blog())
    all_articles.extend(fetch_devblogs_feeds())
    all_articles.extend(fetch_azure_updates_feed())
    all_articles.extend(fetch_aztty_announcements())
    all_articles.extend(fetch_azure_retirements_from_csv())
    raw_article_count = len(all_articles)

    # Fetch Savill video (independent of article feeds)
    savill_video = fetch_savill_video()

    # Sort by date, newest first
    all_articles.sort(key=lambda x: x.get("published", ""), reverse=True)

    # Remove duplicates and discard articles older than 30 days
    unique_articles = dedupe_articles(all_articles)
    retirement_calendar = build_azure_retirement_calendar(unique_articles)
    retirement_buckets = build_retirement_window_buckets(retirement_calendar)

    discarded = len(all_articles) - len(unique_articles)
    if discarded:
        print(f"Filtered out {discarded} duplicate/older-than-30-days articles")

    output_path = os.path.join("data", "feeds.json")
    previous_count = load_previous_article_count(output_path)
    failsafe_triggered, failsafe_details = evaluate_publish_failsafe(
        len(unique_articles), previous_count
    )
    if previous_count is None:
        print("Publish fail-safe bypassed due to missing or unreadable baseline")
    elif failsafe_triggered:
        print("Publish fail-safe triggered; skipping output write to preserve last good data")
        print(f"  {failsafe_details}")
        run_metrics = build_run_metrics(
            raw_article_count=raw_article_count,
            unique_article_count=len(unique_articles),
            previous_article_count=previous_count,
            failsafe_triggered=True,
            failsafe_details=failsafe_details,
            published=False,
            summary_payload=None,
            savill_video=savill_video,
            retirement_calendar=retirement_calendar,
            retirement_buckets=retirement_buckets,
        )
        write_run_metrics(run_metrics)
        return
    else:
        print("Publish fail-safe check passed")
        print(f"  {failsafe_details}")

    # Generate AI summary (optional)
    summary_payload = generate_ai_summary(unique_articles)

    data = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "totalArticles": len(unique_articles),
        "articles": unique_articles,
        "summaryWindowDays": summary_payload.get("windowDays", SUMMARY_WINDOW_DAYS),
        "azureRetirementCalendar": retirement_calendar,
        "azureRetirementBuckets": retirement_buckets,
    }
    if summary_payload.get("publishingDays"):
        data["summaryPublishingDays"] = summary_payload["publishingDays"]
    if summary_payload.get("status"):
        data["summaryStatus"] = summary_payload["status"]
    if summary_payload.get("source"):
        data["summarySource"] = summary_payload["source"]
    if summary_payload.get("articleCount") is not None:
        data["summaryArticleCount"] = summary_payload["articleCount"]
    if summary_payload.get("reason"):
        reason = summary_payload["reason"]
        if reason in PUBLIC_SUMMARY_REASONS:
            data["summaryReason"] = reason
    if summary_payload.get("summary"):
        data["summary"] = summary_payload["summary"]
    if savill_video:
        data["savillVideo"] = savill_video

    os.makedirs("data", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Generate RSS feed
    generate_rss_feed(unique_articles)

    write_checksums_file()

    run_metrics = build_run_metrics(
        raw_article_count=raw_article_count,
        unique_article_count=len(unique_articles),
        previous_article_count=previous_count,
        failsafe_triggered=False,
        failsafe_details=failsafe_details,
        published=True,
        summary_payload=summary_payload,
        savill_video=savill_video,
        retirement_calendar=retirement_calendar,
        retirement_buckets=retirement_buckets,
    )
    write_run_metrics(run_metrics)

    print(f"\n{'=' * 60}")
    print(f"Done! {len(unique_articles)} unique articles saved to {output_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
