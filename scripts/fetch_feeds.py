#!/usr/bin/env python3
"""
Azure News Feed - RSS Feed Fetcher
Fetches articles from Azure blog RSS feeds and generates a JSON data file.
"""

import feedparser
import json
import os
import re
import time
import requests
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from requests.adapters import HTTPAdapter
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib3.util.retry import Retry

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
SAVILL_YOUTUBE_RSS = (
    "https://www.youtube.com/feeds/videos.xml"
    "?channel_id=UCpIn7ox7j7bH_OFj7tYouOQ"
)
SUMMARY_WINDOW_DAYS = 7
MAX_ITEMS_PER_SECTION = 5
MAX_UNCLASSIFIED_FOR_AI = 20
LIFECYCLE_SECTIONS = {
    "in_preview": "In preview",
    "launched_ga": "Launched / Generally Available",
    "in_development": "In development",
}
BULLET_PREFIX = "  \u2022 "
SECTION_HEADING_PREFIX = "- "
FALLBACK_BULLET = "none noted in selected window"
FEED_REQUEST_TIMEOUT = (5, 20)
FEED_RETRY_TOTAL = 2
FEED_BACKOFF_FACTOR = 1
FEED_USER_AGENT = "AzureFeedBot/1.0 (+https://azurefeed.news)"
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
    host = (hostname or "").strip().lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def build_allowed_feed_hosts():
    """Return the set of remote hosts that are allowed for feed retrieval."""
    source_urls = [
        TC_RSS_URL.format(board="azurecompute"),
        AKS_BLOG_FEED,
        AZURE_UPDATES_FEED,
        SAVILL_YOUTUBE_RSS,
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
    retry = Retry(
        total=FEED_RETRY_TOTAL,
        connect=FEED_RETRY_TOTAL,
        read=FEED_RETRY_TOTAL,
        status=FEED_RETRY_TOTAL,
        backoff_factor=FEED_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": FEED_USER_AGENT})
    return session


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
    """Return the most recent publishing days found in article published values."""
    published_days = sorted(
        {
            article.get("published", "")[:10]
            for article in articles
            if re.match(r"\d{4}-\d{2}-\d{2}", article.get("published", ""))
        },
        reverse=True,
    )
    return published_days[:max_days]


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
    raw_url = (url or "").strip()
    if not raw_url:
        return ""

    parsed = urlsplit(raw_url)
    scheme = parsed.scheme.lower()
    host = normalize_host(parsed.hostname)
    if not scheme or not host:
        return raw_url

    port = parsed.port
    netloc = host
    if port and DEFAULT_PORTS.get(scheme) != port:
        netloc = f"{host}:{port}"

    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")

    filtered_query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        normalized_key = key.lower()
        if normalized_key.startswith(TRACKING_QUERY_PREFIXES):
            continue
        if normalized_key in TRACKING_QUERY_KEYS:
            continue
        filtered_query.append((key, value))

    query = urlencode(sorted(filtered_query))
    return urlunsplit((scheme, netloc, path, query, ""))


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

    Returns 'in_preview', 'launched_ga', 'in_development', or None when
    no deterministic signal is present.
    """
    title = (article.get("title") or "").lower()
    # Check in_development first to avoid false GA matches on retirement notices
    if re.search(r"\[in development\]|in development|coming soon|retir|deprecat", title):
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
        'Bucket must be exactly one of: in_preview, launched_ga, in_development, other. '
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
        valid_buckets = {"in_preview", "launched_ga", "in_development", "other"}
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


def fetch_tech_community_feeds():
    """Fetch articles from Tech Community blogs."""
    articles = []

    for board_id, blog_name in BLOGS.items():
        url = TC_RSS_URL.format(board=board_id)
        print(f"Fetching: {blog_name} ({board_id})...")

        try:
            feed = fetch_feed(url)

            if feed.bozo and not feed.entries:
                print(f"  Warning: Could not parse feed for {blog_name}")
                continue

            count = 0
            for entry in feed.entries:
                summary = clean_html(entry.get("summary", ""))
                articles.append(
                    {
                        "title": clean_html(entry.get("title", "Untitled")),
                        "link": entry.get("link", ""),
                        "published": parse_date(entry),
                        "summary": truncate(summary),
                        "blog": blog_name,
                        "blogId": board_id,
                        "author": entry.get("author", "Microsoft"),
                    }
                )
                count += 1

            print(f"  Found {count} articles")

        except Exception as e:
            print(f"  Error fetching {blog_name}: {e}")

        time.sleep(0.5)

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

    except Exception as e:
        print(f"  Error fetching AKS blog: {e}")

    return articles


def fetch_devblogs_feeds():
    """Fetch articles from Microsoft DevBlogs."""
    articles = []

    for blog_id, (blog_name, feed_url) in DEVBLOGS.items():
        print(f"Fetching: {blog_name}...")

        try:
            feed = fetch_feed(feed_url)

            if feed.bozo and not feed.entries:
                print(f"  Warning: Could not parse {blog_name} feed")
                continue

            count = 0
            for entry in feed.entries:
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
                count += 1

            print(f"  Found {count} articles")

        except Exception as e:
            print(f"  Error fetching {blog_name}: {e}")

        time.sleep(0.5)

    return articles


def fetch_savill_video():
    """Fetch John Savill's latest Azure Infrastructure Update video from YouTube RSS."""
    print("Fetching: John Savill YouTube channel...")
    try:
        feed = fetch_feed(SAVILL_YOUTUBE_RSS)
        if not feed.entries:
            print("  Warning: No entries in Savill YouTube feed")
            return None

        def match_score(entry):
            t = entry.get("title", "").lower()
            if "azure infrastructure update" in t:
                return 3
            if "azure" in t and "infrastructure" in t:
                return 2
            if "azure" in t and "update" in t:
                return 1
            return 0

        # Sort by score descending; entries are already newest-first so first
        # high-scoring entry wins when scores are equal.
        best = max(feed.entries, key=match_score)
        if match_score(best) == 0:
            best = feed.entries[0]  # fallback: latest video regardless of topic

        link = best.get("link", "")

        # Extract thumbnail: prefer media:thumbnail element, fall back to ytimg URL
        thumbnail = ""
        media_thumbs = getattr(best, "media_thumbnail", None) or best.get("media_thumbnail", [])
        if media_thumbs:
            thumbnail = media_thumbs[0].get("url", "")
        if not thumbnail:
            m = re.search(r"[?&]v=([A-Za-z0-9_-]+)", link)
            if m:
                thumbnail = f"https://i.ytimg.com/vi/{m.group(1)}/hqdefault.jpg"

        result = {
            "title": clean_html(best.get("title", "")),
            "url": link,
            "published": parse_date(best),
            "thumbnail": thumbnail,
        }
        print(f"  Found: {result['title'][:70]}")
        return result
    except Exception as e:
        print(f"  Error fetching Savill YouTube: {e}")
        return None


def fetch_azure_updates_feed():
    """Fetch articles from Azure Updates RSS feed."""
    articles = []
    print("Fetching: Azure Updates...")

    try:
        feed = fetch_feed(AZURE_UPDATES_FEED)

        if feed.bozo and not feed.entries:
            print("  Warning: Could not parse Azure Updates feed")
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

        print(f"  Found {count} articles")

    except Exception as e:
        print(f"  Error fetching Azure Updates feed: {e}")

    return articles


def generate_rss_feed(articles):
    """Generate an RSS feed XML file from the aggregated articles."""
    from xml.etree.ElementTree import Element, SubElement, tostring

    rss = Element("rss", version="2.0")
    rss.set("xmlns:dc", "http://purl.org/dc/elements/1.1/")
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = "Azure News Feed"
    SubElement(channel, "link").text = "https://azurefeed.news"
    SubElement(channel, "description").text = (
        "Aggregated daily news from Azure blogs"
    )
    SubElement(channel, "lastBuildDate").text = datetime.now(
        timezone.utc
    ).strftime("%a, %d %b %Y %H:%M:%S GMT")
    SubElement(channel, "generator").text = "Azure News Feed"
    SubElement(channel, "language").text = "en"

    for article in articles[:50]:
        item = SubElement(channel, "item")
        SubElement(item, "title").text = article["title"]
        SubElement(item, "link").text = article["link"]
        SubElement(item, "guid").text = article["link"]
        SubElement(item, "description").text = article["summary"]
        SubElement(item, "dc:creator").text = article["author"]
        try:
            dt = datetime.fromisoformat(article["published"])
            SubElement(item, "pubDate").text = dt.strftime(
                "%a, %d %b %Y %H:%M:%S GMT"
            )
        except (ValueError, TypeError):
            pass
        SubElement(item, "category").text = article["blog"]

    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(
        rss, encoding="unicode"
    )
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
        code_buckets = {"in_preview": [], "launched_ga": [], "in_development": []}
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
        final_buckets = {"in_preview": [], "launched_ga": [], "in_development": []}
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


def main():
    print("=" * 60)
    print("Azure News Feed - Fetching RSS Feeds")
    print("=" * 60)

    all_articles = []
    all_articles.extend(fetch_tech_community_feeds())
    all_articles.extend(fetch_aks_blog())
    all_articles.extend(fetch_devblogs_feeds())
    all_articles.extend(fetch_azure_updates_feed())

    # Fetch Savill video (independent of article feeds)
    savill_video = fetch_savill_video()

    # Sort by date, newest first
    all_articles.sort(key=lambda x: x.get("published", ""), reverse=True)

    # Remove duplicates and discard articles older than 30 days
    unique_articles = dedupe_articles(all_articles)

    discarded = len(all_articles) - len(unique_articles)
    if discarded:
        print(f"Filtered out {discarded} duplicate/older-than-30-days articles")

    # Generate AI summary (optional)
    summary_payload = generate_ai_summary(unique_articles)

    data = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "totalArticles": len(unique_articles),
        "articles": unique_articles,
        "summaryWindowDays": summary_payload.get("windowDays", SUMMARY_WINDOW_DAYS),
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
    output_path = os.path.join("data", "feeds.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Generate RSS feed
    generate_rss_feed(unique_articles)

    print(f"\n{'=' * 60}")
    print(f"Done! {len(unique_articles)} unique articles saved to {output_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
