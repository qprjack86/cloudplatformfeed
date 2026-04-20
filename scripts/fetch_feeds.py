#!/usr/bin/env python3
"""
Microsoft Cloud Platform Feed - RSS Feed Fetcher
Fetches articles from Azure and Microsoft 365 blog RSS feeds and generates a JSON data file.
"""

import feedparser
import csv
import json
import os
import sys
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
    build_checksums_payload,
    write_checksums_file as shared_write_checksums_file,
    evaluate_publish_failsafe,
    extract_youtube_video_id as _extract_youtube_video_id,
    build_youtube_thumbnail_from_video_url as _build_youtube_thumbnail_from_video_url,
    load_previous_article_count,
    load_site_config,
    normalize_host,
    resolve_youtube_channel_id_from_seed as _resolve_youtube_channel_id_from_seed,
    select_best_youtube_video_entry as _select_best_youtube_video_entry,
    validate_feed_data,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_CONFIG_PATH = REPO_ROOT / "config" / "site.json"

SITE_CONFIG = load_site_config(SITE_CONFIG_PATH)
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
WORKBOOK_BLOG_ID = "azureretirements"
MICROSOFT_LIFECYCLE_BLOG_ID = "microsoftlifecycle"
MICROSOFT_LIFECYCLE_TAG_API = "https://endoflife.date/api/v1/tags/microsoft"
DEFAULT_MICROSOFT_LIFECYCLE_PRODUCTS = [
    "windows-server",
    "mssqlserver",
    "msexchange",
    "sharepoint",
    "dotnet",
    "dotnetfx",
    "powershell",
    "visual-studio",
]
DEFAULT_MICROSOFT_LIFECYCLE_MILESTONES = ["eoas", "eol", "eoes_start", "eoes"]
DEFAULT_MICROSOFT_LIFECYCLE_EVENT_CAP = 120
DEFAULT_RETIREMENT_CALENDAR_EVENT_CAP = 500


CHECKSUM_ARTIFACTS = [
    Path("data") / "feeds.json",
    Path("data") / "feed.xml",
    Path("data") / "azure-retirements.ics",
]
CHECKSUM_OUTPUT_PATH = Path("data") / "checksums.json"
AZURE_RETIREMENTS_ICS_PATH = Path("data") / "azure-retirements.ics"


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
def build_allowed_feed_hosts():
    """Return the set of remote hosts that are allowed for feed retrieval."""
    source_urls = [
        TC_RSS_URL.format(board="azurecompute"),
        AKS_BLOG_FEED,
        AZURE_UPDATES_FEED,
        AZURE_UPDATES_API,
        AZTTY_DEPRECATIONS_FEED,
        AZTTY_UPDATES_FEED,
        MICROSOFT_LIFECYCLE_TAG_API,
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


def _load_microsoft_lifecycle_config(config_path=SITE_CONFIG_PATH):
    """Load curated Microsoft lifecycle settings from config/site.json."""
    config = {
        "enabled": True,
        "products": list(DEFAULT_MICROSOFT_LIFECYCLE_PRODUCTS),
        "milestones": list(DEFAULT_MICROSOFT_LIFECYCLE_MILESTONES),
        "maxEvents": DEFAULT_MICROSOFT_LIFECYCLE_EVENT_CAP,
    }

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        lifecycle = raw.get("microsoftLifecycle", {})
        if isinstance(lifecycle, dict):
            if "enabled" in lifecycle:
                config["enabled"] = bool(lifecycle.get("enabled"))

            products = lifecycle.get("products")
            if isinstance(products, list):
                config["products"] = [
                    str(item).strip().lower()
                    for item in products
                    if str(item).strip()
                ]

            milestones = lifecycle.get("milestones")
            if isinstance(milestones, list):
                normalized = []
                for milestone in milestones:
                    key = str(milestone or "").strip().lower()
                    if key in {"eoas", "eol", "eoes_start", "eoes"} and key not in normalized:
                        normalized.append(key)
                if normalized:
                    config["milestones"] = normalized

            max_events = lifecycle.get("maxEvents")
            if isinstance(max_events, int) and max_events > 0:
                config["maxEvents"] = min(max_events, 500)
    except (OSError, ValueError, TypeError) as exc:
        print(f"Warning: could not load microsoftLifecycle config: {exc}")

    return config


MICROSOFT_LIFECYCLE_CONFIG = _load_microsoft_lifecycle_config()


def _load_retirement_category_mappings(config_path=SITE_CONFIG_PATH):
    """Load source category mappings from config/site.json."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        mappings = raw.get("categoryMappings", {})
        if not isinstance(mappings, dict):
            return {}, {}
        azure = mappings.get("azure", {})
        m365 = mappings.get("m365", {})
        return (
            azure if isinstance(azure, dict) else {},
            m365 if isinstance(m365, dict) else {},
        )
    except (OSError, ValueError, TypeError) as exc:
        print(f"Warning: could not load categoryMappings config: {exc}")
        return {}, {}


AZURE_RETIREMENT_CATEGORY_MAPPINGS, M365_RETIREMENT_CATEGORY_MAPPINGS = _load_retirement_category_mappings()
MICROSOFT_LIFECYCLE_CATEGORY_OVERRIDES = {
    "mssqlserver": "Data & AI",
    "dotnet": "Apps & Platform",
    "dotnetfx": "Apps & Platform",
    "powershell": "Operations",
    "windows-server": "Infrastructure",
    "sharepoint": "Apps & Platform",
    "msexchange": "Apps & Platform",
    "visual-studio": "Operations",
}


def _normalize_category_match_text(text):
    """Normalize text used by category keyword matching."""
    value = clean_html(str(text or "")).lower()
    value = _split_camel_case(value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _category_from_mapping(search_text, mapping, fallback="Other"):
    """Resolve a category from mapping keywords using case-insensitive contains matching."""
    haystack = _normalize_category_match_text(search_text)
    if not isinstance(mapping, dict):
        return fallback

    for category, keywords in mapping.items():
        if not isinstance(keywords, list):
            continue
        for keyword in keywords:
            needle = _normalize_category_match_text(keyword)
            if not needle:
                continue
            if needle in haystack:
                return str(category)
    return fallback


def _categorize_retirement_article(article):
    """Return a best-effort category for a retirement source article."""
    article = article or {}
    source = str(article.get("_source", "")).strip().lower()
    blog_id = str(article.get("blogId", "")).strip().lower()

    if source == "m365" or blog_id == "m365":
        m365_category = str(article.get("m365Category") or "").strip()
        if m365_category:
            return m365_category
        service = article.get("m365Service", "")
        title = article.get("title", "")
        summary = article.get("summary", "")
        return _category_from_mapping(f"{service} {title} {summary}", M365_RETIREMENT_CATEGORY_MAPPINGS)

    if source == "microsoft" or blog_id == MICROSOFT_LIFECYCLE_BLOG_ID:
        product = article.get("lifecycleProduct", "")
        release = article.get("lifecycleRelease", "")
        title = article.get("title", "")
        override = MICROSOFT_LIFECYCLE_CATEGORY_OVERRIDES.get(str(product or "").strip().lower())
        if override:
            return override
        return _category_from_mapping(f"{product} {release} {title}", AZURE_RETIREMENT_CATEGORY_MAPPINGS)

    title = article.get("title", "")
    summary = article.get("summary", "")
    blog = article.get("blog", "")
    return _category_from_mapping(
        f"{blog_id} {blog} {title} {summary}",
        AZURE_RETIREMENT_CATEGORY_MAPPINGS,
    )


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



def _ics_escape_text(value):
    """Escape text for iCalendar property values (RFC 5545)."""
    raw = str(value or "")
    raw = raw.replace("\\", "\\\\")
    raw = raw.replace(";", "\\;")
    raw = raw.replace(",", "\\,")
    raw = raw.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
    return raw


def _ics_fold_line(line, limit=75):
    """Fold long iCalendar lines to improve client compatibility."""
    if len(line) <= limit:
        return line
    parts = [line[:limit]]
    remainder = line[limit:]
    while remainder:
        parts.append(" " + remainder[: limit - 1])
        remainder = remainder[limit - 1 :]
    return "\r\n".join(parts)


def _ics_uid_for_event(event):
    """Build a deterministic UID for an Azure retirement calendar event."""
    title_token = re.sub(r"\s+", "-", _normalize_for_match(event.get("title", "untitled"))) or "untitled"
    date_token = re.sub(r"[^0-9]", "", str(event.get("retirementDate", ""))) or "nodate"
    link = event.get("link", "")
    update_id = _extract_azure_update_id_from_url(link)
    suffix = update_id or title_token[:40]
    return f"azure-retirement-{date_token}-{suffix}@{CANONICAL_SITE_HOST}"


def _ics_date_fields(retirement_date, precision):
    """Return DTSTART/DTEND fields for a retirement event."""
    parsed = _parse_retirement_calendar_sort_date(retirement_date)
    if not parsed:
        return None

    start = parsed.date()
    end = start + timedelta(days=1)

    return {
        "dtstart": start.strftime("%Y%m%d"),
        "dtend": end.strftime("%Y%m%d"),
    }


def _generate_retirements_ics(
    events,
    *,
    prodid,
    cal_name,
    cal_desc,
    uid_builder,
    include_source=False,
    generated_at=None,
):
    """Generate an ICS calendar payload for retirement events."""
    stamp = generated_at or datetime.now(timezone.utc)
    dtstamp = stamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{prodid}",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{_ics_escape_text(cal_name)}",
        f"X-WR-CALDESC:{_ics_escape_text(cal_desc)}",
    ]

    for event in events:
        retirement_date = str(event.get("retirementDate", "")).strip()
        precision = event.get("datePrecision") or _retirement_date_precision(retirement_date)
        date_fields = _ics_date_fields(retirement_date, precision)
        if not date_fields:
            continue

        sources = event.get("sources", [])
        source_label = ", ".join(str(src) for src in sources if src)
        description_lines = []
        if include_source:
            description_lines.append(f"Source: {event.get('source', 'unknown')}")
        description_lines.extend(
            [
                f"Retirement date: {retirement_date}",
                f"Date precision: {precision}",
            ]
        )
        if source_label:
            description_lines.append(f"Sources: {source_label}")
        if precision != "day":
            description_lines.append("This event uses month-level precision in source data.")

        summary = event.get("title", "Untitled retirement notice")
        link = event.get("link", "")

        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{_ics_escape_text(uid_builder(event))}",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART;VALUE=DATE:{date_fields['dtstart']}",
                f"DTEND;VALUE=DATE:{date_fields['dtend']}",
                f"SUMMARY:{_ics_escape_text(summary)}",
                f"DESCRIPTION:{_ics_escape_text('\\n'.join(description_lines))}",
                f"URL:{_ics_escape_text(link)}" if link else "",
                "END:VEVENT",
            ]
        )

    lines.append("END:VCALENDAR")
    folded = [_ics_fold_line(line) for line in lines if line]
    return "\r\n".join(folded) + "\r\n"


def generate_azure_retirements_ics(events, generated_at=None):
    """Generate an ICS calendar payload for Azure retirement events."""
    return _generate_retirements_ics(
        events,
        prodid="-//Cloud Platform Feed//Azure Impact Lifecycle Calendar//EN",
        cal_name="Azure Impact Lifecycle Calendar",
        cal_desc="Upcoming Azure and curated Microsoft lifecycle retirements from Cloud Platform Feed",
        uid_builder=_ics_uid_for_event,
        include_source=False,
        generated_at=generated_at,
    )


def write_azure_retirements_ics(events, output_path=AZURE_RETIREMENTS_ICS_PATH, generated_at=None):
    """Write Azure retirement events as an ICS artifact."""
    payload = generate_azure_retirements_ics(events, generated_at=generated_at)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    print(f"Azure ICS calendar written to {output_path}")
    return True


def _ics_uid_for_unified_event(event):
    """Build a deterministic UID for a unified retirement calendar event."""
    source = event.get("source", "unknown")
    title_token = re.sub(r"\s+", "-", _normalize_for_match(event.get("title", "untitled"))) or "untitled"
    date_token = re.sub(r"[^0-9]", "", str(event.get("retirementDate", ""))) or "nodate"
    link = event.get("link", "")
    update_id = _extract_azure_update_id_from_url(link) if link else None
    
    # Different UID prefixes based on source
    if source == "azure":
        suffix = update_id or title_token[:40]
        return f"retirement-azure-{date_token}-{suffix}@{CANONICAL_SITE_HOST}"
    elif source == "microsoft":
        suffix = title_token[:40]
        return f"retirement-microsoft-{date_token}-{suffix}@{CANONICAL_SITE_HOST}"
    else:  # m365
        suffix = title_token[:40]
        return f"retirement-m365-{date_token}-{suffix}@{CANONICAL_SITE_HOST}"


def generate_unified_retirements_ics(events, generated_at=None):
    """Generate an ICS calendar payload for all unified retirement events."""
    return _generate_retirements_ics(
        events,
        prodid="-//Cloud Platform Feed//Unified Retirement Calendar//EN",
        cal_name="Unified Retirement Calendar",
        cal_desc="All Azure, Microsoft, and Microsoft 365 retirement events from Cloud Platform Feed",
        uid_builder=_ics_uid_for_unified_event,
        include_source=True,
        generated_at=generated_at,
    )


def write_unified_retirements_ics(events, output_path="data/retirements.ics", generated_at=None):
    """Write unified retirement events as an ICS artifact."""
    payload = generate_unified_retirements_ics(events, generated_at=generated_at)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    print(f"Unified ICS calendar written to {output_path}")
    return True

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


# Words that are too common to be used as distinctive identity tokens for cross-source dedupe.
_CALENDAR_DEDUPE_STOP_WORDS = frozenset({
    # Articles, conjunctions, prepositions
    "the", "and", "for", "to", "in", "on", "by", "of", "or", "an", "a",
    "into", "from", "with", "than", "about",
    # Auxiliary / common verbs
    "will", "be", "is", "are", "was", "been", "has", "have", "had",
    "do", "does", "did", "can", "could", "may", "would", "should",
    "use", "used", "using", "need", "know",
    # Pronouns / determiners
    "you", "your", "our", "we", "it", "its", "this", "that", "these",
    "those", "them", "they", "their", "any", "all", "some", "each",
    "what", "how", "when", "where", "who", "which",
    # Adverbs / discourse words
    "not", "but", "just", "also", "more", "other", "now", "then",
    "please", "before", "after", "only", "still", "soon", "get",
    # Retirement-specific noise words (appear in most retirement titles)
    "retirement", "retiring", "retired", "deprecation", "deprecated",
    "end", "notice", "reminder", "migration", "migrate", "transition",
    "upgrade", "announcement", "announcing", "announces", "begins", "beginning",
    "update", "updates", "life", "review", "move", "switch", "action", "required",
    # Very common vendor context (adds no discrimination)
    "azure", "microsoft",
    # Months (appear in date references)
    "january", "february", "march", "april", "june", "july", "august",
    "september", "october", "november", "december",
})


def _split_camel_case(text):
    """Insert spaces around CamelCase / PascalCase boundaries."""
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)
    return text


def _calendar_identity_tokens(title):
    """Extract distinctive identity tokens from a retirement calendar title.

    Returns a frozenset of lower-cased, punctuation-stripped tokens (length >= 3)
    that survive stop-word filtering.  Used for cross-source same-date fuzzy dedupe.
    """
    value = clean_html(title or "")
    # Strip leading 'Retirement:' / 'Deprecation:' prefixes
    value = re.sub(r"^\s*(retirement|deprecation|update)\s*:\s*", "", value, flags=re.IGNORECASE)
    # Expand CamelCase so 'ContainerLog' → 'Container Log', 'GetAlertSummary' → 'Get Alert Summary'
    value = _split_camel_case(value)
    # Remove years and standalone numbers (dates add noise)
    value = re.sub(r"\b20\d{2}\b", " ", value)
    value = re.sub(r"\b\d+\b", " ", value)
    # Lowercase and strip non-alphanumeric
    value = _normalize_for_match(value)
    tokens = frozenset(
        w for w in value.split()
        if len(w) >= 3 and w not in _CALENDAR_DEDUPE_STOP_WORDS
    )
    return tokens


def _tokens_are_same_event(tokens_a, tokens_b):
    """Return True when two token sets look like the same retirement event.

    Requires at least 2 overlapping tokens.  A single shared token is not
    sufficient because common Azure infrastructure words (e.g. 'virtual', 'service',
    'container') appear across many unrelated service titles and would cause
    false-positive merges.
    """
    if not tokens_a or not tokens_b:
        return False
    overlap = tokens_a & tokens_b
    return len(overlap) >= 2


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


def _extract_openai_message_text(message):
    """Return a plain-text payload from chat completion message content."""
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""

    fragments = []
    for part in content:
        if isinstance(part, str):
            text = part
        elif isinstance(part, dict):
            text = part.get("text", "")
            if isinstance(text, dict):
                text = text.get("value", "")
        else:
            text = getattr(part, "text", "")
            if not isinstance(text, str):
                text = getattr(text, "value", "")
        if isinstance(text, str) and text.strip():
            fragments.append(text.strip())
    
    result = "\n".join(fragments)
    if not result:
        try:
            print(f"  [DEBUG] Empty response extraction; content type={type(content).__name__}, length={len(content) if isinstance(content, list) else 'N/A'}", file=sys.stderr)
            if isinstance(content, list) and content:
                print(f"  [DEBUG] First part: type={type(content[0]).__name__}, value={str(content[0])[:100]}", file=sys.stderr)
        except Exception:
            pass
    return result


def _parse_openai_json_payload(raw_text):
    """Parse a JSON object from a chat completion text payload."""
    raw = (raw_text or "").strip()
    if not raw:
        try:
            print(f"  [DEBUG] Empty raw_text passed to JSON parser; input was: {repr(raw_text)[:200]}", file=sys.stderr)
        except Exception:
            pass
        raise ValueError("empty response content")

    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            try:
                print(f"  [DEBUG] JSON parse failed and no JSON object found; raw={repr(raw)[:200]}, error={e}", file=sys.stderr)
            except Exception:
                pass
            raise
        return json.loads(match.group(0))


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
        raw = _extract_openai_message_text(response.choices[0].message)
        if not raw:
            print(f"  AI classifier received empty message content; falling back to rule-based")
            return []
        parsed = _parse_openai_json_payload(raw)
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


def parse_updated_date(entry):
    """Parse explicit updated timestamp from feed entry, when present."""
    parsed = entry.get("updated_parsed")
    if parsed:
        try:
            dt = datetime(*parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except (ValueError, TypeError):
            pass

    date_str = entry.get("updated", "")
    if date_str:
        try:
            dt = parsedate_to_datetime(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError, IndexError, OverflowError):
            pass

    return ""


def _is_later_timestamp(updated_iso, published_iso):
    """Return True when updated timestamp is later than published timestamp."""
    updated_dt = parse_iso_datetime(updated_iso)
    published_dt = parse_iso_datetime(published_iso)
    if not updated_dt or not published_dt:
        return False
    return updated_dt > published_dt


def _build_article_record(
    *,
    title,
    link,
    published,
    summary,
    blog,
    blog_id,
    author="Microsoft",
    lifecycle_state=None,
    date_precision=None,
    extra_fields=None,
):
    """Build a normalized article payload with optional lifecycle fields."""
    article = {
        "title": title,
        "link": link,
        "published": published,
        "summary": summary,
        "blog": blog,
        "blogId": blog_id,
        "author": author,
    }
    if lifecycle_state is not None:
        article["lifecycleState"] = lifecycle_state
    if date_precision is not None:
        article["datePrecision"] = date_precision
    if extra_fields:
        article.update(extra_fields)
    return article


def _entries_to_articles(entries, blog_name, blog_id):
    """Convert feed entries into article payloads with lifecycle and precision fields."""
    articles = []
    for entry in entries:
        summary = clean_html(entry.get("summary", ""))
        published = parse_date(entry)
        updated = parse_updated_date(entry)
        extra_fields = {}
        if _is_later_timestamp(updated, published):
            extra_fields["azureWasUpdated"] = True
        articles.append(
            _build_article_record(
                title=clean_html(entry.get("title", "Untitled")),
                link=entry.get("link", ""),
                published=published,
                summary=truncate(summary),
                blog=blog_name,
                blog_id=blog_id,
                author=entry.get("author", "Microsoft"),
                lifecycle_state="ga",
                date_precision="day",
                extra_fields=extra_fields,
            )
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


def _fetch_named_feeds_in_parallel(feed_specs):
    """Fetch multiple named feeds in parallel and merge article results."""
    articles = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_MAX_WORKERS) as executor:
        future_to_spec = {}
        for spec in feed_specs:
            print(f"Fetching: {spec['fetch_label']}...")
            future = executor.submit(
                _fetch_named_feed,
                spec["blog_name"],
                spec["blog_id"],
                spec["feed_url"],
            )
            future_to_spec[future] = spec

        for future in concurrent.futures.as_completed(future_to_spec):
            spec = future_to_spec[future]
            try:
                articles.extend(future.result())
            except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
                print(f"  Error fetching {spec['error_label']}: {exc}")

    return articles


def fetch_tech_community_feeds():
    """Fetch articles from Tech Community blogs."""
    feed_specs = [
        {
            "blog_name": blog_name,
            "blog_id": board_id,
            "feed_url": TC_RSS_URL.format(board=board_id),
            "fetch_label": f"{blog_name} ({board_id})",
            "error_label": blog_name,
        }
        for board_id, blog_name in BLOGS.items()
    ]
    return _fetch_named_feeds_in_parallel(feed_specs)


def fetch_aks_blog():
    """Fetch articles from the AKS blog."""
    print("Fetching: AKS Blog...")
    try:
        return _fetch_named_feed("AKS Blog", "aksblog", AKS_BLOG_FEED)
    except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
        print(f"  Error fetching AKS blog: {exc}")
        return []


def fetch_devblogs_feeds():
    """Fetch articles from Microsoft DevBlogs."""
    feed_specs = [
        {
            "blog_name": blog_name,
            "blog_id": blog_id,
            "feed_url": feed_url,
            "fetch_label": blog_name,
            "error_label": blog_name,
        }
        for blog_id, (blog_name, feed_url) in DEVBLOGS.items()
    ]
    return _fetch_named_feeds_in_parallel(feed_specs)


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


def _resolve_linked_retirement_date(link, update_id=None):
    """Resolve retirement date via Azure Updates id API first, then page scrape."""
    token = str(update_id or _extract_azure_update_id_from_url(link) or "").strip()
    try:
        if token:
            retirement_date = _extract_azure_update_retirement_date_by_id(token)
            if retirement_date:
                return retirement_date
        return _extract_azure_update_retirement_date_from_page(link)
    except (
        requests.exceptions.RequestException,
        ValueError,
        TypeError,
        RuntimeError,
    ):
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


def _preferred_retirement_event_link(event):
    """Pick the canonical link for a merged retirement event.

    If any source report comes from Microsoft Lifecycle, keep the endoflife.date
    URL as canonical. For non-lifecycle merges, prefer an Azure Updates URL when
    present because it is the most stable public landing page for the announcement.
    Otherwise keep the current event URL.
    """

    def _is_endoflife_date_link(value):
        if not value:
            return False
        normalized = str(value).strip().lower()
        return normalized.startswith("https://endoflife.date/") or normalized.startswith("http://endoflife.date/")

    source_reports = event.get("sourceReports", [])

    for source_report in source_reports:
        lifecycle_link = source_report.get("link", "")
        if source_report.get("blogId") == MICROSOFT_LIFECYCLE_BLOG_ID and _is_endoflife_date_link(lifecycle_link):
            return lifecycle_link

    if len(source_reports) <= 1:
        return event.get("link", "")

    for source_report in source_reports:
        if source_report.get("blogId") == "azureupdates" and source_report.get("link"):
            return source_report["link"]

    return event.get("link", "")


def _index_retirement_event(events_by_key, event, dedupe_key, update_id_key, runtime_alias_key):
    """Index one retirement event under all relevant dedupe keys."""
    events_by_key[dedupe_key] = event
    if update_id_key:
        events_by_key[update_id_key] = event
    if runtime_alias_key:
        events_by_key[runtime_alias_key] = event


def _collect_unique_retirement_events(events_by_key):
    """Return unique event objects from a key-indexed event map."""
    events = []
    seen_ids = set()
    for event in events_by_key.values():
        event_identity = id(event)
        if event_identity in seen_ids:
            continue
        seen_ids.add(event_identity)
        events.append(event)
    return events


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


def _parse_azure_update_modified(item):
    """Extract best modified timestamp available in an Azure Updates API item."""
    for key in (
        "modified",
        "lastModified",
        "updated",
        "updatedDate",
        "lastUpdatedDate",
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
    modified = _parse_azure_update_modified(item)
    if _is_later_timestamp(modified, published):
        article["azureWasUpdated"] = True
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
                    enriched_retirement_date = _resolve_linked_retirement_date(
                        article.get("link", "")
                    )
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

    for entry in feed.entries:
        summary = clean_html(entry.get("summary", ""))
        articles.append(
            _build_article_record(
                title=clean_html(entry.get("title", "Untitled")),
                link=entry.get("link", ""),
                published=parse_date(entry),
                summary=truncate(summary),
                blog="Azure Updates",
                blog_id="azureupdates",
                author=entry.get("author", "Microsoft"),
            )
        )

    print(f"  Found {len(articles)} RSS articles")
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
        lifecycle = _classify_azure_update_lifecycle(f"{title} {summary_full}", "")
        extra_fields = {"announcementType": announcement_type}
        if lifecycle:
            extra_fields["lifecycle"] = lifecycle
        if lifecycle == "retiring":
            retirement_date = _extract_azure_retirement_date(title, summary_full)
            if retirement_date:
                extra_fields["azureRetirementDate"] = retirement_date
        article = _build_article_record(
            title=title,
            link=entry.get("link", ""),
            published=parse_date(entry),
            summary=truncate(summary_full),
            blog=blog_name,
            blog_id=blog_id,
            author=entry.get("author", "Microsoft"),
            extra_fields=extra_fields,
        )
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
        linked_retirement_date = _resolve_linked_retirement_date(link, update_id=update_id)
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


def _fetch_json_payload(url):
    """Fetch and decode JSON from an allowlisted URL."""
    validate_feed_url(url)
    response = HTTP_SESSION.get(url, timeout=FEED_REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _microsoft_lifecycle_milestone_label(key):
    """Return readable text for endoflife.date milestone keys."""
    return {
        "eoas": "Active support ends",
        "eol": "Security support ends",
        "eoes_start": "Extended security updates begin",
        "eoes": "Extended security updates end",
    }.get(key, "Support milestone")


def _microsoft_lifecycle_milestone_date(release, milestone):
    """Resolve the source date field for a microsoft lifecycle milestone."""
    if not isinstance(release, dict):
        return None

    if milestone == "eoes_start":
        # Only emit ESU start when an ESU end exists for the same release.
        if not release.get("eoesFrom"):
            return None
        return release.get("eolFrom")

    if milestone in {"eoas", "eol", "eoes"}:
        return release.get(f"{milestone}From")

    return None


def fetch_microsoft_lifecycle_retirements(config=None):
    """Load curated Microsoft lifecycle milestones from endoflife.date."""
    settings = dict(MICROSOFT_LIFECYCLE_CONFIG)
    if config:
        settings.update(config)

    if not settings.get("enabled", True):
        print("Skipping Microsoft lifecycle source (disabled in config)")
        return []

    products = [str(p or "").strip().lower() for p in settings.get("products", []) if str(p or "").strip()]
    milestones = [
        m
        for m in settings.get("milestones", [])
        if m in {"eoas", "eol", "eoes_start", "eoes"}
    ]
    max_events = int(settings.get("maxEvents", DEFAULT_MICROSOFT_LIFECYCLE_EVENT_CAP) or DEFAULT_MICROSOFT_LIFECYCLE_EVENT_CAP)

    if not products or not milestones:
        return []

    now_iso = datetime.now(timezone.utc).isoformat()

    valid_products = set()
    try:
        tag_payload = _fetch_json_payload(MICROSOFT_LIFECYCLE_TAG_API)
        for row in tag_payload.get("result", []) if isinstance(tag_payload, dict) else []:
            name = str((row or {}).get("name", "")).strip().lower()
            if name:
                valid_products.add(name)
    except (requests.exceptions.RequestException, ValueError, TypeError, AttributeError) as exc:
        print(f"Warning: failed to fetch Microsoft lifecycle tag index: {exc}")

    articles = []
    seen = set()

    for product in products:
        if valid_products and product not in valid_products:
            print(f"  Skipping lifecycle product not in microsoft tag list: {product}")
            continue

        product_url = f"https://endoflife.date/api/v1/products/{product}/"
        try:
            payload = _fetch_json_payload(product_url)
        except (requests.exceptions.RequestException, ValueError, TypeError, AttributeError) as exc:
            print(f"  Warning: failed lifecycle fetch for {product}: {exc}")
            continue

        result = payload.get("result") if isinstance(payload, dict) else {}
        if not isinstance(result, dict):
            continue

        product_label = clean_html(result.get("label") or result.get("name") or product)
        html_link = ((result.get("links") or {}).get("html") or "").strip()
        releases = result.get("releases") if isinstance(result.get("releases"), list) else []

        for release in releases:
            if not isinstance(release, dict):
                continue

            release_label = clean_html(release.get("label") or release.get("name") or "")
            if not release_label:
                continue

            for milestone in milestones:
                raw_date = _microsoft_lifecycle_milestone_date(release, milestone)
                retirement_date = _normalize_structured_retirement_date(raw_date)
                if not retirement_date or not _is_retirement_date_future(retirement_date):
                    continue

                lifecycle_label = _microsoft_lifecycle_milestone_label(milestone)
                title = f"Retirement: {product_label} {release_label} - {lifecycle_label}"
                key = (product, release_label.lower(), milestone, retirement_date)
                if key in seen:
                    continue
                seen.add(key)

                # Keep lifecycle links on endoflife.date so users land on the
                # product lifecycle page instead of unrelated external docs.
                product_lifecycle_link = f"https://endoflife.date/{product}"

                articles.append(
                    {
                        "title": title,
                        "link": product_lifecycle_link,
                        "published": now_iso,
                        "summary": truncate(
                            f"{product_label} {release_label}: {lifecycle_label} on {retirement_date}."
                        ),
                        "blog": "Microsoft Lifecycle",
                        "blogId": MICROSOFT_LIFECYCLE_BLOG_ID,
                        "author": "endoflife.date",
                        "announcementType": "retirement",
                        "lifecycle": "retiring",
                        "azureRetirementDate": retirement_date,
                        "azureRetirementDateSource": "endoflife",
                        "lifecycleProduct": product,
                        "lifecycleRelease": release_label,
                        "lifecycleMilestone": milestone,
                    }
                )

    articles.sort(
        key=lambda a: (
            _parse_retirement_calendar_sort_date(a.get("azureRetirementDate"))
            or datetime.max.replace(tzinfo=timezone.utc),
            a.get("title", "").lower(),
        )
    )
    if max_events > 0:
        articles = articles[:max_events]

    if articles:
        print(f"  Loaded {len(articles)} curated Microsoft lifecycle milestones")
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


def build_azure_retirement_calendar(articles, max_items=DEFAULT_RETIREMENT_CALENDAR_EVENT_CAP):
    """Build a deduplicated, date-sorted list of upcoming retirement announcements."""
    today = datetime.now(timezone.utc).date()
    events_by_key = {}

    # Index: retirement_date -> list of (token_frozenset, dedupe_key) for token-overlap dedupe.
    # Populated as events are registered; used to catch cross-source near-duplicates whose
    # titles are too different for exact-string matching.
    date_event_tokens = {}

    # Process azureretirements (workbook) articles first so their structured short titles
    # serve as the reference when matching verbose RSS blog-post titles.
    def _source_priority(a):
        return 0 if a.get("blogId") == "azureretirements" else 1

    for article in sorted(articles, key=_source_priority):
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
        category = _categorize_retirement_article(article)
        source_key = str(article.get("blogId", "") or "").strip().lower()
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
            "category": category,
        }

        # --- Exact / structured key lookups ---
        existing = events_by_key.get(dedupe_key)
        if not existing and update_id_key:
            existing = events_by_key.get(update_id_key)
        if not existing and runtime_alias_key:
            existing = events_by_key.get(runtime_alias_key)

        # --- Cross-source fuzzy token-overlap lookup (same retirement date) ---
        # This catches cases where an azureretirements workbook entry (short structured
        # title like "Subscription - Azure Virtual Desktop Classic") and an
        # azuredeprecations RSS entry (long blog title) describe the same event.
        if not existing:
            article_blog_id = article.get("blogId", "")
            article_tokens = _calendar_identity_tokens(title)
            for reg_tokens, reg_key, reg_runtime_key, reg_blog_id in date_event_tokens.get(retirement_date, []):
                # Skip if both articles come from the same feed — same-source deduplication
                # is handled by exact key matching above; fuzzy matching within the same feed
                # causes false merges on dates with many unrelated entries sharing common words.
                if article_blog_id == reg_blog_id:
                    continue
                # If both sides have a runtime alias key and the keys differ, they are
                # different runtime versions (e.g. Node 20 vs Node 22) — do not merge.
                if runtime_alias_key and reg_runtime_key and runtime_alias_key != reg_runtime_key:
                    continue
                if _tokens_are_same_event(article_tokens, reg_tokens):
                    existing = events_by_key.get(reg_key)
                    if existing is not None:
                        break

        if existing:
            existing["sourceReports"].append(source_report)
            if article.get("published", "") > existing.get("published", ""):
                existing["published"] = article.get("published", "")

            existing_categories = [
                c for c in existing.get("categories", []) if str(c or "").strip()
            ]
            if category and category not in existing_categories:
                existing_categories.append(category)
            existing["categories"] = sorted(set(existing_categories))

            source_category_map = existing.get("categorySourceMap")
            if not isinstance(source_category_map, dict):
                source_category_map = {}
            if source_key and category:
                source_category_map[source_key] = category
            existing["categorySourceMap"] = source_category_map

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
                existing["primaryCategory"] = category or existing.get("primaryCategory", "Other")

            if existing.get("blogId") != "azureupdates" and article.get("blogId") == "azureupdates":
                existing["blog"] = article.get("blog", "")
                existing["blogId"] = article.get("blogId", "")
                existing["announcementType"] = article.get("announcementType", "")
                existing["link"] = link or existing.get("link", "")
                existing["primaryCategory"] = category or existing.get("primaryCategory", "Other")
            elif not existing.get("link") and link:
                existing["link"] = link
            if not existing.get("primaryCategory"):
                existing["primaryCategory"] = category or "Other"
            existing["sources"] = sorted(
                {
                    src for src in existing.get("sources", []) + [source_label]
                    if src
                }
            )
            _index_retirement_event(
                events_by_key,
                existing,
                dedupe_key,
                update_id_key,
                runtime_alias_key,
            )
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
            "primaryCategory": category or "Other",
            "categories": [category or "Other"],
            "categorySourceMap": {source_key: category or "Other"} if source_key else {},
        }
        _index_retirement_event(
            events_by_key,
            event,
            dedupe_key,
            update_id_key,
            runtime_alias_key,
        )

        # Register this event's identity tokens for same-date fuzzy matching of later articles.
        # Store runtime_alias_key and blogId alongside so version-distinct and same-feed
        # entries are not falsely merged.
        event_tokens = _calendar_identity_tokens(title)
        if event_tokens:
            date_event_tokens.setdefault(retirement_date, []).append(
                (event_tokens, dedupe_key, runtime_alias_key, article.get("blogId", ""))
            )

    events = []
    events = _collect_unique_retirement_events(events_by_key)
    for event in events:
        event["sourceCount"] = len(event.get("sourceReports", []))
        event["link"] = _preferred_retirement_event_link(event)

    events.sort(
        key=lambda event: (
            _parse_retirement_calendar_sort_date(event.get("retirementDate"))
            or datetime.max.replace(tzinfo=timezone.utc),
            event.get("title", "").lower(),
        )
    )
    return events[:max_items]


def build_unified_retirement_calendar(
    azure_events=None,
    microsoft_events=None,
    m365_events=None,
    max_items=DEFAULT_RETIREMENT_CALENDAR_EVENT_CAP,
):
    """Build a deduplicated, source-tagged calendar from Azure, Microsoft, and M365 events.

    This reuses the Azure calendar dedupe implementation as the shared core,
    then annotates each merged event with unified source metadata.
    """
    def _source_from_blog_id(blog_id):
        raw = str(blog_id or "").strip().lower()
        if raw == "microsoftlifecycle":
            return "microsoft"
        if raw == "m365":
            return "m365"
        if raw:
            return "azure"
        return "unknown"

    def _append_source_articles(combined, source_articles):
        for article in source_articles or []:
            article_copy = dict(article)
            if not article_copy.get("azureRetirementDate"):
                article_copy["azureRetirementDate"] = article_copy.get("m365RetirementDate", "")
            combined.append(article_copy)

    combined_articles = []
    _append_source_articles(combined_articles, azure_events)
    _append_source_articles(combined_articles, microsoft_events)
    _append_source_articles(combined_articles, m365_events)

    calendar = build_azure_retirement_calendar(combined_articles, max_items=max_items)

    for event in calendar:
        event["source"] = _source_from_blog_id(event.get("blogId", ""))
        source_reports = event.get("sourceReports", [])
        for report in source_reports:
            report["source"] = _source_from_blog_id(report.get("blogId", ""))

    return calendar


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
        ai_used = bool(ai_results)
        if unclassified_pool and not ai_used:
            print("  AI classification unavailable; continuing with rule-based summary only")

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
        summary_source = "azure-openai" if ai_used else "rule-based"
        print(f"Summary generated ({summary_source}): {summary[:100]}...")
        return {
            "status": "available",
            "summary": summary,
            "source": summary_source,
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
def filter_main_feed_articles(articles):
    """Exclude calendar-only sources from the main feed article list."""
    excluded_blog_ids = {WORKBOOK_BLOG_ID, MICROSOFT_LIFECYCLE_BLOG_ID}
    return [
        article
        for article in (articles or [])
        if article.get("blogId") not in excluded_blog_ids
    ]


def load_previous_main_feed_article_count(path):
    """Return previous main feed count using current exclusion rules."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"No previous output found at {path}; skip publish fail-safe baseline")
        return None
    except (json.JSONDecodeError, OSError, ValueError) as e:
        print(f"Could not load previous output {path} ({e}); skip publish fail-safe baseline")
        return None

    previous_articles = data.get("articles")
    if isinstance(previous_articles, list):
        return len(filter_main_feed_articles(previous_articles))

    # Fallback for legacy payloads that only expose totalArticles.
    if isinstance(data.get("totalArticles"), int):
        return data.get("totalArticles")
    return None


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


def _record_failed_run(
    *,
    raw_article_count,
    unique_article_count,
    previous_article_count,
    failsafe_details,
    savill_video,
    retirement_calendar,
    retirement_buckets,
):
    """Write failure run metrics for early-return paths."""
    run_metrics = build_run_metrics(
        raw_article_count=raw_article_count,
        unique_article_count=unique_article_count,
        previous_article_count=previous_article_count,
        failsafe_triggered=True,
        failsafe_details=failsafe_details,
        published=False,
        summary_payload=None,
        savill_video=savill_video,
        retirement_calendar=retirement_calendar,
        retirement_buckets=retirement_buckets,
    )
    write_run_metrics(run_metrics)


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
    lifecycle_calendar_articles = fetch_microsoft_lifecycle_retirements()
    raw_article_count = len(all_articles)

    # Fetch Savill video (independent of article feeds)
    savill_video = fetch_savill_video()

    # Sort by date, newest first
    all_articles.sort(key=lambda x: x.get("published", ""), reverse=True)

    # Remove duplicates and discard articles older than 30 days
    unique_articles = dedupe_articles(all_articles)
    lifecycle_and_azure = list(unique_articles) + list(lifecycle_calendar_articles)
    
    # Separate events by source for unified calendar
    azure_events = [a for a in lifecycle_and_azure if a.get("blogId") not in ("m365",)]
    microsoft_events = [a for a in lifecycle_calendar_articles]  # Only microsoft lifecycle
    
    # Build unified calendar (Azure + Microsoft; M365 will be added by fetch_m365_data.py)
    unified_retirement_calendar = build_unified_retirement_calendar(
        azure_events=azure_events,
        microsoft_events=microsoft_events,
        m365_events=None,  # M365 will be added by fetch_m365_data.py
    )
    
    # For backward compatibility, also build the Azure-only view
    retirement_calendar = build_azure_retirement_calendar(lifecycle_and_azure)
    retirement_buckets = build_retirement_window_buckets(retirement_calendar)
    main_feed_articles = filter_main_feed_articles(unique_articles)

    discarded = len(all_articles) - len(unique_articles)
    if discarded:
        print(f"Filtered out {discarded} duplicate/older-than-30-days articles")

    output_path = os.path.join("data", "feeds.json")

    # Validate feed data schema and quality (NEW: Improvement #1)
    is_valid, validation_msg = validate_feed_data(main_feed_articles, min_coverage_percent=85)
    if not is_valid:
        print(f"❌ Feed validation failed: {validation_msg}")
        print("Aborting publish to preserve data integrity")
        previous_count = load_previous_main_feed_article_count(output_path)
        failsafe_details = f"Feed validation failed: {validation_msg}"
        _record_failed_run(
            raw_article_count=raw_article_count,
            unique_article_count=len(main_feed_articles),
            previous_article_count=previous_count,
            failsafe_details=failsafe_details,
            savill_video=savill_video,
            retirement_calendar=retirement_calendar,
            retirement_buckets=retirement_buckets,
        )
        return
    
    print(f"✅ {validation_msg}")

    previous_count = load_previous_main_feed_article_count(output_path)
    failsafe_triggered, failsafe_details = evaluate_publish_failsafe(
        len(main_feed_articles), previous_count
    )
    if previous_count is None:
        print("Publish fail-safe bypassed due to missing or unreadable baseline")
    elif failsafe_triggered:
        print("Publish fail-safe triggered; skipping output write to preserve last good data")
        print(f"  {failsafe_details}")
        _record_failed_run(
            raw_article_count=raw_article_count,
            unique_article_count=len(main_feed_articles),
            previous_article_count=previous_count,
            failsafe_details=failsafe_details,
            savill_video=savill_video,
            retirement_calendar=retirement_calendar,
            retirement_buckets=retirement_buckets,
        )
        return
    else:
        print("Publish fail-safe check passed")
        print(f"  {failsafe_details}")

    # Generate AI summary (optional)
    summary_payload = generate_ai_summary(main_feed_articles)

    data = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "totalArticles": len(main_feed_articles),
        "articles": main_feed_articles,
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

    # Write unified retirement calendar (for use by fetch_m365_data.py)
    unified_calendar_path = os.path.join("data", "retirements.json")
    unified_calendar_data = {
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "unifiedRetirementCalendar": unified_retirement_calendar,
        "unifiedRetirementBuckets": build_retirement_window_buckets(unified_retirement_calendar),
    }
    with open(unified_calendar_path, "w", encoding="utf-8") as f:
        json.dump(unified_calendar_data, f, indent=2, ensure_ascii=False)
    print(f"Unified retirement calendar saved to {unified_calendar_path}")

    # Generate RSS feed
    generate_rss_feed(main_feed_articles)

    # Generate ICS calendar for subscription/sync scenarios.
    write_azure_retirements_ics(retirement_calendar)

    write_checksums_file()

    run_metrics = build_run_metrics(
        raw_article_count=raw_article_count,
        unique_article_count=len(main_feed_articles),
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
    print(
        f"Done! {len(main_feed_articles)} feed articles "
        f"(from {len(unique_articles)} unique source articles) saved to {output_path}"
    )
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
