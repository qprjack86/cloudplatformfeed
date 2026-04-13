#!/usr/bin/env python3
"""
Microsoft 365 Change Intelligence Feed - MCP Data Fetcher
Fetches Microsoft 365 Roadmap and Message Center items from DeltaPulse MCP endpoint.
"""

import json
import os
import re
import concurrent.futures
import requests
import feedparser
from pathlib import Path
from datetime import datetime, timedelta, timezone

from feed_common import (
    canonicalize_url,
    create_http_session as shared_create_http_session,
    build_checksums_payload as shared_build_checksums_payload,
    build_youtube_thumbnail_from_video_url as shared_build_youtube_thumbnail_from_video_url,
    evaluate_publish_failsafe as shared_evaluate_publish_failsafe,
    extract_youtube_video_id as shared_extract_youtube_video_id,
    load_previous_article_count as shared_load_previous_article_count,
    load_site_config as shared_load_site_config,
    resolve_youtube_channel_id_from_seed as shared_resolve_youtube_channel_id_from_seed,
    select_best_youtube_video_entry as shared_select_best_youtube_video_entry,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_CONFIG_PATH = REPO_ROOT / "config" / "site.json"

# DeltaPulse MCP Endpoint Configuration
DELTAPULSE_MCP_ENDPOINT = "https://deltapulse.app/mcp"
DELTAPULSE_PRODUCTS_API = "https://deltapulse.app/mcp"
DELTAPULSE_ROADMAP_ITEM_API = "https://deltapulse.app/api/roadmap/items"

# Data configuration
M365_DATA_OUTPUT = REPO_ROOT / "data" / "m365_data.json"
M365_CHECKSUMS_OUTPUT = REPO_ROOT / "data" / "m365_checksums.json"
M365_RETIREMENTS_ICS_OUTPUT = REPO_ROOT / "data" / "m365-retirements.ics"
M365_PREVIOUS_COUNT_FILE = M365_DATA_OUTPUT  # Read totalArticles from previous m365_data.json

# Failsafe configuration (same as Azure)
FAILSAFE_MIN_ARTICLES = 80
FAILSAFE_MIN_RATIO = 0.60

# Request configuration
MCP_REQUEST_TIMEOUT = (5, 20)
MCP_RETRY_TOTAL = 2
MCP_BACKOFF_FACTOR = 1
MCP_USER_AGENT = "M365FeedBot/1.0"
MCP_MAX_WORKERS = 4

M365_VIDEO_SEED_URL = "https://www.youtube.com/watch?v=HdO9NV8a9yE&t=83s"
M365_VIDEO_TITLE_PREFIX = "what's new in microsoft 365"
YOUTUBE_RSS_BASE = "https://www.youtube.com/feeds/videos.xml"

# Tracking parameters to filter
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "mc_cid", "mc_eid", "mkt_tok", "ocid", "spm", "trk", "wt.mc_id",
}

# M365 Product category mapping for high-level organization
# (Improvement #4: Configurable Category Mappings - loaded from config/site.json)
def _load_m365_category_mappings(config_path=SITE_CONFIG_PATH):
    """Load M365 category mappings from config, with fallback to defaults."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            mappings = config.get("categoryMappings", {}).get("m365", {})
            if mappings:
                print(f"✅ Loaded M365 category mappings from config ({len(mappings)} categories)")
                return mappings
    except Exception as e:
        print(f"⚠️  Could not load M365 mappings from config: {e}")
    
    # Fallback to built-in defaults
    print("✅ Using built-in M365 category mappings")
    return {
        "Collaboration": [
            "Teams", "SharePoint Online", "Outlook", "Exchange Online", "OneDrive for Business",
            "Yammer", "Skype for Business", "Stream", "OneDrive"
        ],
        "Productivity": [
            "Word", "Excel", "PowerPoint", "OneNote", "Access", "Publisher", "Project", "Visio",
            "Microsoft 365 apps", "Microsoft 365 suite"
        ],
        "AI & Automation": [
            "Microsoft Copilot", "Copilot Pro", "Power Automate", "Copilot in Teams", 
            "Copilot for Microsoft 365", "Copilot Studio", "Microsoft Foundry"
        ],
        "Data & Analytics": [
            "Power BI", "Power Query", "Analysis Services", "Data Factory", "Microsoft Fabric",
            "Excel Services", "Dataverse"
        ],
        "Security & Compliance": [
            "Microsoft 365 Defender", "Azure Information Protection", "Advanced Threat Protection",
            "Security & Compliance Center", "Insider Risk Management", "Microsoft Purview",
            "Microsoft Defender XDR"
        ],
        "Administration": [
            "Microsoft 365 Admin Center", "Entra ID", "Active Directory", "Intune",
            "Endpoint Manager", "Compliance Manager", "Microsoft 365 Lighthouse",
            "Entra", "Windows", "Windows 365"
        ],
        "Business Applications": [
            "Dynamics 365 Apps", "Dynamics 365 Sales", "Dynamics 365 Customer Service",
            "Dynamics 365 Finance", "Dynamics 365 Supply Chain", "Project Operations",
            "Business Central", "Finance and Operations Apps", "Power Apps", "Power Platform"
        ],
        "Other": [
            "Bookings", "Forms", "Lists", "Planner", "Shifts", "To Do", "Viva",
            "Viva Engage", "Viva Topics", "Viva Learning", "Viva Goals"
        ]
    }


M365_PRODUCT_CATEGORIES = _load_m365_category_mappings()

RETIREMENT_MONTH_PATTERN = (
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
)
RETIREMENT_CONTEXT_PATTERN = re.compile(
    r"retir|will end|end of support|end-of-support|end of life|eol|deprecated|deprecat|sunset|stop supporting",
    re.IGNORECASE,
)
RETIREMENT_TAG_PATTERN = re.compile(
    r"retir|deprecat|sunset|end of support|end-of-support|end of life|eol",
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
ACT_BY_FIELD_KEYS = (
    "actByDate",
    "actBy",
    "actionByDate",
    "actionBy",
)


def load_site_config(path=SITE_CONFIG_PATH):
    """Load canonical site config (same as Azure feed for consistency)."""
    return shared_load_site_config(path)


SITE_CONFIG = load_site_config()
CANONICAL_SITE_HOST = SITE_CONFIG["canonicalHost"]
CANONICAL_SITE_URL = SITE_CONFIG["canonicalUrl"]

# M365 cache for fallback (Improvement #3: Resilient MCP Layer)
M365_CACHE_PATH = REPO_ROOT / "data" / ".m365_cache.json"


def load_m365_cache():
    """Load last successful M365 response from cache for fallback."""
    if not M365_CACHE_PATH.exists():
        return None
    try:
        with open(M365_CACHE_PATH, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
            cached_at = cache_data.get("cachedAt", "unknown")
            print(f"⚠️  Loading cached M365 data (cached at {cached_at})")
            return cache_data
    except Exception as e:
        print(f"⚠️  Could not load M365 cache: {e}")
        return None


def save_m365_cache(data):
    """Save successful M365 response to cache for fallback."""
    try:
        M365_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        cache_data = {
            "items": data,
            "cachedAt": datetime.now(timezone.utc).isoformat()
        }
        with open(M365_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False)
        print(f"💾 M365 cache updated: {M365_CACHE_PATH}")
    except Exception as e:
        print(f"⚠️  Could not save M365 cache: {e}")



def create_http_session():
    """Create HTTP session with retry logic."""
    return shared_create_http_session(
        retry_total=MCP_RETRY_TOTAL,
        backoff_factor=MCP_BACKOFF_FACTOR,
        user_agent=MCP_USER_AGENT,
        allowed_methods=("GET", "HEAD", "OPTIONS"),
        raise_on_status=False,
    )


def call_mcp_tool(session: requests.Session, tool_name: str, arguments: dict = None, max_retries=3):
    """Call a DeltaPulse MCP tool with retry logic and graceful error handling.
    
    (Improvement #3: Resilient MCP Layer)
    """
    import time
    
    for attempt in range(max_retries):
        try:
            payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": arguments or {}
                },
                "id": 1,
            }
            
            response = session.post(
                DELTAPULSE_MCP_ENDPOINT,
                json=payload,
                timeout=MCP_REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            result = response.json()
            
            if "error" in result:
                error = result["error"]
                error_msg = error.get('message', 'Unknown error')
                print(f"❌ MCP tool error calling {tool_name}: {error_msg}")
                
                if attempt < max_retries - 1:
                    backoff = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    print(f"   Retrying in {backoff}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff)
                    continue
                else:
                    return []
            
            # Extract items from nested MCP response structure
            content = result.get("result", {}).get("content", [])
            if not content:
                return []
            
            outer_text = content[0].get("text", "{}")
            try:
                outer_json = json.loads(outer_text)
                inner_content = outer_json.get("content", [])
                
                if not inner_content or not isinstance(inner_content, list):
                    return []
                
                inner_text = inner_content[0].get("text", "{}") if inner_content else "{}"
                inner_json = json.loads(inner_text)
                
                print(f"✅ MCP tool {tool_name} succeeded (attempt {attempt + 1})")
                return inner_json.get("items", [])
            except (json.JSONDecodeError, KeyError, TypeError, IndexError) as e:
                print(f"❌ Error parsing MCP response for {tool_name}: {e}")
                if attempt < max_retries - 1:
                    backoff = 2 ** attempt
                    print(f"   Retrying in {backoff}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff)
                    continue
                return []
                
        except requests.exceptions.RequestException as exc:
            print(f"❌ Network error calling MCP tool {tool_name}: {exc}")
            if attempt < max_retries - 1:
                backoff = 2 ** attempt
                print(f"   Retrying in {backoff}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff)
                continue
            else:
                return []
    
    return []


def call_mcp_fetch_metadata(session: requests.Session, item_id: str) -> dict:
    """Fetch detailed metadata for an item via DeltaPulse fetch tool."""
    if not item_id:
        return {}

    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "fetch",
            "arguments": {"id": str(item_id)},
        },
        "id": 1,
    }

    try:
        response = session.post(
            DELTAPULSE_MCP_ENDPOINT,
            json=payload,
            timeout=MCP_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()
        if "error" in result:
            return {}

        content = result.get("result", {}).get("content", [])
        if not content:
            return {}

        text = content[0].get("text", "{}")
        parsed = json.loads(text)
        metadata = parsed.get("metadata")
        return metadata if isinstance(metadata, dict) else {}
    except (
        requests.exceptions.RequestException,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        KeyError,
        IndexError,
    ):
        return {}


def call_roadmap_item_details(session: requests.Session, item_id: str) -> dict:
    """Fetch roadmap item details from DeltaPulse public item API."""
    if not item_id:
        return {}

    try:
        response = session.get(
            f"{DELTAPULSE_ROADMAP_ITEM_API}/{item_id}",
            timeout=MCP_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}
    except (
        requests.exceptions.RequestException,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ):
        return {}


def _first_non_empty(item: dict, keys: tuple[str, ...]):
    """Return first non-empty value from candidate keys."""
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value.strip()
            continue
        if isinstance(value, (list, tuple)):
            if value:
                return value
            continue
        return value
    return None


def _flatten_to_strings(value) -> list[str]:
    """Flatten nested scalar/list values into normalized strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [_normalise_whitespace(part) for part in value.split(",") if _normalise_whitespace(part)]
    if isinstance(value, dict):
        flattened = []
        for nested in value.values():
            flattened.extend(_flatten_to_strings(nested))
        return flattened
    if isinstance(value, (list, tuple, set)):
        flattened = []
        for nested in value:
            flattened.extend(_flatten_to_strings(nested))
        return flattened
    normalized = _normalise_whitespace(str(value))
    return [normalized] if normalized else []


def _extract_m365_tags(item: dict) -> list[str]:
    """Extract normalized tags from known DeltaPulse tag fields."""
    tags = []
    for key in ("tags", "tag", "labels", "label"):
        tags.extend(_flatten_to_strings(item.get(key)))

    seen = set()
    result = []
    for tag in tags:
        lowered = tag.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(tag)
    return result


def _has_retirement_tag(tags: list[str]) -> bool:
    return any(RETIREMENT_TAG_PATTERN.search(tag or "") for tag in tags)


def _has_retirement_signal(title_raw: str, summary_raw: str, tags: list[str]) -> bool:
    """Prefer explicit retirement tags, with text-context fallback when tags are absent."""
    if tags:
        return _has_retirement_tag(tags)
    combined = _normalise_whitespace(f"{title_raw} {summary_raw}")
    return bool(RETIREMENT_CONTEXT_PATTERN.search(combined))


def resolve_m365_target_date(item: dict):
    """Resolve expected release date/month from known DeltaPulse fields."""
    direct = _first_non_empty(item, (
        "targetedReleaseDate",
        "targetReleaseDate",
        "expectedReleaseDate",
        "releaseDate",
        "targetDate",
        "deploymentDate",
    ))
    if direct is not None:
        if isinstance(direct, str):
            normalized = re.sub(r"\b([CF]Y)(\d{4})\b", r"\2", direct).strip()
            normalized = re.sub(r"\s+", " ", normalized)
            return normalized
        return direct

    months = item.get("months")
    if isinstance(months, list):
        month_values = [str(month).strip() for month in months if str(month).strip()]
        if month_values:
            return ", ".join(month_values)

    return None


def _normalise_whitespace(value: str) -> str:
    """Collapse repeated whitespace and trim."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _extract_rollout_window(text: str) -> str:
    """Extract concise timeline text from rollout sentence."""
    raw = _normalise_whitespace(text)
    if not raw:
        return ""

    month_phrase = r"(?:early|mid|late)?\s*[A-Za-z]+\s+\d{4}"
    begin_match = re.search(rf"begin(?:ning)?\s+in\s+({month_phrase})", raw, flags=re.IGNORECASE)
    complete_match = re.search(rf"complete(?:s|d)?\s+by\s+({month_phrase})", raw, flags=re.IGNORECASE)
    if begin_match and complete_match:
        return f"{begin_match.group(1).strip()} - {complete_match.group(1).strip()}"

    phrases = re.findall(month_phrase, raw, flags=re.IGNORECASE)
    if not phrases:
        return raw
    if len(phrases) == 1:
        return phrases[0].strip()
    return f"{phrases[0].strip()} - {phrases[-1].strip()}"


def extract_when_will_happen_dates(text: str) -> dict:
    """Extract Preview/GA timing from a roadmap detail section."""
    cleaned = _normalise_whitespace(text)
    if not cleaned:
        return {}

    section_match = re.search(
        r"\[When this will happen\](.*?)(?:\[[^\]]+\]|$)",
        str(text),
        flags=re.IGNORECASE | re.DOTALL,
    )
    section_text = section_match.group(1) if section_match else str(text)

    phase_dates = {}
    for line in section_text.splitlines():
        line_clean = _normalise_whitespace(line)
        if not line_clean:
            continue

        phase_match = re.match(
            r"^(?:[-*•]\s*)?(Public\s+Preview|Preview|General\s+Availability(?:\s*\([^)]+\))?|GA)\s*:\s*(.+)$",
            line_clean,
            flags=re.IGNORECASE,
        )
        if not phase_match:
            continue

        phase_name = phase_match.group(1).lower()
        timeline = _extract_rollout_window(phase_match.group(2))
        if not timeline:
            continue

        if "preview" in phase_name:
            phase_dates["m365PreviewDate"] = timeline
        elif "general availability" in phase_name or phase_name == "ga":
            phase_dates["m365GeneralAvailabilityDate"] = timeline

    return phase_dates


def normalize_url(url: str) -> str:
    """Normalize DeltaPulse URLs for deduplication."""
    return canonicalize_url(
        url,
        tracking_query_prefixes=TRACKING_QUERY_PREFIXES,
        tracking_query_keys=TRACKING_QUERY_KEYS,
        default_scheme="https",
    )


def _normalize_retirement_title(title: str) -> str:
    """Normalize retirement titles for stable cross-item dedupe."""
    value = _normalise_whitespace(title)
    value = re.sub(r"^\s*(retirement|deprecation|update)\s*:\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"[^\w\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip().lower()


def _m365_retirement_date_precision(value: str) -> str | None:
    raw = str(value or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return "day"
    if re.match(r"^\d{4}-\d{2}$", raw):
        return "month"
    return None


def _parse_retirement_calendar_sort_date(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None
    day_match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if day_match:
        return datetime(
            int(day_match.group(1)),
            int(day_match.group(2)),
            int(day_match.group(3)),
            tzinfo=timezone.utc,
        )
    month_match = re.match(r"^(\d{4})-(\d{2})$", raw)
    if month_match:
        return datetime(
            int(month_match.group(1)),
            int(month_match.group(2)),
            1,
            tzinfo=timezone.utc,
        )
    return None


def _is_retirement_date_future(value: str, today=None) -> bool:
    precision = _m365_retirement_date_precision(value)
    if not precision:
        return False

    reference = today or datetime.now(timezone.utc).date()
    raw = str(value or "").strip()
    if precision == "month":
        year, month = raw.split("-")
        return (int(year), int(month)) >= (reference.year, reference.month)

    sort_dt = _parse_retirement_calendar_sort_date(raw)
    return bool(sort_dt and sort_dt.date() >= reference)


def _normalize_retirement_date_candidate(match: re.Match[str], precision: str):
    groups = match.groupdict()
    month = RETIREMENT_MONTH_TO_INT.get((groups.get("month") or "")[:3].lower())
    if not month:
        return None

    try:
        year = int(groups.get("year", "0"))
    except ValueError:
        return None

    if precision == "day":
        try:
            day = int(groups.get("day", "0"))
            datetime(year, month, day)
        except (TypeError, ValueError):
            return None
        value = f"{year:04d}-{month:02d}-{day:02d}"
    else:
        value = f"{year:04d}-{month:02d}"

    return {
        "value": value,
        "precision": precision,
        "sortDate": _parse_retirement_calendar_sort_date(value),
    }


def _extract_retirement_date_without_context(raw_text: str):
    """Extract a future date from act-by style text without requiring retirement keywords."""
    cleaned = _normalise_whitespace(raw_text)
    if not cleaned:
        return None

    direct_precision = _m365_retirement_date_precision(cleaned)
    if direct_precision and _is_retirement_date_future(cleaned):
        return cleaned

    candidates = []
    for pattern, precision in RETIREMENT_DATE_PATTERNS:
        for match in pattern.finditer(cleaned):
            candidate = _normalize_retirement_date_candidate(match, precision)
            if not candidate:
                continue
            if not _is_retirement_date_future(candidate["value"]):
                continue
            candidates.append(
                (
                    1 if candidate["precision"] == "day" else 0,
                    candidate["sortDate"] or datetime.min.replace(tzinfo=timezone.utc),
                    candidate["value"],
                )
            )

    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2]


def _extract_m365_retirement_date(title_raw: str, summary_raw: str, act_by_raw: str = ""):
    """Extract explicit retirement/deprecation date from title or summary text."""
    act_by_date = _extract_retirement_date_without_context(act_by_raw)
    if act_by_date:
        return act_by_date

    now_date = datetime.now(timezone.utc).date()
    sources = (("title", title_raw), ("summary", summary_raw))
    candidates = []

    for source_name, text in sources:
        cleaned = _normalise_whitespace(text)
        if not cleaned:
            continue
        if not RETIREMENT_CONTEXT_PATTERN.search(cleaned):
            continue

        for pattern, precision in RETIREMENT_DATE_PATTERNS:
            for match in pattern.finditer(cleaned):
                candidate = _normalize_retirement_date_candidate(match, precision)
                if not candidate:
                    continue

                if not _is_retirement_date_future(candidate["value"], today=now_date):
                    continue

                context_start = max(0, match.start() - 100)
                context_end = min(len(cleaned), match.end() + 100)
                context = cleaned[context_start:context_end]
                if not RETIREMENT_CONTEXT_PATTERN.search(context):
                    continue

                candidates.append(
                    (
                        1 if source_name == "title" else 0,
                        1 if candidate["precision"] == "day" else 0,
                        candidate["sortDate"] or datetime.min.replace(tzinfo=timezone.utc),
                        candidate["value"],
                    )
                )

    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][3]


def build_m365_retirement_calendar(articles: list, max_items: int = 120):
    """Build a deduplicated, date-sorted M365 retirement calendar from explicit dates."""
    today = datetime.now(timezone.utc).date()
    events_by_key = {}

    for article in articles:
        if article.get("m365RetirementSignal") is False:
            continue

        retirement_date = str(article.get("m365RetirementDate") or "").strip()
        if not retirement_date:
            retirement_date = _extract_m365_retirement_date(
                article.get("title", ""),
                article.get("summary", ""),
                article.get("m365ActByDate", ""),
            ) or ""
        # Fallback: for lifecycle=retiring articles, use m365TargetDate when no
        # explicit retirement date could be extracted from text (e.g. empty summary).
        if not retirement_date and article.get("lifecycle") == "retiring":
            target_raw = str(article.get("m365TargetDate") or "")
            for part in target_raw.split(","):
                candidate = _extract_retirement_date_without_context(part.strip())
                if candidate and _is_retirement_date_future(candidate, today=today):
                    retirement_date = candidate
                    break
        if not retirement_date:
            continue

        sort_dt = _parse_retirement_calendar_sort_date(retirement_date)
        if not sort_dt:
            continue
        if not _is_retirement_date_future(retirement_date, today=today):
            continue

        title = _normalise_whitespace(article.get("title") or "") or "Untitled retirement notice"
        dedupe_key = _normalize_retirement_title(title) or f"id:{article.get('m365Id', '')}"
        source_label = article.get("m365Service") or "Microsoft 365"
        precision = _m365_retirement_date_precision(retirement_date) or "month"

        event = {
            "title": re.sub(r"^\s*(retirement|deprecation|update)\s*:\s*", "", title, flags=re.IGNORECASE).strip() or title,
            "link": article.get("link", ""),
            "retirementDate": retirement_date,
            "datePrecision": precision,
            "published": article.get("published", ""),
            "blog": source_label,
            "blogId": "m365",
            "announcementType": "retirement",
            "sources": [source_label] if source_label else [],
            "sourceCount": 1,
        }

        existing = events_by_key.get(dedupe_key)
        if not existing:
            events_by_key[dedupe_key] = event
            continue

        existing_rank = (
            1 if existing.get("datePrecision") == "day" else 0,
            _parse_retirement_calendar_sort_date(existing.get("retirementDate", ""))
            or datetime.min.replace(tzinfo=timezone.utc),
        )
        incoming_rank = (
            1 if precision == "day" else 0,
            sort_dt,
        )
        if incoming_rank > existing_rank:
            events_by_key[dedupe_key] = event
            existing = events_by_key[dedupe_key]

        combined_sources = sorted(
            {
                src
                for src in (existing.get("sources", []) + ([source_label] if source_label else []))
                if src
            }
        )
        existing["sources"] = combined_sources
        existing["sourceCount"] = len(combined_sources)
        if not existing.get("link") and article.get("link"):
            existing["link"] = article.get("link")

    events = list(events_by_key.values())
    events.sort(
        key=lambda event: (
            _parse_retirement_calendar_sort_date(event.get("retirementDate"))
            or datetime.max.replace(tzinfo=timezone.utc),
            event.get("title", "").lower(),
        )
    )
    return events[:max_items]


def build_retirement_window_buckets(events: list, today=None, preview_limit: int = 8):
    """Build rolling retirement windows (0-3, 3-6, 6-9, 9-12 months)."""
    reference = today or datetime.now(timezone.utc).date()
    window_defs = (
        ("0_3_months", 0, 3),
        ("3_6_months", 3, 6),
        ("6_9_months", 6, 9),
        ("9_12_months", 9, 12),
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
                            or _m365_retirement_date_precision(retirement_date)
                            or "month",
                        }
                    )
                break

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "referenceMonth": f"{reference.year:04d}-{reference.month:02d}",
        "windows": buckets,
    }


def classify_m365_lifecycle(item: dict) -> str:
    """Classify lifecycle status based on M365 source and status/severity."""
    text_blob = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("description") or ""),
            str(item.get("summary") or ""),
            str(item.get("status") or ""),
        ]
    )
    if RETIREMENT_CONTEXT_PATTERN.search(text_blob):
        return "retiring"

    source = item.get("source", "")
    
    if source == "roadmap":
        # Prefer DeltaPulse's own releasePhase; fall back to status
        phase = (item.get("releasePhase") or item.get("status") or "").lower()
        if "development" in phase or "in development" in phase:
            return "in_development"
        elif "preview" in phase or "in preview" in phase:
            return "in_preview"
        elif "general" in phase or "available" in phase or "launched" in phase:
            return "launched_ga"
        return "in_preview"  # Default for roadmap items
    
    elif source == "message_center":
        severity = (item.get("severity") or "").lower()
        # Message Center items are operational updates, not lifecycle-based
        # Treat all as "launched_ga" since they're current/operational
        return "launched_ga"
    
    return "launched_ga"  # Safe default


def dedupe_m365_articles(articles: list, max_age_days: int = 30, major_change_age_days: int = 90) -> list:
    """Deduplicate M365 items by normalized URL; discard stale items.

    Major change articles (m365IsMajorChange=True) are kept for
    ``major_change_age_days`` (default 90); all others for ``max_age_days``
    (default 30).
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max_age_days)
    major_cutoff = now - timedelta(days=major_change_age_days)

    seen_keys = {}  # dedupe key -> article (first seen wins)
    deduped = []

    for article in articles:
        # Check age — major changes get extended retention.
        pub_date_str = article.get("published", "")  # Article dict uses 'published', not 'publishedDate'
        if pub_date_str:
            try:
                pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
                retirement_date = article.get("m365RetirementDate")
                if retirement_date and _is_retirement_date_future(retirement_date, today=now.date()):
                    age_cutoff = None
                else:
                    age_cutoff = major_cutoff if article.get("m365IsMajorChange") else cutoff
                if age_cutoff and pub_date < age_cutoff:
                    continue  # Skip stale items
            except (ValueError, AttributeError):
                pass  # If unparseable, include it

        # Prefer source/id dedupe to avoid collapsing admin URLs that carry IDs in fragments.
        source = (article.get("m365Source") or "").strip().lower()
        item_id = str(article.get("m365Id") or "").strip()
        dedupe_key = f"{source}:{item_id}" if source and item_id else ""

        if not dedupe_key:
            url = article.get("link", "")  # Article dict uses 'link', not 'url'
            dedupe_key = normalize_url(url)

        if dedupe_key and dedupe_key not in seen_keys:
            seen_keys[dedupe_key] = article
            article["_normalized_url"] = dedupe_key
            deduped.append(article)

    return deduped


def resolve_m365_item_link(item: dict) -> str:
    """Resolve the best outbound link for a DeltaPulse item."""
    source = item.get("source", "")
    item_id = str(item.get("id", "")).strip()

    # Use DeltaPulse public item page (works without authentication).
    if item_id and source in ("message_center", "roadmap"):
        return f"https://deltapulse.app/item/{item_id}"

    return item.get("url", "")


def _resolve_published_date(item: dict) -> str:
    """Pick the best available date from a DeltaPulse item."""
    for key in (
        "publishedDate",
        "lastModifiedDateTime",
        "updatedDateTime",
        "updatedDate",
        "addedDateTime",
        "createdDateTime",
        "createdDate",
        "modifiedDate",
    ):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return datetime.now(timezone.utc).isoformat()


def build_article_from_m365_item(item: dict) -> dict:
    """Convert a DeltaPulse item into article schema."""
    services = item.get("service", [])
    main_service = services[0] if services else "Microsoft 365"
    
    summary_raw = _first_non_empty(item, ("summary", "description", "message", "details"))
    summary = _normalise_whitespace(summary_raw) if isinstance(summary_raw, str) else ""
    tags = _extract_m365_tags(item)
    retirement_signal = _has_retirement_signal(item.get("title", ""), summary, tags)
    act_by = _first_non_empty(item, ACT_BY_FIELD_KEYS)
    retirement_date = (
        _extract_m365_retirement_date(item.get("title", ""), summary, str(act_by or ""))
        if retirement_signal
        else None
    )

    return {
        "title": item.get("title", ""),
        "link": resolve_m365_item_link(item),
        "published": _resolve_published_date(item),
        "summary": summary,
        "source": "m365",
        "m365Service": main_service,
        "m365AllServices": services,
        "m365Id": item.get("id", ""),
        "m365Source": item.get("source", ""),  # "roadmap" or "message_center"
        "m365Category": item.get("category", ""),
        "m365Severity": item.get("severity"),
        "m365Status": None if item.get("source") == "roadmap" else item.get("status"),
        "m365TargetDate": resolve_m365_target_date(item),
        "m365PreviewDate": item.get("m365PreviewDate"),
        "m365GeneralAvailabilityDate": item.get("m365GeneralAvailabilityDate"),
        "m365IsMajorChange": item.get("isMajorChange", False),
        "m365Tags": tags,
        "m365RetirementSignal": retirement_signal,
        "m365ActByDate": str(act_by).strip() if act_by is not None else None,
        "m365RetirementDate": retirement_date,
        "m365RetirementDatePrecision": _m365_retirement_date_precision(retirement_date) if retirement_date else None,
        "lifecycle": classify_m365_lifecycle(item),
    }


def categorize_by_product(articles: list) -> dict:
    """Group articles by M365 product category."""
    categories = {cat: [] for cat in M365_PRODUCT_CATEGORIES.keys()}
    categories["Uncategorised"] = []
    
    for article in articles:
        service = article.get("m365Service", "")
        categorized = False
        
        for category_name, products in M365_PRODUCT_CATEGORIES.items():
            if any(service.lower() in prod.lower() or prod.lower() in service.lower() 
                   for prod in products):
                categories[category_name].append(article)
                categorized = True
                break
        
        if not categorized:
            categories["Uncategorised"].append(article)
    
    return {k: v for k, v in categories.items() if v}  # Remove empty categories


ROADMAP_DETAIL_FIELDS = (
    "description",
    "product",
    "status",
    "releaseDate",
    "createdDate",
    "modifiedDate",
    "lastUpdated",
    "cloudInstances",
    "releasePhase",
    "platforms",
    "thirdPartyLinks",
    "isMajorChange",
    "tags",
    "actBy",
    "actByDate",
    "actionBy",
    "actionByDate",
)

MCP_METADATA_FIELDS = (
    "status",
    "severity",
    "category",
    "service",
    "months",
    "releaseDate",
    "targetedReleaseDate",
    "targetDate",
    "expectedReleaseDate",
    "deploymentDate",
    "publishedDate",
    "lastUpdatedDate",
    "createdDate",
    "modifiedDate",
    "isMajorChange",
    "tags",
    "labels",
    "actBy",
    "actByDate",
    "actionBy",
    "actionByDate",
)


def _apply_patch_if_missing(item: dict, patch: dict):
    """Apply patch fields only when item field is missing/empty."""
    for key, value in patch.items():
        if key not in item or item.get(key) in (None, "", []):
            item[key] = value


def _enrich_m365_item(session: requests.Session, item: dict) -> dict:
    """Build enrichment patch for a single M365 item."""
    item_id = str(item.get("id", "")).strip()
    source = str(item.get("source", "")).strip().lower()
    patch = {}

    if source == "roadmap":
        roadmap_details = call_roadmap_item_details(session, item_id)
        for field in ROADMAP_DETAIL_FIELDS:
            if field in roadmap_details:
                patch[field] = roadmap_details.get(field)

        extracted_dates = extract_when_will_happen_dates(roadmap_details.get("description", ""))
        for key, value in extracted_dates.items():
            if value:
                patch[key] = value

    metadata = call_mcp_fetch_metadata(session, item_id)
    if metadata:
        for field in MCP_METADATA_FIELDS:
            if field in metadata:
                patch[field] = metadata.get(field)

        if source == "roadmap":
            metadata_dates = extract_when_will_happen_dates(metadata.get("description", ""))
            for key, value in metadata_dates.items():
                if value:
                    patch[key] = value

    return patch


def fetch_m365_items(session: requests.Session) -> list:
    """Fetch new and updated M365 items from DeltaPulse MCP with cache fallback.
    
    (Improvement #3: Resilient MCP Layer & Graceful Degradation)
    """
    print("Fetching M365 items from DeltaPulse MCP...")
    
    try:
        all_items = []
        
        # Fetch new items from last 7 days
        print("  - Fetching new items (last 7 days)...")
        new_items = call_mcp_tool(session, "list_new_items", {
            "limit": 100,
            "dateRange": "last_7_days",
        })
        all_items.extend(new_items)
        print(f"    Found {len(new_items)} new items")
        
        # Fetch updated items from last 7 days
        print("  - Fetching updated items (last 7 days)...")
        updated_items = call_mcp_tool(session, "list_updated_items", {
            "limit": 100,
            "dateRange": "last_7_days",
        })
        all_items.extend(updated_items)
        print(f"    Found {len(updated_items)} updated items")
    
        # If both fetches returned nothing, try cache fallback
        if not all_items:
            print("⚠️  MCP fetch returned no items; attempting cache fallback...")
            cache = load_m365_cache()
            if cache and cache.get("items"):
                return cache["items"]
            else:
                print("❌ No M365 data available (cache empty/missing)")
                return []

        # Enrich each unique item with metadata from fetch(id)
        by_key = {}
        for item in all_items:
            source = str(item.get("source", "")).strip()
            item_id = str(item.get("id", "")).strip()
            key = f"{source}:{item_id}" if source and item_id else ""
            if key and key not in by_key:
                by_key[key] = item

        print(f"  - Enriching metadata for {len(by_key)} unique items...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MCP_MAX_WORKERS) as executor:
            future_to_item = {
                executor.submit(_enrich_m365_item, session, item): item
                for item in by_key.values()
            }
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    patch = future.result()
                except (
                    requests.exceptions.RequestException,
                    json.JSONDecodeError,
                    TypeError,
                    ValueError,
                ) as exc:
                    item_id = str(item.get("id", "")).strip() or "unknown"
                    print(f"  Warning: enrichment failed for {item_id}: {exc}")
                    continue
                _apply_patch_if_missing(item, patch)
        
        print(f"  - Total raw items: {len(all_items)}")
        
        # Save successful fetch to cache
        save_m365_cache(all_items)
        
        return all_items
        
    except Exception as e:
        print(f"❌ Error fetching M365 items: {e}")
        # Try cache fallback on any error
        cache = load_m365_cache()
        if cache and cache.get("items"):
            return cache["items"]
        return []


def _extract_youtube_video_id(url: str) -> str:
    """Extract YouTube video ID from watch or youtu.be URL."""
    if not isinstance(url, str) or not url:
        return ""
    return shared_extract_youtube_video_id(url)


def _build_thumbnail_from_video_url(url: str) -> str:
    """Build a YouTube thumbnail URL from a video URL."""
    return shared_build_youtube_thumbnail_from_video_url(url)


def _build_youtube_thumbnail_from_video_url(url: str) -> str:
    """Alias for parity with Azure script helper naming."""
    return _build_thumbnail_from_video_url(url)


def _resolve_youtube_channel_id_from_seed(
    session: requests.Session,
    seed_url: str,
    timeout,
) -> str:
    """Resolve a YouTube channel id by reading the seed video page payload."""
    return shared_resolve_youtube_channel_id_from_seed(session, seed_url, timeout)


def _select_best_youtube_video_entry(entries: list, match_score_fn):
    """Select highest scoring entry; fall back to latest upload when no match."""
    return shared_select_best_youtube_video_entry(entries, match_score_fn)


def _normalize_summary_title(value: str) -> str:
    """Normalize title text for stable matching."""
    value = (value or "").strip().lower()
    value = value.replace("’", "'").replace("‘", "'")
    return re.sub(r"\s+", " ", value)


def _is_m365_monthly_video_title(title: str) -> bool:
    """Match the dedicated "What's new in Microsoft 365" series title."""
    normalized = _normalize_summary_title(title)
    if not normalized:
        return False

    variants = (
        M365_VIDEO_TITLE_PREFIX,
        M365_VIDEO_TITLE_PREFIX.replace("'", ""),
    )
    for prefix in variants:
        if normalized == prefix or normalized.startswith(prefix + " ") or normalized.startswith(prefix + "|") or normalized.startswith(prefix + ":"):
            return True
    return False


def fetch_m365_video(session: requests.Session) -> dict:
    """Fetch latest "What's new in Microsoft 365" video from YouTube channel RSS."""
    print("Fetching: Microsoft 365 YouTube summary video...")

    fallback = {
        "title": "What's new in Microsoft 365",
        "url": M365_VIDEO_SEED_URL,
        "published": "",
        "thumbnail": _build_youtube_thumbnail_from_video_url(M365_VIDEO_SEED_URL),
    }

    try:
        channel_id = _resolve_youtube_channel_id_from_seed(
            session,
            M365_VIDEO_SEED_URL,
            MCP_REQUEST_TIMEOUT,
        )
        if not channel_id:
            print("  Warning: Could not resolve YouTube channel id from seed video")
            return fallback

        rss_url = f"{YOUTUBE_RSS_BASE}?channel_id={channel_id}"
        rss_resp = session.get(rss_url, timeout=MCP_REQUEST_TIMEOUT)
        rss_resp.raise_for_status()
        feed = feedparser.parse(rss_resp.content)
        if not feed.entries:
            print("  Warning: No entries in Microsoft 365 YouTube feed")
            return fallback

        matching_entries = [
            entry
            for entry in feed.entries
            if _is_m365_monthly_video_title((entry.get("title", "") or "").strip())
        ]
        if not matching_entries:
            print("  Warning: No matching 'What's new in Microsoft 365' video found")
            return fallback

        best = max(matching_entries, key=lambda entry: entry.get("published", ""))

        link = best.get("link", "")
        thumbnail = ""
        media_thumbs = getattr(best, "media_thumbnail", None) or best.get("media_thumbnail", [])
        if media_thumbs:
            thumbnail = media_thumbs[0].get("url", "")
        if not thumbnail:
            thumbnail = _build_youtube_thumbnail_from_video_url(link)

        print(f"  Found: {best.get('title', '')[:70]}")
        return {
            "title": best.get("title", fallback["title"]),
            "url": link or fallback["url"],
            "published": best.get("published", "") or fallback["published"],
            "thumbnail": thumbnail or fallback["thumbnail"],
        }

    except (
        requests.exceptions.RequestException,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        KeyError,
        IndexError,
    ) as exc:
        print(f"  Error fetching Microsoft 365 video: {exc}")
        return fallback


def build_m365_feed(raw_items: list, m365_video: dict = None) -> dict:
    """Build the complete M365 feed data structure."""
    # Convert items to article schema
    articles = [build_article_from_m365_item(item) for item in raw_items]
    
    # Deduplicate and filter stale
    deduped = dedupe_m365_articles(articles)
    print(f"After deduplication: {len(deduped)} unique articles")
    
    # Categorize by product
    by_category = categorize_by_product(deduped)
    
    # Build lifecycle buckets (for potential summarization)
    by_lifecycle = {}
    for lifecycle in ["in_preview", "launched_ga", "in_development", "retiring"]:
        by_lifecycle[lifecycle] = [a for a in deduped if a.get("lifecycle") == lifecycle]

    retirement_calendar = build_m365_retirement_calendar(deduped)
    retirement_buckets = build_retirement_window_buckets(retirement_calendar)
    
    # Compute distinct publishing days for the summary date range.
    # Determine the strict 7-day calendar window backwards from the most recent article.
    valid_dates = []
    for a in deduped:
        try:
            dt = datetime.fromisoformat(a["published"].replace("Z", "+00:00"))
            valid_dates.append(dt)
        except (ValueError, KeyError):
            pass

    pub_days = set()
    if valid_dates:
        latest_date = max(valid_dates)
        summary_cutoff = latest_date - timedelta(days=7)
        for dt in valid_dates:
            if dt > summary_cutoff:
                pub_days.add(dt.strftime("%Y-%m-%d"))
                
    publishing_days = sorted(pub_days, reverse=True)

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalArticles": len(deduped),
        "articles": deduped,
        "byCategory": by_category,
        "byLifecycle": by_lifecycle,
        "m365RetirementCalendar": retirement_calendar,
        "m365RetirementBuckets": retirement_buckets,
        "m365Video": m365_video,
        "summaryPublishingDays": publishing_days,
        "source": "m365",
    }


def _ics_escape_text(value: str) -> str:
    """Escape text for ICS fields."""
    text = str(value or "")
    text = text.replace("\\", "\\\\")
    text = text.replace(";", "\\;")
    text = text.replace(",", "\\,")
    text = text.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "")
    return text


def _ics_fold_line(line: str, limit: int = 75) -> str:
    """Fold long ICS lines for client compatibility."""
    if len(line) <= limit:
        return line

    parts = [line[:limit]]
    remainder = line[limit:]
    while remainder:
        parts.append(" " + remainder[: limit - 1])
        remainder = remainder[limit - 1 :]
    return "\r\n".join(parts)


def _ics_uid_for_m365_event(event):
    """Build deterministic UID for an M365 retirement event."""
    title_token = re.sub(r"\s+", "-", _normalize_retirement_title(event.get("title", "untitled"))) or "untitled"
    date_token = re.sub(r"[^0-9]", "", str(event.get("retirementDate", ""))) or "nodate"
    source_token = re.sub(r"[^a-z0-9]", "", str(event.get("blog", "m365")).lower()) or "m365"
    return f"m365-retirement-{date_token}-{title_token[:32]}-{source_token[:12]}@{CANONICAL_SITE_HOST}"


def _ics_date_fields(retirement_date, precision):
    """Return DTSTART/DTEND fields for retirement events."""
    parsed = _parse_retirement_calendar_sort_date(retirement_date)
    if not parsed:
        return None

    start = parsed.date()
    end = start + timedelta(days=1)
    return {
        "dtstart": start.strftime("%Y%m%d"),
        "dtend": end.strftime("%Y%m%d"),
        "precision": precision,
    }


def generate_m365_retirements_ics(events, generated_at=None):
    """Generate ICS payload for Microsoft 365 retirement events."""
    stamp = generated_at or datetime.now(timezone.utc)
    dtstamp = stamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Cloud Platform Feed//M365 Retirement Calendar//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{_ics_escape_text('Microsoft 365 Retirement Calendar')}",
        f"X-WR-CALDESC:{_ics_escape_text('Upcoming Microsoft 365 retirement announcements from Cloud Platform Feed')}",
    ]

    for event in events:
        retirement_date = str(event.get("retirementDate", "")).strip()
        precision = event.get("datePrecision") or _m365_retirement_date_precision(retirement_date)
        date_fields = _ics_date_fields(retirement_date, precision)
        if not date_fields:
            continue

        sources = event.get("sources", [])
        source_label = ", ".join(str(src) for src in sources if src)
        description_lines = [
            f"Retirement date: {retirement_date}",
            f"Date precision: {precision}",
        ]
        if source_label:
            description_lines.append(f"Sources: {source_label}")
        if precision != "day":
            description_lines.append("This event uses month-level precision in source data.")

        summary = event.get("title", "Untitled retirement notice")
        link = event.get("link", "")

        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{_ics_escape_text(_ics_uid_for_m365_event(event))}",
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


def write_m365_retirements_ics(events, output_path: Path = M365_RETIREMENTS_ICS_OUTPUT, generated_at=None):
    """Write M365 retirement events as an ICS artifact."""
    payload = generate_m365_retirements_ics(events, generated_at=generated_at)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload, encoding="utf-8")
    print(f"M365 ICS calendar written to {output_path}")
    return True


def write_m365_data(feed_data: dict, output_path: Path = M365_DATA_OUTPUT) -> bool:
    """Write M365 feed data to JSON file."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(feed_data, f, indent=2, ensure_ascii=False)
        print(f"M365 data written to {output_path}")
        return True
    except (OSError, TypeError, ValueError) as exc:
        print(f"Error writing M365 data: {exc}")
        return False


def build_checksums_payload(paths: list, generated_at: str = None) -> dict:
    """Build checksum metadata (same pattern as Azure feed)."""
    return shared_build_checksums_payload(paths, generated_at=generated_at)


def write_m365_checksums(paths: list[Path], output_path: Path = M365_CHECKSUMS_OUTPUT) -> bool:
    """Write checksums for M365 data artifacts."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = build_checksums_payload(paths)
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        
        print(f"M365 checksums written to {output_path}")
        return True
    except (OSError, TypeError, ValueError) as exc:
        print(f"Error writing checksums: {exc}")
        return False


def load_previous_article_count(path: Path = M365_DATA_OUTPUT) -> int:
    """Load previous article count for failsafe comparison."""
    return shared_load_previous_article_count(path)


def evaluate_m365_failsafe(new_count: int, previous_count: int = None) -> tuple:
    """Evaluate publish failsafe (same logic as Azure feed)."""
    if previous_count is None:
        return False, "baseline_unavailable"
    return shared_evaluate_publish_failsafe(
        new_count,
        previous_count,
        min_articles=FAILSAFE_MIN_ARTICLES,
        min_ratio=FAILSAFE_MIN_RATIO,
    )


def main():
    """Main entry point: fetch M365 data and write to files."""
    print("Starting M365 Feed Data Fetch...")
    
    session = create_http_session()
    
    try:
        # Fetch items from DeltaPulse
        raw_items = fetch_m365_items(session)
        
        if not raw_items:
            print("Warning: No items fetched from DeltaPulse MCP")
        
        m365_video = fetch_m365_video(session)

        # Build feed structure
        feed_data = build_m365_feed(raw_items, m365_video)
        
        # Evaluate failsafe
        previous_count = load_previous_article_count()
        failsafe_triggered, failsafe_details = evaluate_m365_failsafe(
            feed_data["totalArticles"],
            previous_count
        )
        
        if failsafe_triggered:
            print(f"⚠️  Failsafe triggered: {failsafe_details}")
            print(f"   Previous: {previous_count}, Current: {feed_data['totalArticles']}")
            # In production workflow, this would prevent publishing
        
        # Write data and checksums
        success = write_m365_data(feed_data)
        if success:
            write_m365_retirements_ics(feed_data.get("m365RetirementCalendar", []))
            write_m365_checksums([M365_DATA_OUTPUT, M365_RETIREMENTS_ICS_OUTPUT])
            print("\n✓ M365 feed data fetch completed successfully")
        else:
            print("\n✗ Failed to write M365 data")
            return 1
        
        return 0
    
    finally:
        session.close()


if __name__ == "__main__":
    exit(main())
