"""Shared helpers for feed fetch scripts."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_STATUS_FORCELIST = (429, 500, 502, 503, 504)
DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_host(value):
    """Normalize hostnames used for URL allow-listing and dedup."""
    host = (value or "").strip().lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def load_site_config(path):
    """Load and validate canonical site settings from config/site.json."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    canonical_host = normalize_host(raw.get("canonicalHost"))
    configured_url = (raw.get("canonicalUrl") or "").strip()
    parsed_url = urlsplit(configured_url)
    url_host = normalize_host(parsed_url.hostname)

    if not canonical_host:
        raise ValueError("site config canonicalHost must be a non-empty string")
    if parsed_url.scheme != "https":
        raise ValueError("site config canonicalUrl must use https")
    if not url_host or url_host != canonical_host:
        raise ValueError("site config canonicalUrl host must match canonicalHost")
    if parsed_url.path not in ("", "/") or parsed_url.query or parsed_url.fragment:
        raise ValueError("site config canonicalUrl must point to site root")

    return {
        "canonicalHost": canonical_host,
        "canonicalUrl": f"https://{canonical_host}",
    }


def create_http_session(
    *,
    retry_total,
    backoff_factor,
    user_agent,
    status_forcelist=DEFAULT_STATUS_FORCELIST,
    allowed_methods=("GET",),
    raise_on_status=False,
):
    """Create a requests session with retry policy and user agent."""
    retry = Retry(
        total=retry_total,
        connect=retry_total,
        read=retry_total,
        status=retry_total,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        allowed_methods=frozenset(allowed_methods) if allowed_methods else None,
        respect_retry_after_header=True,
        raise_on_status=raise_on_status,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if user_agent:
        session.headers.update({"User-Agent": user_agent})
    return session


def canonicalize_url(
    url,
    *,
    tracking_query_prefixes=(),
    tracking_query_keys=frozenset(),
    default_ports=None,
    default_scheme=None,
):
    """Canonicalize URLs for deduplication and stable comparisons."""
    raw_url = (url or "").strip()
    if not raw_url:
        return ""

    ports = default_ports or DEFAULT_PORTS
    parsed = urlsplit(raw_url)
    scheme = (parsed.scheme or default_scheme or "").lower()
    host = normalize_host(parsed.hostname)
    if not scheme or not host:
        return raw_url

    port = parsed.port
    netloc = host
    if port and ports.get(scheme) != port:
        netloc = f"{host}:{port}"

    path = parsed.path or "/"
    while "//" in path:
        path = path.replace("//", "/")
    if path != "/":
        path = path.rstrip("/")

    filtered_query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        normalized_key = key.lower()
        if any(normalized_key.startswith(prefix) for prefix in tracking_query_prefixes):
            continue
        if normalized_key in tracking_query_keys:
            continue
        filtered_query.append((key, value))

    query = urlencode(sorted(filtered_query))
    return urlunsplit((scheme, netloc, path, query, ""))


def extract_youtube_video_id(url):
    """Extract a YouTube video id from watch or youtu.be links."""
    parsed = urlsplit((url or "").strip())
    host = normalize_host(parsed.hostname)
    if not host:
        return ""

    if host == "youtu.be":
        return parsed.path.strip("/")

    if host in {"youtube.com", "m.youtube.com"}:
        query = dict(parse_qsl(parsed.query or ""))
        return query.get("v", "")

    return ""


def build_youtube_thumbnail_from_video_url(url):
    """Build a deterministic thumbnail URL from a YouTube video link."""
    video_id = extract_youtube_video_id(url)
    if not video_id:
        return ""
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


def resolve_youtube_channel_id_from_seed(session, seed_url, timeout):
    """Resolve YouTube channel id by parsing a seed video page payload."""
    response = session.get(seed_url, timeout=timeout)
    response.raise_for_status()

    import re

    channel_match = re.search(r'"channelId"\s*:\s*"([A-Za-z0-9_-]+)"', response.text)
    if not channel_match:
        return ""
    return channel_match.group(1)


def select_best_youtube_video_entry(entries, match_score_fn):
    """Pick highest scoring YouTube feed entry, fallback to latest upload."""
    if not entries:
        return None, False

    best = max(entries, key=match_score_fn)
    used_fallback = match_score_fn(best) <= 0
    if used_fallback:
        best = entries[0]
    return best, used_fallback


def _artifact_checksum_record(path, generated_at):
    """Return checksum metadata for an existing artifact file."""
    if ".." in str(path):
        raise Exception("Invalid file path")
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)

    return {
        "path": Path(path).as_posix(),
        "algorithm": "sha256",
        "value": sha256.hexdigest(),
        "generatedAt": generated_at,
    }


def build_checksums_payload(paths, generated_at=None):
    """Build checksum metadata for published artifacts."""
    timestamp = generated_at or datetime.now(timezone.utc).isoformat()
    artifacts = [_artifact_checksum_record(path, timestamp) for path in paths]
    return {
        "generatedAt": timestamp,
        "artifacts": artifacts,
    }


def write_checksums_file(paths, output_path, generated_at=None, logger=None):
    """Write checksum metadata after published artifacts are finalized."""
    payload = build_checksums_payload(paths, generated_at=generated_at)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    if logger:
        logger(f"Checksums saved to {output}")
    return payload


def load_previous_article_count(path, logger=None):
    """Return prior article count from JSON payload, or None when unavailable."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            existing = json.load(f)
    except FileNotFoundError:
        if logger:
            logger(f"Publish fail-safe baseline not found: {path}")
        return None
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        if logger:
            logger(f"Publish fail-safe baseline unreadable at {path}: {exc}")
        return None

    articles = existing.get("articles")
    if isinstance(articles, list):
        return len(articles)

    total_articles = existing.get("totalArticles")
    if isinstance(total_articles, int) and total_articles >= 0:
        return total_articles

    if logger:
        logger(
            "Publish fail-safe baseline missing both articles array and totalArticles; "
            "bypassing guard for this run"
        )
    return None


def evaluate_publish_failsafe(new_count, previous_count, min_articles=80, min_ratio=0.60):
    """Return (triggered, details) for publish fail-safe guard logic."""
    if previous_count is None:
        details = (
            f"baseline unavailable; new_count={new_count}; "
            f"min_articles={min_articles}; min_ratio={min_ratio:.2f}"
        )
        return False, details

    relative_threshold = math.ceil(previous_count * min_ratio)
    relative_trigger = new_count < relative_threshold
    absolute_trigger = previous_count >= min_articles and new_count < min_articles
    triggered = relative_trigger or absolute_trigger

    details = (
        f"new_count={new_count}, previous_count={previous_count}, "
        f"relative_threshold={relative_threshold} (ratio={min_ratio:.2f}), "
        f"absolute_threshold={min_articles}, "
        f"relative_trigger={relative_trigger}, absolute_trigger={absolute_trigger}"
    )
    return triggered, details
