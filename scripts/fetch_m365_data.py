#!/usr/bin/env python3
"""
Microsoft 365 Change Intelligence Feed - MCP Data Fetcher
Fetches Microsoft 365 Roadmap and Message Center items from DeltaPulse MCP endpoint.
"""

import hashlib
import json
import os
import re
import time
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_CONFIG_PATH = REPO_ROOT / "config" / "site.json"

# DeltaPulse MCP Endpoint Configuration
DELTAPULSE_MCP_ENDPOINT = "https://deltapulse.app/mcp"
DELTAPULSE_PRODUCTS_API = "https://deltapulse.app/mcp"

# Data configuration
M365_DATA_OUTPUT = REPO_ROOT / "data" / "m365_data.json"
M365_CHECKSUMS_OUTPUT = REPO_ROOT / "data" / "m365_checksums.json"
M365_PREVIOUS_COUNT_FILE = M365_DATA_OUTPUT  # Read totalArticles from previous m365_data.json

# Failsafe configuration (same as Azure)
FAILSAFE_MIN_ARTICLES = 80
FAILSAFE_MIN_RATIO = 0.60

# Request configuration
MCP_REQUEST_TIMEOUT = (5, 20)
MCP_RETRY_TOTAL = 2
MCP_BACKOFF_FACTOR = 1
MCP_USER_AGENT = "M365FeedBot/1.0"

# Tracking parameters to filter
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "mc_cid", "mc_eid", "mkt_tok", "ocid", "spm", "trk", "wt.mc_id",
}

DEFAULT_PORTS = {"http": 80, "https": 443}

# M365 Product category mapping for high-level organization
M365_PRODUCT_CATEGORIES = {
    "Collaboration": [
        "Teams", "SharePoint Online", "Outlook", "Exchange Online", "OneDrive for Business",
        "Yammer", "Skype for Business", "Stream"
    ],
    "Productivity": [
        "Word", "Excel", "PowerPoint", "OneNote", "Access", "Publisher", "Project", "Visio",
        "Microsoft 365 apps"
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
        "Security & Compliance Center", "Insider Risk Management", "Microsoft Purview"
    ],
    "Administration": [
        "Microsoft 365 Admin Center", "Entra ID", "Active Directory", "Intune",
        "Endpoint Manager", "Compliance Manager", "Microsoft 365 Lighthouse"
    ],
    "Business Applications": [
        "Dynamics 365 Apps", "Dynamics 365 Sales", "Dynamics 365 Customer Service",
        "Dynamics 365 Finance", "Dynamics 365 Supply Chain", "Project Operations",
        "Business Central", "Finance and Operations Apps"
    ],
    "Other": { # Fallback
        "Bookings", "Forms", "Lists", "Planner", "Shifts", "To Do", "Viva",
        "Viva Engage", "Viva Topics", "Viva Learning", "Viva Goals"
    }
}


def load_site_config(path=SITE_CONFIG_PATH):
    """Load canonical site config (same as Azure feed for consistency)."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    
    canonical_host = (raw.get("canonicalHost") or "").strip().lower().rstrip(".")
    configured_url = (raw.get("canonicalUrl") or "").strip()
    
    if not canonical_host:
        raise ValueError("site config canonicalHost must be a non-empty string")
    
    parsed_url = urlsplit(configured_url)
    if parsed_url.scheme != "https":
        raise ValueError("site config canonicalUrl must use https")
    
    return {
        "canonicalHost": canonical_host,
        "canonicalUrl": f"https://{canonical_host}",
    }


SITE_CONFIG = load_site_config()
CANONICAL_SITE_HOST = SITE_CONFIG["canonicalHost"]
CANONICAL_SITE_URL = SITE_CONFIG["canonicalUrl"]


def create_http_session():
    """Create HTTP session with retry logic."""
    retry = Retry(
        total=MCP_RETRY_TOTAL,
        connect=MCP_RETRY_TOTAL,
        read=MCP_RETRY_TOTAL,
        status=MCP_RETRY_TOTAL,
        backoff_factor=MCP_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": MCP_USER_AGENT})
    return session


def call_mcp_tool(session: requests.Session, tool_name: str, arguments: dict = None):
    """Call a DeltaPulse MCP tool."""
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments or {}
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
            error = result["error"]
            print(f"MCP tool error calling {tool_name}: {error.get('message', 'Unknown error')}")
            return []
        
        # Extract items from nested MCP response structure
        # Structure: result.content[0].text = JSON string
        #           → parse → content[0].text = JSON string  
        #           → parse → items array
        content = result.get("result", {}).get("content", [])
        if not content:
            return []
        
        # First level: content[0].text
        outer_text = content[0].get("text", "{}")
        try:
            outer_json = json.loads(outer_text)
            inner_content = outer_json.get("content", [])
            
            if not inner_content or not isinstance(inner_content, list):
                return []
            
            # Second level: content[0].text
            inner_text = inner_content[0].get("text", "{}") if inner_content else "{}"
            inner_json = json.loads(inner_text)
            
            return inner_json.get("items", [])
        except (json.JSONDecodeError, KeyError, TypeError, IndexError) as e:
            print(f"Error parsing MCP response for {tool_name}: {e}")
            return []
    except Exception as e:
        print(f"Error calling MCP tool {tool_name}: {e}")
        return []


def normalize_url(url: str) -> str:
    """Normalize DeltaPulse URLs for deduplication."""
    if not url:
        return ""
    
    # Parse URL
    parsed = urlsplit(url.strip())
    scheme = parsed.scheme.lower() or "https"
    hostname = (parsed.hostname or "").lower().rstrip(".")
    
    # Remove www. prefix
    if hostname.startswith("www."):
        hostname = hostname[4:]
    
    # Normalize port
    port = parsed.port or DEFAULT_PORTS.get(scheme)
    if port and port == DEFAULT_PORTS.get(scheme):
        port = None  # Omit default port
    netloc = f"{hostname}:{port}" if port else hostname
    
    # Normalize path (remove duplicate slashes, trailing slash)
    path = re.sub(r"/+", "/", parsed.path).rstrip("/") or "/"
    
    # Filter tracking parameters
    query_params = dict(parse_qsl(parsed.query or ""))
    query_params = {
        k: v for k, v in query_params.items()
        if not any(k.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES)
        and k not in TRACKING_QUERY_KEYS
    }
    
    # Sort remaining parameters
    query = urlencode(sorted(query_params.items())) if query_params else ""
    
    # Reconstruct URL (no fragment)
    return urlunsplit((scheme, netloc, path, query, ""))


def classify_m365_lifecycle(item: dict) -> str:
    """Classify lifecycle status based on M365 source and status/severity."""
    source = item.get("source", "")
    
    if source == "roadmap":
        status = (item.get("status") or "").lower()
        if "development" in status or "in development" in status:
            return "in_development"
        elif "preview" in status or "in preview" in status:
            return "in_preview"
        elif "general" in status or "available" in status or "launched" in status:
            return "launched_ga"
        return "in_preview"  # Default for roadmap items
    
    elif source == "message_center":
        severity = (item.get("severity") or "").lower()
        # Message Center items are operational updates, not lifecycle-based
        # Treat all as "launched_ga" since they're current/operational
        return "launched_ga"
    
    return "launched_ga"  # Safe default


def dedupe_m365_articles(articles: list, max_age_days: int = 30) -> list:
    """Deduplicate M365 items by normalized URL; discard stale items."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max_age_days)
    
    seen_keys = {}  # dedupe key -> article (first seen wins)
    deduped = []
    
    for article in articles:
        # Check age
        pub_date_str = article.get("published", "")  # Article dict uses 'published', not 'publishedDate'
        if pub_date_str:
            try:
                pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
                if pub_date < cutoff:
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

    if source == "message_center" and item_id:
        return (
            "https://admin.microsoft.com/Adminportal/Home"
            f"?#/MessageCenter/:/messages/{item_id}"
        )

    if source == "roadmap" and item_id:
        return (
            "https://www.microsoft.com/en-us/microsoft-365/roadmap"
            f"?filters=&searchterms={item_id}"
        )

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
    
    return {
        "title": item.get("title", ""),
        "link": resolve_m365_item_link(item),
        "published": _resolve_published_date(item),
        "source": "m365",
        "m365Service": main_service,
        "m365AllServices": services,
        "m365Id": item.get("id", ""),
        "m365Source": item.get("source", ""),  # "roadmap" or "message_center"
        "m365Category": item.get("category", ""),
        "m365Severity": item.get("severity"),
        "m365Status": item.get("status"),
        "m365TargetDate": item.get("targetedReleaseDate"),
        "lifecycle": classify_m365_lifecycle(item),
    }


def categorize_by_product(articles: list) -> dict:
    """Group articles by M365 product category."""
    categories = {cat: [] for cat in M365_PRODUCT_CATEGORIES.keys()}
    categories["Uncategorized"] = []
    
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
            categories["Uncategorized"].append(article)
    
    return {k: v for k, v in categories.items() if v}  # Remove empty categories


def fetch_m365_items(session: requests.Session) -> list:
    """Fetch new and updated M365 items from DeltaPulse MCP."""
    print("Fetching M365 items from DeltaPulse MCP...")
    
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
    
    print(f"  - Total raw items: {len(all_items)}")
    return all_items


def build_m365_feed(raw_items: list) -> dict:
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
    
    # Compute distinct publishing days for the summary date range
    pub_days = set()
    for a in deduped:
        try:
            dt = datetime.fromisoformat(a["published"].replace("Z", "+00:00"))
            pub_days.add(dt.strftime("%Y-%m-%d"))
        except (ValueError, KeyError):
            pass
    publishing_days = sorted(pub_days, reverse=True)

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalArticles": len(deduped),
        "articles": deduped,
        "byCategory": by_category,
        "byLifecycle": by_lifecycle,
        "summaryPublishingDays": publishing_days,
        "source": "m365",
    }


def write_m365_data(feed_data: dict, output_path: Path = M365_DATA_OUTPUT) -> bool:
    """Write M365 feed data to JSON file."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(feed_data, f, indent=2, ensure_ascii=False)
        print(f"M365 data written to {output_path}")
        return True
    except Exception as e:
        print(f"Error writing M365 data: {e}")
        return False


def build_checksums_payload(paths: list, generated_at: str = None) -> dict:
    """Build checksum metadata (same pattern as Azure feed)."""
    timestamp = generated_at or datetime.now(timezone.utc).isoformat()
    artifacts = []
    
    for path in paths:
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        
        artifacts.append({
            "path": Path(path).as_posix(),
            "algorithm": "sha256",
            "value": sha256.hexdigest(),
            "generatedAt": timestamp,
        })
    
    return {
        "generatedAt": timestamp,
        "artifacts": artifacts,
    }


def write_m365_checksums(m365_data_path: Path, output_path: Path = M365_CHECKSUMS_OUTPUT) -> bool:
    """Write checksums for M365 data file."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = build_checksums_payload([m365_data_path])
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        
        print(f"M365 checksums written to {output_path}")
        return True
    except Exception as e:
        print(f"Error writing checksums: {e}")
        return False


def load_previous_article_count(path: Path = M365_DATA_OUTPUT) -> int:
    """Load previous article count for failsafe comparison."""
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("totalArticles")
    except (json.JSONDecodeError, FileNotFoundError, IOError):
        return None


def evaluate_m365_failsafe(new_count: int, previous_count: int = None) -> tuple:
    """Evaluate publish failsafe (same logic as Azure feed)."""
    if previous_count is None:
        return False, "baseline_unavailable"
    
    relative_trigger = (new_count / max(previous_count, 1)) < FAILSAFE_MIN_RATIO
    absolute_trigger = new_count < FAILSAFE_MIN_ARTICLES and previous_count >= FAILSAFE_MIN_ARTICLES
    
    details = f"relative_trigger={relative_trigger}, absolute_trigger={absolute_trigger}"
    triggered = relative_trigger or absolute_trigger
    
    return triggered, details


def main():
    """Main entry point: fetch M365 data and write to files."""
    print("Starting M365 Feed Data Fetch...")
    
    session = create_http_session()
    
    try:
        # Fetch items from DeltaPulse
        raw_items = fetch_m365_items(session)
        
        if not raw_items:
            print("Warning: No items fetched from DeltaPulse MCP")
        
        # Build feed structure
        feed_data = build_m365_feed(raw_items)
        
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
            write_m365_checksums(M365_DATA_OUTPUT)
            print("\n✓ M365 feed data fetch completed successfully")
        else:
            print("\n✗ Failed to write M365 data")
            return 1
        
        return 0
    
    finally:
        session.close()


if __name__ == "__main__":
    exit(main())
