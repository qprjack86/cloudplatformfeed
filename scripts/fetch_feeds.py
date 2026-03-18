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
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone
from html import unescape

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
SUMMARY_MAX_ARTICLES = 20

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


def normalize_summary_text(summary_text):
    """Normalize model phrasing so it reflects a multi-day summary window."""
    normalized = summary_text or ""
    normalized = re.sub(
        r"\bnone today\b", "none noted in selected window", normalized, flags=re.IGNORECASE
    )
    normalized = re.sub(
        r"\btoday\b", "the selected window", normalized, flags=re.IGNORECASE
    )
    return normalized


def _normalize_for_match(text):
    """Normalize text for fuzzy title matching."""
    value = (text or "").lower()
    value = re.sub(r"\[[^\]]+\]", " ", value)
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


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
            return date_str

    return datetime.now(timezone.utc).isoformat()


def fetch_tech_community_feeds():
    """Fetch articles from Tech Community blogs."""
    articles = []

    for board_id, blog_name in BLOGS.items():
        url = TC_RSS_URL.format(board=board_id)
        print(f"Fetching: {blog_name} ({board_id})...")

        try:
            feed = feedparser.parse(url)

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
        feed = feedparser.parse(AKS_BLOG_FEED)

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
            feed = feedparser.parse(feed_url)

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
        feed = feedparser.parse(SAVILL_YOUTUBE_RSS)
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
        feed = feedparser.parse(AZURE_UPDATES_FEED)

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

        # Scale article cap and token budget with the number of days being summarised
        max_articles = SUMMARY_MAX_ARTICLES * len(summary_days)
        max_tokens = min(250 * len(summary_days), 800)
        window_desc = summary_days[-1] + " to " + summary_days[0]

        azure_updates_articles = [
            a for a in day_articles if a.get("blogId") == "azureupdates"
        ]
        if azure_updates_articles:
            remaining_articles = [
                article
                for article in day_articles
                if article.get("blogId") != "azureupdates"
            ]
            summary_articles = (
                azure_updates_articles[:max_articles]
                + remaining_articles[: max(0, max_articles - len(azure_updates_articles[:max_articles]))]
            )
            prompt = (
                "You are an Azure Updates lifecycle editor. Summarize only items from Azure "
                "Updates and recent Azure news. Prioritize Azure Updates entries where blogId "
                "is azureupdates, but use the additional recent items for context when helpful. "
                "Build a short structured summary with exactly 3 sections using these headings: "
                "'In preview', 'Launched / Generally Available', and 'In development'. "
                "Format each section like this example:\n"
                "- In preview:\n  \u2022 item one\n  \u2022 item two\n\n"
                "List each item on its own line starting with '  \u2022 '. "
                "Do NOT use semicolons to separate items. "
                "If a section has no strong match write '  \u2022 none noted in selected window'. "
                "Keep each bullet to one concise line; call out the key service or feature name. "
                "Do not use the word 'today' anywhere because this is a multi-day digest. "
                "Selected publishing-day range: "
                + window_desc
                + ". Here are the recent items for publishing days "
                + ", ".join(summary_days)
                + ":\n\n"
            )
        else:
            print(
                "No Azure Updates entries found in configured publishing-day window; summarizing recent articles"
            )
            summary_articles = day_articles[:max_articles]
            prompt = (
                "You are an Azure cloud editor. Create a concise AI summary over the selected "
                "recent publishing days with exactly 3 sections under these headings: 'Platform "
                "launches', 'In preview', and 'Developer / operations notes'. Focus on concrete "
                "product news, major releases, and notable platform changes. "
                "Format each section like this example:\n"
                "- Platform launches:\n  • [item one](https://example.com/post-1)\n  • [item two](https://example.com/post-2)\n\n"
                "List each item on its own line starting with '  • '. "
                "Each bullet must be a markdown link in the form [short title](url). "
                "Use only URLs from the provided list and do not invent or alter links. "
                "If a section has no strong match, write '  • none noted in selected window'. "
                "Do not use the word "
                "'today' anywhere because this is a multi-day digest. Selected publishing-day "
                "range: "
                + window_desc
                + ". Here are the recent articles for publishing "
                "days "
                + ", ".join(summary_days)
                + ":\n\n"
            )

        titles = "\n".join(
            [
                "- "
                + a["title"]
                + " | source="
                + a["blog"]
                + " | blogId="
                + a.get("blogId", "")
                + " | url="
                + a.get("link", "")
                for a in summary_articles
            ]
        )
        prompt += titles

        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version,
        )
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise Azure release editor. Follow formatting instructions exactly.",
                },
                {"role": "user", "content": prompt},
            ],
            max_completion_tokens=max_tokens,
        )
        summary = normalize_summary_text(response.choices[0].message.content.strip())
        summary = attach_links_to_summary(summary, summary_articles)
        print(f"AI summary generated: {summary[:100]}...")
        return {
            "status": "available",
            "summary": summary,
            "source": "azure-openai",
            "windowDays": len(summary_days),
            "publishingDays": summary_days,
            "articleCount": len(summary_articles),
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
            "error": error_msg,
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

    # Remove duplicates by link and discard articles older than 30 days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    seen_links = set()
    unique_articles = []
    for article in all_articles:
        if article["link"] and article["link"] not in seen_links:
            if article.get("published", "") >= cutoff:
                seen_links.add(article["link"])
                unique_articles.append(article)

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
        data["summaryReason"] = summary_payload["reason"]
    if summary_payload.get("error"):
        data["summaryError"] = summary_payload["error"]
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
