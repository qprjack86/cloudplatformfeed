
# ☁️ Microsoft Cloud Platform Feed

A daily-updated Microsoft cloud news aggregator hosted on GitHub Pages. It collects articles from Azure and Microsoft 365 blogs and presents them in a clean, searchable interface covering the last 30 days.

**Live site:** [cloudplatformfeed.kailice.uk](https://cloudplatformfeed.kailice.uk)

## Features

- 📰 **50+ sources** — Azure blogs, Microsoft 365 blogs, Azure Updates, DevOps, Security, Developer Tools, Data & AI, and more
- 🔄 **Microsoft 365 integration** — Fetches and displays Microsoft 365 Roadmap and Message Center updates alongside Azure news, with product categorisation and lifecycle status
- 🎬 **Latest update videos** — Shows the latest Azure and Microsoft 365 update videos in the UI
- 🤖 **AI-generated summaries** — Summaries for both Azure and M365 feeds (if configured)
- 🔍 **Search & filter** — Find articles by keyword, blog category, product area, or date range
- ⭐ **Bookmarks** — Save articles for later (stored locally per browser)
- 🌙 **Dark mode** — Easy on the eyes
- 📱 **Responsive** — Works on desktop, tablet, and mobile
- 🧭 **Tabbed navigation** — Switch between Azure and M365 feeds with a single click
- 🛠️ **Debug & schema tools** — Scripts for DeltaPulse MCP debugging, schema discovery, and deduplication logic
- 🤖 **Auto-updated** — GitHub Actions fetches new articles and M365 data three times a day at 8 AM, 12 PM, and 4 PM UTC
- 📅 **Last 30 days** — Keeps only recent articles for a lean, fast experience
- 🔒 **Hardened publish surface** — AI summary failures expose only safe status codes, not raw Azure OpenAI diagnostics
- 🛡️ **Browser hardening** — CSP and referrer policy limit third-party script and data exposure while preserving Microsoft Clarity

## Blog Sources

| Area | Coverage |
| ---------- | ----- |
| **Azure** | Compute, Data & AI, Infrastructure, Security, Architecture, Apps & Platform, Operations, Community, Developer Tools, and specialized Azure product blogs |
| **Microsoft 365** | Microsoft 365 apps, Teams, SharePoint, OneDrive, Exchange, Microsoft Viva, Microsoft 365 Defender, Purview, Intune, Copilot, and related admin/community blogs |

## New & Updated Scripts

- `scripts/fetch_m365_data.py` — Fetches Microsoft 365 Roadmap and Message Center items from DeltaPulse MCP, writes to `data/m365_data.json` and `data/m365_checksums.json`.
- `scripts/debug_mcp.py` — Debugs DeltaPulse MCP tool calls, prints payloads and responses.
- `scripts/discover_deltapulse_schema.py` — Discovers available MCP tools and schemas.
- `scripts/debug_dedup.py` — Tests and debugs M365 deduplication logic.

## Data Files

- `data/m365_data.json` — Microsoft 365 articles, categories, and video metadata
- `data/m365_checksums.json` — Checksum and generation metadata for M365 data
- `data/feeds.json`, `data/feed.xml`, `data/checksums.json` — Azure and combined feed data and checksums

## UI/UX Improvements

- Tabbed navigation for Azure/M365 feeds
- Product category mapping and filtering for M365 (including complete mapping for Entra, OneDrive, Defender, and Windows)
- AI-generated summaries for both Azure and M365 (calculating precise 7-day windows anchored to the most recent matching article)
- Video panels for latest Azure and M365 update videos

## Workflows

- `.github/workflows/fetch-feeds.yml` — Fetches both Azure and M365 feeds, runs tests, commits new data
- `.github/workflows/smoke-check.yml` — Runs Python unit tests and Ruby smoke checks on push/PR

## Configuration

- `config/site.json` — Canonical host and URL for site validation, used by both fetch scripts
- Environment variables for Azure OpenAI (for AI summaries): `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_VERSION`, `AZURE_OPENAI_DEPLOYMENT`

## Dependencies

- **Python 3.12+** — Required for all scripts

- **Ruby 3.3+** — Required for smoke tests
- **Python packages:** `feedparser`, `openai`, `requests` (see `scripts/requirements.in` and `scripts/requirements.txt`)

---

## Setup

### 1. Create the GitHub repository

```bash
gh repo create cloudplatformfeed --public --source=. --remote=origin
```

### 2. Push the code

```bash
git init
git add .
git commit -m "Initial commit - Microsoft Cloud Platform Feed"
git push -u origin main
```

### 3. Enable GitHub Pages

Go to **Settings → Pages → Source** and select **Deploy from a branch** → **main** → **/ (root)**.

### 4. Trigger the first data fetch

Go to **Actions → Fetch Cloud Platform Feeds → Run workflow** to populate the initial data. The workflow fetches both Azure and Microsoft 365 feeds and runs all tests. Data is committed only if new articles are found.

### 5. Visit your site

Your feed will be live at `https://cloudplatformfeed.kailice.uk`

## Local Development

To test the feed fetchers locally:

1. **Set up environment variables for AI summaries (optional):**

  ```bash
  export AZURE_OPENAI_API_KEY="<your-azure-openai-key>"
  export AZURE_OPENAI_ENDPOINT="https://<your-resource-name>.openai.azure.com"
  export AZURE_OPENAI_API_VERSION="2024-02-15-preview"
  export AZURE_OPENAI_DEPLOYMENT="gpt-4o-mini"
  ```

2. **Install dependencies:**

  ```bash
  pip install -r scripts/requirements.txt
  ```

3. **Fetch Azure and M365 feeds:**

  ```bash
  python scripts/fetch_feeds.py
  python scripts/fetch_m365_data.py
  ```

4. **Run tests:**

  ```bash
  python -m unittest discover -s tests -p "test_*.py"
  ruby scripts/smoke_test.rb
  ```

### Python dependency locking (maintainers)

- `scripts/requirements.in` is the source of truth for direct Python dependencies.
- `scripts/requirements.txt` is generated/locked output and should not be edited by hand.
- When updating dependencies, regenerate the lock file and commit both files together:

  ```bash
  pip install pip-tools
  pip-compile --upgrade --output-file scripts/requirements.txt scripts/requirements.in
  ```

### Canonical site URL (maintainers)

- Canonical host/URL is centralized in `config/site.json`.
- Keep `canonicalHost` and `canonicalUrl` in sync (`https://<host>`).
- Smoke checks enforce consistency across `CNAME`, `index.html` metadata, `README.md`, and generated RSS output.

### Publish fail-safe (maintainers)

- Feed generation compares the newly deduplicated article count against the previously published `data/feeds.json` and `data/m365_data.json`.
- Guard thresholds in `scripts/fetch_feeds.py` and `scripts/fetch_m365_data.py` are:
  - `FAILSAFE_MIN_ARTICLES = 80`
  - `FAILSAFE_MIN_RATIO = 0.60`
- A run skips publishing (keeps existing feed files unchanged) when the new count is below either threshold rule:
  - `new_count < ceil(previous_count * 0.60)`
  - `previous_count >= 80` and `new_count < 80`

### CI run observability (maintainers)

- `fetch-feeds` workflow writes core run metrics to JSON when `AZUREFEED_RUN_METRICS_PATH` is set.
- Each fetch run publishes a GitHub Actions Step Summary with feed volume, fail-safe status, AI summary status, and commit outcome.
- The same metrics payload is uploaded as the `azurefeed-run-metrics` artifact (14-day retention) for debugging.

If these variables are not set, feed fetching still works and the site will show that the AI summary is unavailable for that update. The published JSON now exposes only a safe summary status and reason code, never raw Azure OpenAI error text.

Feed retrieval is also hardened before parsing: only the configured HTTPS feed hosts are requested, requests use explicit timeouts and bounded retries, and article deduplication normalises URLs to drop common tracking parameters before duplicate checks.

For GitHub Actions, add the same values as repository secrets: `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_VERSION`, and `AZURE_OPENAI_DEPLOYMENT`.

Then serve the site:

```bash
python -m http.server 8000
```

Open [http://localhost:8000](http://localhost:8000) in your browser.

## How It Works

1. **GitHub Actions** runs at 8 AM, 12 PM, and 4 PM UTC each day (or manually)
2. **Python scripts** fetch RSS feeds from Azure, Microsoft 365, and Microsoft developer blogs plus Azure Updates using allowlisted HTTPS requests with explicit timeouts and retries
3. Articles from the last 30 days are deduplicated with canonical URL normalization, sorted, and saved to `data/feeds.json` (Azure) and `data/m365_data.json` (M365)
4. The commit triggers **GitHub Pages** to redeploy
5. The **static frontend** loads the JSON, applies viewer-local date grouping/filtering, and renders the feed with tabbed navigation for Azure and M365

## Security Notes

- The site uses a meta Content Security Policy and referrer policy because GitHub Pages does not provide a native way to set custom response headers for this static site.
- Microsoft Clarity telemetry is integrated smoothly, with the CSP explicitly tuned to securely permit its dynamic load-balancer endpoints (`*.clarity.ms`) and required inline execution.
- AI summary failures are logged in CI, but public feed data includes only safe summary reason codes.
- Each successful fetch now writes `data/checksums.json` after `data/feeds.json` and `data/feed.xml` are finalised. The file records the artifact path, `sha256` algorithm, digest, and generation timestamp for both published outputs.
- During incident review or debugging, compare the published artifacts against `data/checksums.json` to confirm whether a suspicious file matches the last known generated content, or to spot unexpected post-generation changes.

## Acknowledgements

This project was originally built from <https://github.com/ricmmartins/azurenewsfeed>, but has since been extensively customised and refactored into a standalone application.

## License

MIT
