# ☁️ Azure News Feed

A daily-updated Azure blog aggregator hosted on GitHub Pages. Collects articles from Azure blogs and presents them in a clean, searchable interface — last 30 days only.

**Live site:** [azurefeed.kailice.uk](https://azurefeed.kailice.uk)

## Features

- 📰 **49 sources** — Azure blogs, Azure Updates, DevOps, Security, Developer Tools, Data & AI, and more
- 🔍 **Search & filter** — Find articles by keyword, blog category, or date range
- ⭐ **Bookmarks** — Save articles for later (stored locally per browser)
- 🌙 **Dark mode** — Easy on the eyes
- 📱 **Responsive** — Works on desktop, tablet, and mobile
- 🤖 **Auto-updated** — GitHub Actions fetches new articles three times a day at 8 AM, 12 PM, and 4 PM UTC
- 📅 **Last 30 days** — Keeps only recent articles for a lean, fast experience
- 🔒 **Hardened publish surface** — AI summary failures expose only safe status codes, not raw Azure OpenAI diagnostics
- 🛡️ **Browser hardening** — CSP and referrer policy limit third-party script and data exposure while preserving Microsoft Clarity

## Blog Sources

| Category | Blogs |
| ---------- | ----- |
| **Compute** | Azure Compute, AKS, Azure Virtual Desktop, High Performance Computing |
| **Data & AI** | Analytics on Azure, Azure Databricks, Oracle on Azure, Cosmos DB, Azure SQL, Microsoft Foundry |
| **Infrastructure** | Azure Infrastructure, Azure Arc, Azure Stack, Azure Networking, Azure Storage |
| **Security** | Azure Network Security, Microsoft Sentinel, Microsoft Defender for Cloud, Azure Advanced Threat Protection |
| **Architecture** | Azure Architecture, Customer Innovation, ISE Developer Blog |
| **Apps & Platform** | Apps on Azure, Azure PaaS, Integrations, Messaging, Aspire, Azure SDK |
| **Operations** | Governance & Management, Observability, FinOps, Azure Tools, Migration, Azure DevOps |
| **Community** | Azure Dev Community, Azure Events, Linux & Open Source, All Things Azure, Microsoft Developers Blog |
| **Developer Tools** | Visual Studio, VS Code, Windows Command Line, Develop from the Cloud |
| **Specialized** | Communication Services, Confidential Computing, Maps, Telecommunications, Planetary Computer |

## Setup

### 1. Create the GitHub repository

```bash
gh repo create azurenewsfeed --public --source=. --remote=origin
```

### 2. Push the code

```bash
git init
git add .
git commit -m "Initial commit - Azure News Feed"
git push -u origin main
```

### 3. Enable GitHub Pages

Go to **Settings → Pages → Source** and select **Deploy from a branch** → **main** → **/ (root)**.

### 4. Trigger the first data fetch

Go to **Actions → Fetch Azure Blog Feeds → Run workflow** to populate the initial data.
The regeneration workflow now runs only on its daily schedule or from manual dispatch. It no longer writes generated files back on every source push.

### 5. Visit your site

Your feed will be live at `https://azurefeed.kailice.uk`

## Local Development

To test the feed fetcher locally, configure Azure OpenAI for optional AI summaries:

```bash
export AZURE_OPENAI_API_KEY="<your-azure-openai-key>"
export AZURE_OPENAI_ENDPOINT="https://<your-resource-name>.openai.azure.com"
export AZURE_OPENAI_API_VERSION="2024-02-15-preview"
export AZURE_OPENAI_DEPLOYMENT="gpt-4o-mini"
```

The number of recent publishing days included in the AI summary is controlled in `scripts/fetch_feeds.py` via `SUMMARY_WINDOW_DAYS`. Change that constant to values like `1`, `3`, or `7` depending on how broad you want the AI summary to be.

Then run the fetcher:

```bash
pip install -r scripts/requirements.txt
python scripts/fetch_feeds.py
```

If these variables are not set, feed fetching still works and the site will show that the AI summary is unavailable for that update. The published JSON now exposes only a safe summary status and reason code, never raw Azure OpenAI error text.

Feed retrieval is also hardened before parsing: only the configured HTTPS feed hosts are requested, requests use explicit timeouts and bounded retries, and article deduplication normalizes URLs to drop common tracking parameters before duplicate checks.

For GitHub Actions, add the same values as repository secrets: `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_VERSION`, and `AZURE_OPENAI_DEPLOYMENT`.

Then serve the site:

```bash
python -m http.server 8000
```

Open [http://localhost:8000](http://localhost:8000) in your browser.

## How It Works

1. **GitHub Actions** runs at 8 AM, 12 PM, and 4 PM UTC each day (or manually)
2. **Python script** fetches RSS feeds from Azure and Microsoft developer blogs plus Azure Updates using allowlisted HTTPS requests with explicit timeouts and retries
3. Articles from the last 30 days are deduplicated with canonical URL normalization, sorted, and saved to `data/feeds.json`
4. The commit triggers **GitHub Pages** to redeploy
5. The **static frontend** loads the JSON, applies viewer-local date grouping/filtering, and renders the feed

## Security Notes

- The site uses a meta Content Security Policy and referrer policy because GitHub Pages does not provide a native way to set custom response headers for this static site.
- Microsoft Clarity is loaded from a first-party bootstrap file so the page does not need inline scripts.
- AI summary failures are logged in CI, but public feed data includes only safe summary reason codes.

## License

MIT
