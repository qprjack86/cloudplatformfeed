(function () {
  "use strict";

  // ===== Category Mapping =====
  var CATEGORIES = {
    "Compute": ["azurecompute", "aksblog", "azurevirtualdesktopblog", "azurehighperformancecomputingblog"],
    "Data & AI": ["analyticsonazure", "azure-databricks", "oracleonazureblog", "cosmosdbblog", "azuresqlblog", "foundryblog"],
    "Infrastructure": ["azureinfrastructureblog", "azurearcblog", "azurestackblog", "azurenetworkingblog", "azurestorageblog"],
    "Security": ["azurenetworksecurityblog", "microsoftsentinelblog", "microsoftdefendercloudblog", "azureadvancedthreatprotection"],
    "Architecture": ["azurearchitectureblog", "azure-customer-innovation-blog", "iseblog"],
    "Apps & Platform": ["appsonazureblog", "azurepaasblog", "integrationsonazureblog", "messagingonazureblog", "aspireblog", "azuresdkblog"],
    "Operations": ["azuregovernanceandmanagementblog", "azureobservabilityblog", "finopsblog", "azuretoolsblog", "azuremigrationblog", "azuredevops", "azureupdates"],
    "Community": ["azuredevcommunityblog", "azure-events", "linuxandopensourceblog", "allthingsazure", "msdevblog"],
    "Developer Tools": ["visualstudio", "vscodeblog", "commandline", "developfromthecloud"],
    "Specialized": ["azurecommunicationservicesblog", "azureconfidentialcomputingblog", "azuremapsblog", "telecommunications-industry-blog", "microsoft-planetary-computer-blog"]
  };

  var AZURE_UPDATES_BLOG_ID = "azureupdates";
  var AZURE_UPDATES_CATEGORY_KEYWORDS = {
    "Compute": ["batch", "virtual machine", "vm", "aks", "kubernetes", "gpu", "container", "app service"],
    "Data & AI": ["sql", "database", "cosmos", "databricks", "ai", "openai", "machine learning", "fabric", "synapse"],
    "Infrastructure": ["network", "vnet", "storage", "backup", "disaster recovery", "firewall", "load balancer", "vpn", "expressroute"],
    "Security": ["sentinel", "defender", "security", "threat", "idps", "siem", "soc", "identity", "entra", "waf", "firewall"],
    "Architecture": ["architecture", "well-architected", "reference architecture", "design pattern"],
    "Apps & Platform": ["api management", "functions", "logic apps", "service bus", "event grid", "web app", "integration"],
    "Operations": ["monitor", "observability", "policy", "governance", "cost", "finops", "devops", "migration", "retirement", "support"],
    "Community": ["event", "community", "conference", "meetup", "hackathon"],
    "Developer Tools": ["visual studio", "vscode", "sdk", "cli", "powershell", "bicep", "terraform", "github"],
    "Specialized": ["iot", "maps", "quantum", "confidential", "communication services", "telecommunications", "planetary"]
  };

  // ===== State =====
  var articles = [];
  var filteredArticles = [];
  var currentCategory = "all";
  var currentFilter = "all";
  var currentSource = "azure";  // New: track active feed source (azure|m365)
  var searchQuery = "";
  var sortBy = "date-desc";
  var PAGE_SIZE = 30;
  var renderedCount = 0;
  var bookmarks = new Set(
    JSON.parse(localStorage.getItem("cloudplatformfeed-bookmarks") || "[]")
  );
  var showBookmarksOnly = false;
  var showOtherBlogs = localStorage.getItem("cloudplatformfeed-other-blogs") === "true";

  // Color palette for blog tags
  var blogColors = {};
  var blogColorClasses = {};
  var colorPalette = [
    "#BD8D32", "#1F2C35", "#7719AA", "#E3008C", "#D83B01",
    "#107C10", "#008575", "#4F6BED", "#B4009E", "#C239B3",
    "#E81123", "#FF8C00", "#00B294", "#68217A", "#0063B1",
    "#2D7D9A", "#5C2D91", "#CA5010", "#038387", "#8764B8",
    "#567C73", "#C30052", "#6B69D6", "#8E8CD8", "#00B7C3",
    "#EE5E00", "#847545", "#5D5A58", "#767676", "#4C4A48",
    "#0099BC",
  ];

  // ===== DOM Elements =====
  var articlesGrid = document.getElementById("articles-grid");
  var loadingEl = document.getElementById("loading");
  var noResultsEl = document.getElementById("no-results");
  var searchInput = document.getElementById("search-input");
  var sortSelect = document.getElementById("sort-by");
  var dateFilter = document.getElementById("date-filter");
  var themeToggle = document.getElementById("theme-toggle");
  var filterPills = document.getElementById("filter-pills");
  var showingCount = document.getElementById("showing-count");
  var lastUpdated = document.getElementById("last-updated");
  var totalCount = document.getElementById("total-count");
  var headerEl = document.querySelector("header");
  var toastEl = document.getElementById("toast");
  var bookmarksToggle = document.getElementById("bookmarks-toggle");
  var otherBlogsToggle = document.getElementById("other-blogs-toggle");
  var aiSummaryEl = document.getElementById("ai-summary");
  var savillVideoEl = document.getElementById("savill-video");
  var subtitleEl = document.querySelector(".subtitle");
  var tabsContainerEl = document.querySelector(".tabs-container");

  var azureFeedData = null;
  var m365FeedData = null;
  
  // Tab buttons (M365 feature)
  var tabButtons = document.querySelectorAll(".tab-button");
  var SUMMARY_REASON_MESSAGES = {
    no_dated_articles: "No recent dated articles were available to summarise.",
    no_articles_in_window: "No recent articles were available in the current summary window.",
    missing_azure_openai_config: "AI summary generation is not configured for this refresh.",
    azure_openai_failed: "AI summary generation was temporarily unavailable for this refresh."
  };

  var LIFECYCLE_LABELS = {
    in_preview: "In preview",
    launched_ga: "Launched / GA",
    retiring: "Retiring",
    in_development: "In development"
  };
  var AZURE_LIFECYCLE_FILTER_ORDER = [
    "launched_ga",
    "in_preview",
    "in_development",
    "retiring",
    "unknown"
  ];
  var AZURE_LIFECYCLE_FILTER_LABELS = {
    launched_ga: "Launched / GA",
    in_preview: "In preview",
    in_development: "In development",
    retiring: "Retiring",
    unknown: "Unknown"
  };

  function showElement(element) {
    if (element) element.classList.remove("is-hidden");
  }

  function hideElement(element) {
    if (element) element.classList.add("is-hidden");
  }

  function parseDateValue(value) {
    if (!value) return null;
    var date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function getArticleDate(article) {
    if (!article) return null;
    return (
      parseDateValue(article.published) ||
      parseDateValue(article.publishedDate) ||
      parseDateValue(article.updated) ||
      parseDateValue(article.modified)
    );
  }

  function parsePublishingDay(day) {
    var match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day || "");
    if (!match) return null;
    return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  }

  function formatLocalDate(date, options) {
    return date ? date.toLocaleDateString(undefined, options) : "";
  }

  function formatUkNumericDate(date) {
    if (!date) return "";
    return date.toLocaleDateString("en-GB", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric"
    });
  }

  function formatM365TargetDate(value) {
    if (!value) return "";
    var raw = String(value).trim();
    if (!raw) return "";

    var cycleMonth = /^([A-Za-z]+)\s+(?:CY|FY)(\d{4})$/.exec(raw);
    if (cycleMonth) {
      raw = cycleMonth[1] + " " + cycleMonth[2];
    }

    var monthOnly = /^(\d{4})-(\d{2})$/.exec(raw);
    if (monthOnly) {
      var monthDate = new Date(Number(monthOnly[1]), Number(monthOnly[2]) - 1, 1);
      return formatLocalDate(monthDate, { month: "short", year: "numeric" });
    }

    var dayMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
    if (dayMatch) {
      var dayDate = new Date(Number(dayMatch[1]), Number(dayMatch[2]) - 1, Number(dayMatch[3]));
      return formatUkNumericDate(dayDate);
    }

    // Preserve fuzzy month timing (for example: "late June 2026") and
    // month-only values ("June 2026") instead of guessing a day-of-month.
    if (/^(?:early|mid|late)\s+[A-Za-z]+\s+\d{4}$/i.test(raw)) {
      return raw;
    }
    if (/^[A-Za-z]+\s+\d{4}$/.test(raw)) {
      return raw;
    }

    // Only parse free-form dates when a specific day is present.
    var hasExplicitDay = /^([A-Za-z]+)\s+\d{1,2},\s*\d{4}$/.test(raw);
    var parsed = hasExplicitDay ? parseDateValue(raw) : null;
    if (parsed && !Number.isNaN(parsed.getTime())) {
      return formatUkNumericDate(parsed);
    }

    return raw;
  }

  function parseM365TargetDateParts(value) {
    if (!value) return [];
    return String(value)
      .split(",")
      .map(function (part) { return part.trim(); })
      .filter(function (part) { return Boolean(part); });
  }

  function parseM365MonthDate(value) {
    if (!value) return null;
    var raw = String(value).trim();
    if (!raw) return null;

    var cycleMonth = /^([A-Za-z]+)\s+(?:CY|FY)(\d{4})$/i.exec(raw);
    if (cycleMonth) {
      raw = cycleMonth[1] + " " + cycleMonth[2];
    }

    var monthOnly = /^(\d{4})-(\d{2})$/.exec(raw);
    if (monthOnly) {
      return new Date(Number(monthOnly[1]), Number(monthOnly[2]) - 1, 1);
    }

    var monthYear = /^([A-Za-z]+)\s+(\d{4})$/.exec(raw);
    if (monthYear) {
      var parsed = new Date(monthYear[1] + " 1, " + monthYear[2]);
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    return null;
  }

  function toMonthKey(date) {
    if (!date) return "";
    return (date.getFullYear() * 12 + date.getMonth()).toString();
  }

  function buildM365TargetDatePills(targetDateValue) {
    var parts = parseM365TargetDateParts(targetDateValue);
    if (!parts.length) return [];

    if (parts.length === 1) {
      var single = formatM365TargetDate(parts[0]);
      return single ? [{ label: "Expected Release", value: single }] : [];
    }

    var datedParts = parts.map(function (part) {
      return {
        raw: part,
        formatted: formatM365TargetDate(part),
        monthDate: parseM365MonthDate(part)
      };
    });

    var allHaveMonthDates = datedParts.every(function (item) { return Boolean(item.monthDate); });
    if (!allHaveMonthDates) {
      var combined = formatM365TargetDate(targetDateValue);
      return combined ? [{ label: "Expected Release", value: combined }] : [];
    }

    // Group contiguous months into rollout windows.
    var windows = [];
    datedParts.forEach(function (item) {
      if (!windows.length) {
        windows.push([item]);
        return;
      }

      var currentWindow = windows[windows.length - 1];
      var previousItem = currentWindow[currentWindow.length - 1];
      var previousMonth = Number(toMonthKey(previousItem.monthDate));
      var currentMonth = Number(toMonthKey(item.monthDate));

      if ((currentMonth - previousMonth) <= 1) {
        currentWindow.push(item);
      } else {
        windows.push([item]);
      }
    });

    if (windows.length < 2) {
      var joinedSingleWindow = datedParts
        .map(function (item) { return item.formatted; })
        .join(", ");
      return joinedSingleWindow ? [{ label: "Expected Release", value: joinedSingleWindow }] : [];
    }

    return windows.map(function (window, idx) {
      var label = idx === 0 ? "Preview" : (idx === 1 ? "GA" : "Expected Release");
      var value = window.map(function (item) { return item.formatted; }).join(", ");
      return { label: label, value: value };
    }).filter(function (pill) { return Boolean(pill.value); });
  }

  function formatUkRetirementDate(value) {
    if (!value) return "";
    var raw = String(value).trim();
    if (!raw) return "";

    var dayMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
    if (dayMatch) {
      var year = dayMatch[1];
      var month = dayMatch[2];
      var day = dayMatch[3];
      return day + "/" + month + "/" + year;
    }

    var monthMatch = /^(\d{4})-(\d{2})$/.exec(raw);
    if (monthMatch) {
      var monthDate = new Date(Number(monthMatch[1]), Number(monthMatch[2]) - 1, 1);
      return formatLocalDate(monthDate, { month: "short", year: "numeric" });
    }

    return raw;
  }

  function formatLocalDateTime(date) {
    return date
      ? date.toLocaleDateString(undefined, {
          weekday: "short",
          year: "numeric",
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit"
        })
      : "";
  }

  function renderSummaryHtml(text) {
    function renderBulletContent(content) {
      var html = "";
      var cursor = 0;
      var linkRe = /\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g;
      var match;
      while ((match = linkRe.exec(content)) !== null) {
        html += escapeHtml(content.slice(cursor, match.index));
        html +=
          '<a class="ai-link" href="' +
          escapeHtml(match[2]) +
          '" target="_blank" rel="noopener noreferrer">' +
          escapeHtml(match[1]) +
          "</a>";
        cursor = match.index + match[0].length;
      }
      html += escapeHtml(content.slice(cursor));
      return html;
    }

    var html = "";
    var sectionRe = /^- (.+?):[ \t]*$/;
    var bulletRe = /^[ \t]+[•\-\*] (.+)$/;
    var lines = String(text || "").split(/\n/);
    var inList = false;
    lines.forEach(function (line) {
      var sec = line.match(sectionRe);
      var bul = line.match(bulletRe);
      if (sec) {
        if (inList) { html += "</ul>"; inList = false; }
        html += '<div class="ai-section"><h3>' + escapeHtml(sec[1]) + "</h3><ul>";
        inList = true;
      } else if (bul && inList) {
        html += "<li>" + renderBulletContent(bul[1]) + "</li>";
      } else if (line.trim()) {
        if (inList) { html += "</ul>"; inList = false; html += "</div>"; }
        html += "<p>" + escapeHtml(line) + "</p>";
      }
    });
    if (inList) { html += "</ul></div>"; }
    return html || "<p>" + escapeHtml(text || "") + "</p>";
  }

  function resolveArticleOutboundLink(article) {
    if (!article) return "";
    if ((article.source || "azure") === "m365") {
      return article.link || "";
    }
    return article.link || "";
  }

  function buildLifecycleSummaryMarkdown(byLifecycle) {
    var order = ["in_preview", "launched_ga", "retiring", "in_development"];
    var lines = [];

    order.forEach(function (key) {
      var label = LIFECYCLE_LABELS[key];
      var bucket = Array.isArray(byLifecycle && byLifecycle[key]) ? byLifecycle[key] : [];
      lines.push("- " + label + ":");

      if (!bucket.length) {
        lines.push("  • none noted in selected window");
        return;
      }

      bucket.slice(0, 6).forEach(function (article) {
        var articleTitle = article.title || "Untitled update";
        var articleLink = resolveArticleOutboundLink(article);
        if (articleLink) {
          lines.push("  • [" + articleTitle + "](" + articleLink + ")");
        } else {
          lines.push("  • " + articleTitle);
        }
      });
    });

    return lines.join("\n");
  }

  function renderSummaryPanel() {
    if (!aiSummaryEl) return;

    if (currentSource === "m365") {
      if (!m365FeedData) {
        hideElement(aiSummaryEl);
        return;
      }

      var m365Days = Array.isArray(m365FeedData.summaryPublishingDays)
        ? m365FeedData.summaryPublishingDays
        : [];

      function toM365Date(day) {
        return formatLocalDate(parsePublishingDay(day), {
          day: "numeric",
          month: "short",
          year: "numeric"
        });
      }

      var m365DateLabel = "";
      if (m365Days.length >= 2) {
        m365DateLabel = toM365Date(m365Days[m365Days.length - 1]) + " – " + toM365Date(m365Days[0]);
      } else if (m365Days.length === 1) {
        m365DateLabel = toM365Date(m365Days[0]);
      }
      var m365Label = "Microsoft 365 Updates" + (m365DateLabel ? ": " + m365DateLabel : "");
      var m365SummaryText = m365FeedData.summary || buildLifecycleSummaryMarkdown(m365FeedData.byLifecycle || {});

      aiSummaryEl.innerHTML =
        "<h2>🤖 " + escapeHtml(m365Label) + "</h2>" +
        renderSummaryHtml(m365SummaryText);
      aiSummaryEl.classList.remove("is-unavailable");
      showElement(aiSummaryEl);
      return;
    }

    if (!azureFeedData) {
      hideElement(aiSummaryEl);
      return;
    }

    if (azureFeedData.summary) {
      var publishingDays = Array.isArray(azureFeedData.summaryPublishingDays)
        ? azureFeedData.summaryPublishingDays
        : [];

      function toLocalPublishingDate(day) {
        return formatLocalDate(parsePublishingDay(day), {
          day: "numeric",
          month: "short",
          year: "numeric"
        });
      }

      var azureDateLabel = "";
      if (publishingDays.length >= 2) {
        var oldest = publishingDays[publishingDays.length - 1];
        var newest = publishingDays[0];
        azureDateLabel = toLocalPublishingDate(oldest) + " – " + toLocalPublishingDate(newest);
      } else if (publishingDays.length === 1) {
        azureDateLabel = toLocalPublishingDate(publishingDays[0]);
      }
      var summaryLabel = "Azure Updates" + (azureDateLabel ? ": " + azureDateLabel : "");

      aiSummaryEl.innerHTML =
        "<h2>🤖 " + escapeHtml(summaryLabel) + "</h2>" +
        renderSummaryHtml(azureFeedData.summary);
      aiSummaryEl.classList.remove("is-unavailable");
      showElement(aiSummaryEl);
      return;
    }

    if (azureFeedData.summaryStatus === "unavailable") {
      var unavailMsg = "Azure OpenAI did not return a summary for this update.";
      if (azureFeedData.summaryReason && SUMMARY_REASON_MESSAGES[azureFeedData.summaryReason]) {
        unavailMsg += "<br><small class=\"ai-summary-note\">" +
          escapeHtml(SUMMARY_REASON_MESSAGES[azureFeedData.summaryReason]) + "</small>";
      }
      aiSummaryEl.innerHTML =
        "<h2>🤖 AI Summary Unavailable</h2>" +
        "<p>" + unavailMsg + "</p>";
      aiSummaryEl.classList.add("is-unavailable");
      showElement(aiSummaryEl);
      return;
    }

    hideElement(aiSummaryEl);
  }

  function renderSavillVideoPanel() {
    if (!savillVideoEl) return;

    if (currentSource !== "azure" || !azureFeedData || !azureFeedData.savillVideo) {
      hideElement(savillVideoEl);
      return;
    }

    var sv = azureFeedData.savillVideo;
    var svDate = "";
    if (sv.published) {
      var svd = parseDateValue(sv.published);
      svDate = formatLocalDate(svd, {
        day: "numeric",
        month: "short",
        year: "numeric"
      });
    }
    var thumbHtml = '<div class="savill-thumb-wrap' +
      (sv.thumbnail ? '' : ' thumb-fallback') +
      '">' +
      (sv.thumbnail
        ? '<img class="savill-thumb" src="' + escapeHtml(sv.thumbnail) +
          '" alt="Video thumbnail" loading="lazy" />'
        : '') +
      '<div class="savill-thumb-placeholder" aria-hidden="true">▶</div>' +
      '<span class="savill-play">▶</span></div>';
    savillVideoEl.innerHTML =
      '<a class="savill-card" href="' + escapeHtml(sv.url) +
      '" target="_blank" rel="noopener noreferrer">' +
      '<div class="savill-label">🎬 Latest Azure Update Video</div>' +
      '<div class="savill-body">' +
      thumbHtml +
      '<div class="savill-info">' +
      '<div class="savill-title">' + escapeHtml(sv.title) + '</div>' +
      (svDate ? '<div class="savill-date">' + escapeHtml(svDate) + '</div>' : '') +
      '</div></div></a>';
    showElement(savillVideoEl);
  }

  function renderM365VideoPanel() {
    if (!savillVideoEl) return;

    if (currentSource !== "m365" || !m365FeedData || !m365FeedData.m365Video) {
      hideElement(savillVideoEl);
      return;
    }

    var mv = m365FeedData.m365Video;
    var mvDate = "";
    if (mv.published) {
      var mvd = parseDateValue(mv.published);
      mvDate = formatLocalDate(mvd, {
        day: "numeric",
        month: "short",
        year: "numeric"
      });
    }

    var thumbHtml = '<div class="savill-thumb-wrap' +
      (mv.thumbnail ? '' : ' thumb-fallback') +
      '">' +
      (mv.thumbnail
        ? '<img class="savill-thumb" src="' + escapeHtml(mv.thumbnail) +
          '" alt="Video thumbnail" loading="lazy" />'
        : '') +
      '<div class="savill-thumb-placeholder" aria-hidden="true">▶</div>' +
      '<span class="savill-play">▶</span></div>';

    savillVideoEl.innerHTML =
      '<a class="savill-card" href="' + escapeHtml(mv.url) +
      '" target="_blank" rel="noopener noreferrer">' +
      '<div class="savill-label">🎬 Latest Microsoft 365 Update Video</div>' +
      '<div class="savill-body">' +
      thumbHtml +
      '<div class="savill-info">' +
      '<div class="savill-title">' + escapeHtml(mv.title) + '</div>' +
      (mvDate ? '<div class="savill-date">' + escapeHtml(mvDate) + '</div>' : '') +
      '</div></div></a>';
    showElement(savillVideoEl);
  }

  function updateOtherBlogsToggleVisibility() {
    if (!otherBlogsToggle) return;
    if (currentSource === "m365") {
      hideElement(otherBlogsToggle);
      return;
    }
    showElement(otherBlogsToggle);
  }

  function refreshSourcePanels() {
    updateOtherBlogsToggleVisibility();
    renderSummaryPanel();
    if (currentSource === "m365") {
      renderM365VideoPanel();
      return;
    }
    renderSavillVideoPanel();
  }

  function startOfLocalDay(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate());
  }

  function localDaysAgo(dayCount) {
    var date = startOfLocalDay(new Date());
    date.setDate(date.getDate() - dayCount);
    return date;
  }

  function startOfCurrentLocalWeek() {
    var today = startOfLocalDay(new Date());
    var dayOfWeek = today.getDay();
    // Use Monday as week start: Mon=0, Tue=1, ..., Sun=6.
    var mondayOffset = (dayOfWeek + 6) % 7;
    var weekStart = new Date(today);
    weekStart.setDate(today.getDate() - mondayOffset);
    return weekStart;
  }

  // ===== Initialize =====
  async function init() {
    loadTheme();
    updateHeaderOffset();
    registerServiceWorker();
    setupInfiniteScroll();
    await loadData();
    updateHeaderOffset();
    setupEventListeners();
  }

  // ===== Infinite Scroll =====
  function setupInfiniteScroll() {
    var sentinelEl = document.getElementById("load-more-sentinel");
    if (!sentinelEl || !window.IntersectionObserver) return;
    var observer = new IntersectionObserver(function (entries) {
      if (!entries[0].isIntersecting) return;
      if (renderedCount >= filteredArticles.length) return;
      renderedCount = Math.min(renderedCount + PAGE_SIZE, filteredArticles.length);
      showingCount.textContent =
        "Showing " + renderedCount + " of " + filteredArticles.length;
      renderArticles();
    }, { rootMargin: "200px" });
    observer.observe(sentinelEl);
  }

  // ===== Service Worker =====
  function registerServiceWorker() {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("sw.js").catch(function () {});
    }
  }

  // ===== Load Data =====
  async function loadData() {
    showLoading(true);
    try {
      // Load Azure feeds
      var azureResponse = await fetch("data/feeds.json");
      if (!azureResponse.ok) throw new Error("Failed to load Azure feeds");
      var azureData = await azureResponse.json();
      azureFeedData = azureData;
      var azureArticles = azureData.articles || [];
      
      // Mark Azure articles with source
      azureArticles.forEach(function (a) { a.source = "azure"; });
      
      // Try to load M365 data (graceful fallback if not available)
      var m365Articles = [];
      try {
        var m365Response = await fetch("data/m365_data.json");
        if (m365Response.ok) {
          var m365Data = await m365Response.json();
          m365FeedData = m365Data;
          var m365Cutoff = localDaysAgo(30);
          var m365MajorCutoff = localDaysAgo(90);
          m365Articles = (m365Data.articles || []).filter(function (article) {
            var articleDate = getArticleDate(article);
            if (!articleDate) return false;
            var cutoff = article.m365IsMajorChange ? m365MajorCutoff : m365Cutoff;
            return articleDate >= cutoff;
          });
          // Mark M365 articles with source
          m365Articles.forEach(function (a) { a.source = "m365"; });

          // Auto-bookmark major changes for all visitors.
          var bookmarksChanged = false;
          m365Articles.forEach(function (a) {
            if (a.m365IsMajorChange && a.link && !bookmarks.has(a.link)) {
              bookmarks.add(a.link);
              bookmarksChanged = true;
            }
          });
          if (bookmarksChanged) {
            localStorage.setItem(
              "cloudplatformfeed-bookmarks",
              JSON.stringify(Array.from(bookmarks))
            );
          }

          // Populate productCategory using a stable source+id key.
          var byCategory = m365Data.byCategory || {};
          var categoryBySourceId = {};
          Object.keys(byCategory).forEach(function (catName) {
            var catArticles = byCategory[catName] || [];
            catArticles.forEach(function (catArticle) {
              var key = String((catArticle.m365Source || "") + ":" + (catArticle.m365Id || ""));
              if (key !== ":") {
                categoryBySourceId[key] = catName;
              }
            });
          });

          m365Articles.forEach(function (article) {
            var key = String((article.m365Source || "") + ":" + (article.m365Id || ""));
            article.productCategory =
              categoryBySourceId[key] ||
              article.m365Category ||
              "Uncategorised";
          });
        }
      } catch (e) {
        // M365 data is optional - graceful degradation if unavailable
        console.log("M365 data not available (optional feature)");
      }
      
      // Combine both sources
      articles = azureArticles.concat(m365Articles);

      // Assign colors to blogs/services
      var blogs = [];
      var seen = {};
      articles.forEach(function (a) {
        var id = a.source === "m365" ? (a.m365Service || "m365") : a.blogId;
        if (!seen[id]) {
          seen[id] = true;
          blogs.push(id);
        }
      });
      blogs.forEach(function (blogId, i) {
        var colorIndex = i % colorPalette.length;
        blogColors[blogId] = colorPalette[colorIndex];
        blogColorClasses[blogId] = "blog-color-" + colorIndex;
      });

      // Update header stats
      var lastDate = azureData.lastUpdated;
      if (lastDate) {
        var date = parseDateValue(lastDate);
        lastUpdated.textContent = "Last updated: " + formatLocalDateTime(date);
      }
      totalCount.textContent = articles.length + " articles";

      updateOtherBlogsToggleUI();

      renderFilters();
      refreshSourcePanels();
      applyFilters();
    } catch (err) {
      console.error("Error loading feeds:", err);
      articlesGrid.innerHTML =
        '<div class="empty-state empty-state-full">' +
        '<p class="empty-state-title">📡 No feed data available yet</p>' +
        "<p>Run the GitHub Action to fetch the latest articles, or check back later.</p>" +
        "</div>";
    }
    showLoading(false);
  }

  function getVisibleArticles() {
    // For M365, return only M365 articles (no blogId filtering needed)
    if (currentSource === "m365") {
      return articles.filter(function (a) { return a.source === "m365"; });
    }

    // For Azure, apply the showOtherBlogs logic
    var azureArticles = articles.filter(function (a) { return (a.source || "azure") === "azure"; });
    if (showOtherBlogs) {
      return azureArticles;
    }
    return azureArticles.filter(function (a) {
      return a.blogId === AZURE_UPDATES_BLOG_ID;
    });
  }

  function isAzureLifecyclePillMode() {
    return currentSource === "azure" && !showOtherBlogs;
  }

  function deriveAzureLifecycleKey(article) {
    if (!article || article.blogId !== AZURE_UPDATES_BLOG_ID) {
      return "unknown";
    }

    var lifecycle = String(article.lifecycle || "").toLowerCase().trim();
    if (lifecycle === "launched_ga" || lifecycle === "in_preview" || lifecycle === "in_development" || lifecycle === "retiring") {
      return lifecycle;
    }

    var status = String(article.azureStatus || "").toLowerCase();
    if (/retir|deprecat|sunset|end of support/.test(status)) return "retiring";
    if (/in development|coming soon|develop/.test(status)) return "in_development";
    if (/preview/.test(status)) return "in_preview";
    if (/launch|generally available|\bga\b|available/.test(status)) return "launched_ga";
    return "unknown";
  }

  function updateOtherBlogsToggleUI() {
    if (!otherBlogsToggle) return;
    otherBlogsToggle.classList.toggle("active", showOtherBlogs);
    otherBlogsToggle.textContent = showOtherBlogs
      ? "📰 Other Blogs On"
      : "📰 Other Blogs Off";
    otherBlogsToggle.title = showOtherBlogs
      ? "Hide non-Updates blogs"
      : "Show non-Updates blogs";
  }

  function syncActiveCategoryPill() {
    var categoryButtons = filterPills.querySelectorAll(".category-pill");
    categoryButtons.forEach(function (p) {
      p.classList.remove("active");
    });

    var activeCategoryButton = filterPills.querySelector(
      '.category-pill[data-category="' + currentCategory + '"]'
    );

    if (!activeCategoryButton) {
      currentCategory = "all";
      activeCategoryButton = filterPills.querySelector(
        '.category-pill[data-category="all"]'
      );
    }

    if (activeCategoryButton) {
      activeCategoryButton.classList.add("active");
    }
  }

  // ===== Render Filter Pills (with category grouping) =====
  function renderFilters() {
    var sourceArticles = getVisibleArticles();
    
    if (currentSource === "m365") {
      renderFiltersM365(sourceArticles);
    } else {
      renderFiltersAzure(sourceArticles);
    }
  }

  function renderFiltersAzure(sourceArticles) {
    if (isAzureLifecyclePillMode()) {
      currentFilter = "all";
      renderFiltersAzureLifecycle(sourceArticles);
      return;
    }

    var blogCounts = {};
    var azureUpdatesCategoryCounts = {};
    sourceArticles.forEach(function (a) {
      if (!blogCounts[a.blogId]) {
        blogCounts[a.blogId] = { name: a.blog, count: 0 };
      }
      blogCounts[a.blogId].count++;

      if (a.blogId === AZURE_UPDATES_BLOG_ID) {
        Object.keys(CATEGORIES).forEach(function (catName) {
          if (articleMatchesCategory(a, catName)) {
            azureUpdatesCategoryCounts[catName] = (azureUpdatesCategoryCounts[catName] || 0) + 1;
          }
        });
      }
    });

    var fragment = document.createDocumentFragment();
    var categoryBar = document.createElement("div");
    categoryBar.className = "category-bar";
    categoryBar.id = "category-bar";

    function createCategoryPill(categoryValue, label, count) {
      var button = document.createElement("button");
      button.className = "category-pill";
      button.dataset.category = categoryValue;
      button.appendChild(document.createTextNode(label + " "));

      var countEl = document.createElement("span");
      countEl.className = "count";
      countEl.textContent = String(count);
      button.appendChild(countEl);

      return button;
    }

    var allPill = createCategoryPill("all", "All", sourceArticles.length);
    allPill.classList.add("active");
    categoryBar.appendChild(allPill);

    Object.keys(CATEGORIES).forEach(function (catName) {
      var catBlogs = CATEGORIES[catName];
      var catCount = 0;
      catBlogs.forEach(function (blogId) {
        if (blogCounts[blogId]) catCount += blogCounts[blogId].count;
      });
      catCount += azureUpdatesCategoryCounts[catName] || 0;
      if (catCount > 0) {
        categoryBar.appendChild(createCategoryPill(catName, catName, catCount));
      }
    });
    fragment.appendChild(categoryBar);

    // Blog pills (shown below categories)
    var blogPillsRow = document.createElement("div");
    blogPillsRow.className = "blog-pills-row is-hidden";
    blogPillsRow.id = "blog-pills-row";

    var blogFilterPills = document.createElement("div");
    blogFilterPills.className = "filter-pills";
    blogFilterPills.id = "blog-filter-pills";
    blogPillsRow.appendChild(blogFilterPills);
    fragment.appendChild(blogPillsRow);

    filterPills.replaceChildren(fragment);
    syncActiveCategoryPill();
  }

  function renderFiltersAzureLifecycle(sourceArticles) {
    var lifecycleCounts = {};
    AZURE_LIFECYCLE_FILTER_ORDER.forEach(function (key) {
      lifecycleCounts[key] = 0;
    });

    sourceArticles.forEach(function (a) {
      var key = deriveAzureLifecycleKey(a);
      if (!lifecycleCounts[key]) lifecycleCounts[key] = 0;
      lifecycleCounts[key]++;
    });

    var fragment = document.createDocumentFragment();
    var categoryBar = document.createElement("div");
    categoryBar.className = "category-bar";
    categoryBar.id = "category-bar";

    function createCategoryPill(categoryValue, label, count) {
      var button = document.createElement("button");
      button.className = "category-pill";
      button.dataset.category = categoryValue;
      button.appendChild(document.createTextNode(label + " "));

      var countEl = document.createElement("span");
      countEl.className = "count";
      countEl.textContent = String(count);
      button.appendChild(countEl);

      return button;
    }

    var allPill = createCategoryPill("all", "All", sourceArticles.length);
    allPill.classList.add("active");
    categoryBar.appendChild(allPill);

    AZURE_LIFECYCLE_FILTER_ORDER.forEach(function (key) {
      var count = lifecycleCounts[key] || 0;
      if (!count) return;
      categoryBar.appendChild(createCategoryPill(key, AZURE_LIFECYCLE_FILTER_LABELS[key], count));
    });

    fragment.appendChild(categoryBar);
    filterPills.replaceChildren(fragment);
    syncActiveCategoryPill();
  }

  function renderFiltersM365(sourceArticles) {
    var m365CategoryCounts = {};
    
    sourceArticles.forEach(function (a) {
      var category = a.productCategory || "Uncategorised";
      m365CategoryCounts[category] = (m365CategoryCounts[category] || 0) + 1;
    });

    var fragment = document.createDocumentFragment();
    var categoryBar = document.createElement("div");
    categoryBar.className = "category-bar";
    categoryBar.id = "category-bar";

    function createCategoryPill(categoryValue, label, count) {
      var button = document.createElement("button");
      button.className = "category-pill";
      button.dataset.category = categoryValue;
      button.appendChild(document.createTextNode(label + " "));

      var countEl = document.createElement("span");
      countEl.className = "count";
      countEl.textContent = String(count);
      button.appendChild(countEl);

      return button;
    }

    var allPill = createCategoryPill("all", "All", sourceArticles.length);
    allPill.classList.add("active");
    categoryBar.appendChild(allPill);

    Object.keys(m365CategoryCounts).sort().forEach(function (catName) {
      var catCount = m365CategoryCounts[catName];
      if (catCount > 0) {
        categoryBar.appendChild(createCategoryPill(catName, catName, catCount));
      }
    });

    fragment.appendChild(categoryBar);
    filterPills.replaceChildren(fragment);
    syncActiveCategoryPill();
  }

  function renderBlogPills(categoryName) {
    var blogPillsRow = document.getElementById("blog-pills-row");
    var blogFilterPillsEl = document.getElementById("blog-filter-pills");

    if (currentSource === "m365" || categoryName === "all" || isAzureLifecyclePillMode()) {
      hideElement(blogPillsRow);
      return;
    }

    if (!blogFilterPillsEl || !blogPillsRow) return;

    var blogCounts = {};
    var azureUpdatesCount = 0;
    getVisibleArticles().forEach(function (a) {
      if (!blogCounts[a.blogId]) {
        blogCounts[a.blogId] = { name: a.blog, count: 0 };
      }
      blogCounts[a.blogId].count++;

      if (a.blogId === AZURE_UPDATES_BLOG_ID && articleMatchesCategory(a, categoryName)) {
        azureUpdatesCount++;
      }
    });

    var catBlogs = CATEGORIES[categoryName] || [];
    var html = '<button class="pill active" data-filter="all">All in ' +
      escapeHtml(categoryName) + "</button>";
    catBlogs.forEach(function (blogId) {
      if (blogCounts[blogId]) {
        html +=
          '<button class="pill" data-filter="' + blogId + '">' +
          escapeHtml(blogCounts[blogId].name) +
          ' <span class="count">' + blogCounts[blogId].count + "</span></button>";
      }
    });

    if (
      categoryName !== "Operations" &&
      azureUpdatesCount > 0 &&
      blogCounts[AZURE_UPDATES_BLOG_ID]
    ) {
      html +=
        '<button class="pill" data-filter="' + AZURE_UPDATES_BLOG_ID + '">' +
        escapeHtml(blogCounts[AZURE_UPDATES_BLOG_ID].name) +
        ' <span class="count">' + azureUpdatesCount + "</span></button>";
    }

    blogFilterPillsEl.innerHTML = html;
    showElement(blogPillsRow);
  }

  // ===== Apply Filters & Sort =====
  function applyFilters() {
    var visibleArticles = getVisibleArticles();
    var result = visibleArticles.slice();

    // Source filter (Azure vs M365)
    result = result.filter(function (a) {
      return (a.source || "azure") === currentSource;
    });

    // Category filter
    if (currentCategory !== "all") {
      result = result.filter(function (a) {
        return articleMatchesCategory(a, currentCategory);
      });
    }

    // Blog filter (within category) - only applies to Azure articles
    if (currentFilter !== "all" && currentSource === "azure") {
      result = result.filter(function (a) { return a.blogId === currentFilter; });
    }

    // Search filter
    if (searchQuery) {
      var q = searchQuery.toLowerCase();
      result = result.filter(function (a) {
        return (
          a.title.toLowerCase().includes(q) ||
          (a.summary || "").toLowerCase().includes(q) ||
          (a.blog || a.m365Service || "").toLowerCase().includes(q) ||
          (a.author || "").toLowerCase().includes(q)
        );
      });
    }

    // Date filter
    var dateVal = dateFilter ? dateFilter.value : "all";
    if (dateVal !== "all") {
      var now = new Date();
      var cutoff = startOfLocalDay(now);
      switch (dateVal) {
        case "today": break;
        case "week": cutoff = startOfCurrentLocalWeek(); break;
        case "month": cutoff = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate()); break;
      }
      result = result.filter(function (a) {
        var published = getArticleDate(a);
        return published && published >= cutoff;
      });
    }

    // Bookmarks filter
    if (showBookmarksOnly) {
      result = result.filter(function (a) { return bookmarks.has(a.link); });
    }

    // Sort
    switch (sortBy) {
      case "date-desc":
        result.sort(function (a, b) {
          return (getArticleDate(b) || 0) - (getArticleDate(a) || 0);
        });
        break;
      case "date-asc":
        result.sort(function (a, b) {
          return (getArticleDate(a) || 0) - (getArticleDate(b) || 0);
        });
        break;
      case "blog":
        result.sort(function (a, b) {
          var aBlog = a.blog || a.m365Service || "";
          var bBlog = b.blog || b.m365Service || "";
          return aBlog.localeCompare(bBlog) || ((getArticleDate(b) || 0) - (getArticleDate(a) || 0));
        });
        break;
    }

    filteredArticles = result;
    renderedCount = Math.min(PAGE_SIZE, result.length);
    showingCount.textContent =
      "Showing " + renderedCount + " of " + result.length;
    renderArticles();
  }

  // ===== Render Articles =====
  function renderArticles() {
    var sentinelEl = document.getElementById("load-more-sentinel");
    if (filteredArticles.length === 0) {
      articlesGrid.innerHTML = "";
      noResultsEl.classList.add("visible");
      hideElement(sentinelEl);
      return;
    }
    noResultsEl.classList.remove("visible");

    var toRender = filteredArticles.slice(0, renderedCount);
    var groups = groupByDate(toRender);
    var html = "";
    for (var groupName in groups) {
      if (!groups.hasOwnProperty(groupName)) continue;
      html +=
        '<div class="date-group-header">📅 ' +
        escapeHtml(groupName) +
        "</div>";
      groups[groupName].forEach(function (article) {
        html += renderCard(article);
      });
    }

    articlesGrid.innerHTML = html;

    if (renderedCount < filteredArticles.length) {
      showElement(sentinelEl);
    } else {
      hideElement(sentinelEl);
    }
  }

  function articleMatchesCategory(article, categoryName) {
    // Handle M365 articles (use productCategory field)
    if ((article.source || "azure") === "m365") {
      var productCategory = article.productCategory || article.m365Category || "Uncategorised";
      return productCategory === categoryName;
    }

    if (isAzureLifecyclePillMode()) {
      return deriveAzureLifecycleKey(article) === categoryName;
    }

    // Handle Azure articles (original logic)
    var catBlogs = CATEGORIES[categoryName] || [];
    if (catBlogs.indexOf(article.blogId) !== -1) {
      return true;
    }

    if (article.blogId !== AZURE_UPDATES_BLOG_ID) {
      return false;
    }

    var text = (article.title + " " + article.summary).toLowerCase();
    var keywords = AZURE_UPDATES_CATEGORY_KEYWORDS[categoryName] || [];

    return keywords.some(function (keyword) {
      return text.indexOf(keyword) !== -1;
    });
  }

  // ===== Group by Date =====
  function groupByDate(list) {
    var groups = {
      "Today": [],
      "Yesterday": [],
      "This Week": []
    };
    var monthGroups = {};
    var unknown = [];
    var today = startOfLocalDay(new Date());
    var yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    var weekStart = startOfCurrentLocalWeek();

    list.forEach(function (article) {
      var date = getArticleDate(article);
      if (!date) {
        unknown.push(article);
      } else if (date >= today) {
        groups["Today"].push(article);
      } else if (date >= yesterday) {
        groups["Yesterday"].push(article);
      } else if (date >= weekStart) {
        groups["This Week"].push(article);
      } else {
        var monthKey = formatLocalDate(date, {
          year: "numeric",
          month: "long"
        });
        if (!monthGroups[monthKey]) monthGroups[monthKey] = [];
        monthGroups[monthKey].push(article);
      }
    });

    var ordered = {};
    ["Today", "Yesterday", "This Week"].forEach(function (key) {
      if (groups[key].length) ordered[key] = groups[key];
    });

    Object.keys(monthGroups).sort(function (a, b) {
      return new Date(b) - new Date(a);
    }).forEach(function (key) {
      ordered[key] = monthGroups[key];
    });

    if (unknown.length) {
      ordered["Unknown Date"] = unknown;
    }

    return ordered;
  }

  // ===== Check if article is new (last 24h) =====
  function isNew(article) {
    var now = new Date();
    var published = getArticleDate(article);
    if (!published) return false;
    return (now - published) < 24 * 60 * 60 * 1000;
  }

  // ===== Render Single Card =====
  function renderCard(article) {
    var isM365 = (article.source || "azure") === "m365";
    var color = isM365 ? "#BD8D32" : (blogColors[article.blogId] || "#BD8D32");
    var colorClass = isM365 ? "blog-color-0" : (blogColorClasses[article.blogId] || "blog-color-0");
    var isBookmarked = bookmarks.has(article.link);
    var date = getArticleDate(article);
    var dateStr = formatLocalDate(date, {
      month: "short",
      day: "numeric",
      year: "numeric"
    });
    var encodedLink = encodeURIComponent(article.link);
    var newBadge = isNew(article) ? '<span class="new-badge">NEW</span>' : "";

    var shareUrl = encodeURIComponent(article.link);
    var shareTitle = encodeURIComponent(article.title);

    // For M365 articles, use service name as blog tag, lifecycle as meta, m365Source for source label
    var blogTagText = isM365 ? (article.m365Service || "Microsoft 365") : article.blog;
    var metaContent = isM365 
      ? ("<span>📌 " + escapeHtml(article.m365Source || "message_center") + " · " + escapeHtml(article.lifecycle || "") + "</span>" +
         "<span>📅 " + dateStr + "</span>")
      : ("<span>✍️ " + escapeHtml(article.author) + "</span>" +
         "<span>📅 " + dateStr + "</span>");
    var summary = isM365
      ? String(article.summary || "").trim()
      : (article.summary || "No additional information available.");

    var cardTagsHtml = "";
    if (isM365) {
      var m365Status = String(article.m365Status || "").trim();
      var releasePhase = LIFECYCLE_LABELS[article.lifecycle] || "";
      var previewTarget = formatM365TargetDate(article.m365PreviewDate);
      var gaTarget = formatM365TargetDate(article.m365GeneralAvailabilityDate);
      var expectedReleasePills = buildM365TargetDatePills(article.m365TargetDate);
      var tags = [];

      if (m365Status) {
        tags.push(
          '<span class="m365-tag"><span class="m365-tag-label">Status:</span> ' +
          escapeHtml(m365Status) +
          "</span>"
        );
      }
      if (releasePhase) {
        tags.push(
          '<span class="m365-tag"><span class="m365-tag-label">Release Phase:</span> ' +
          escapeHtml(releasePhase) +
          "</span>"
        );
      }
      if (previewTarget) {
        tags.push(
          '<span class="m365-tag"><span class="m365-tag-label">Preview:</span> ' +
          escapeHtml(previewTarget) +
          "</span>"
        );
      }
      if (gaTarget) {
        tags.push(
          '<span class="m365-tag"><span class="m365-tag-label">GA:</span> ' +
          escapeHtml(gaTarget) +
          "</span>"
        );
      }
      if (!previewTarget && !gaTarget) {
        expectedReleasePills.forEach(function (pill) {
          tags.push(
            '<span class="m365-tag"><span class="m365-tag-label">' +
            escapeHtml(pill.label) +
            ":</span> " +
            escapeHtml(pill.value) +
            "</span>"
          );
        });
      }
      if (article.m365IsMajorChange) {
        tags.push(
          '<span class="m365-tag major-change"><span class="m365-tag-label">Major change</span></span>'
        );
      }

      if (tags.length) {
        cardTagsHtml = '<div class="m365-tags">' + tags.join("") + "</div>";
      }
    } else if (article.blogId === AZURE_UPDATES_BLOG_ID) {
      var azureReleasePhase = LIFECYCLE_LABELS[article.lifecycle] || String(article.azureStatus || "").trim();
      var previewTarget = formatM365TargetDate(article.azurePreviewDate);
      var gaTarget = formatM365TargetDate(article.azureGeneralAvailabilityDate);
      var fallbackTarget = formatM365TargetDate(article.azureTargetDate);
      var retirementTarget = formatUkRetirementDate(article.azureRetirementDate);
      var isRetiring = String(article.lifecycle || "").toLowerCase().trim() === "retiring";
      var azureTags = [];

      if (azureReleasePhase) {
        azureTags.push(
          '<span class="m365-tag"><span class="m365-tag-label">Release Phase:</span> ' +
          escapeHtml(azureReleasePhase) +
          "</span>"
        );
      }
      if (previewTarget) {
        azureTags.push(
          '<span class="m365-tag"><span class="m365-tag-label">Preview:</span> ' +
          escapeHtml(previewTarget) +
          "</span>"
        );
      }
      if (gaTarget) {
        azureTags.push(
          '<span class="m365-tag"><span class="m365-tag-label">GA:</span> ' +
          escapeHtml(gaTarget) +
          "</span>"
        );
      }
      if (isRetiring && retirementTarget) {
        azureTags.push(
          '<span class="m365-tag"><span class="m365-tag-label">Retires On:</span> ' +
          escapeHtml(retirementTarget) +
          "</span>"
        );
      }
      if (!previewTarget && !gaTarget && fallbackTarget) {
        azureTags.push(
          '<span class="m365-tag"><span class="m365-tag-label">Expected Release:</span> ' +
          escapeHtml(fallbackTarget) +
          "</span>"
        );
      }

      if (azureTags.length) {
        cardTagsHtml = '<div class="m365-tags">' + azureTags.join("") + "</div>";
      }
    }

    var summaryHtml = summary
      ? ('<p class="article-summary">' + escapeHtml(summary) + "</p>")
      : "";

    return (
      '<article class="article-card clickable-card" data-href="' + escapeHtml(resolveArticleOutboundLink(article)) + '">' +
      '<div class="card-header">' +
      '<span class="blog-tag ' + colorClass + '" title="' + escapeHtml(color) + '">' +
      escapeHtml(blogTagText) + "</span>" +
      '<button class="bookmark-btn ' + (isBookmarked ? "bookmarked" : "") +
      '" data-action="bookmark" data-link="' + encodedLink +
      '" title="' + (isBookmarked ? "Remove bookmark" : "Bookmark this article") + '">' +
      (isBookmarked ? "⭐" : "☆") + "</button>" +
      "</div>" +
      '<h3 class="article-title">' +
      '<a href="' + escapeHtml(resolveArticleOutboundLink(article)) + '" target="_blank" rel="noopener">' +
      escapeHtml(article.title) + "</a>" + newBadge +
      "</h3>" +
      '<div class="article-meta">' +
      metaContent +
      "</div>" +
      cardTagsHtml +
      summaryHtml +
      '<div class="share-buttons">' +
      "</div>" +
      "</article>"
    );
  }

  // ===== Toggle Bookmark =====
  function toggleBookmark(link) {
    if (bookmarks.has(link)) {
      bookmarks.delete(link);
      showToast("Bookmark removed");
    } else {
      bookmarks.add(link);
      showToast("⭐ Article bookmarked!");
    }
    localStorage.setItem(
      "cloudplatformfeed-bookmarks",
      JSON.stringify(Array.from(bookmarks))
    );
    applyFilters();
  }

  // ===== Find article by encoded link =====
  function findArticleByEncodedLink(encodedLink) {
    var link = decodeURIComponent(encodedLink);
    return articles.find(function (a) {
      return a.link === link;
    });
  }

  // ===== Toast =====
  var toastTimeout;
  function showToast(message) {
    clearTimeout(toastTimeout);
    toastEl.textContent = message;
    toastEl.classList.add("visible");
    toastTimeout = setTimeout(function () {
      toastEl.classList.remove("visible");
    }, 3000);
  }

  // ===== Loading =====
  function showLoading(show) {
    loadingEl.classList.toggle("visible", show);
  }

  function updateHeaderOffset() {
    if (!headerEl) return;
    document.documentElement.style.setProperty(
      "--header-height",
      headerEl.offsetHeight + "px"
    );
    document.documentElement.style.setProperty(
      "--tabs-height",
      ((tabsContainerEl && tabsContainerEl.offsetHeight) || 0) + "px"
    );
  }

  // ===== Theme =====
  function loadTheme() {
    var saved = localStorage.getItem("cloudplatformfeed-theme") || "light";
    document.documentElement.setAttribute("data-theme", saved);
    themeToggle.textContent = saved === "dark" ? "☀️" : "🌙";
  }

  function toggleTheme() {
    var current = document.documentElement.getAttribute("data-theme");
    var next = current === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    localStorage.setItem("cloudplatformfeed-theme", next);
    themeToggle.textContent = next === "dark" ? "☀️" : "🌙";
  }

  // ===== Escape Helpers =====
  var escapeDiv = document.createElement("div");
  function escapeHtml(str) {
    if (!str) return "";
    escapeDiv.textContent = str;
    return escapeDiv.innerHTML;
  }

  // ===== Event Listeners =====
  function setupEventListeners() {
    // Search with debounce
    var searchTimeout;
    searchInput.addEventListener("input", function (e) {
      clearTimeout(searchTimeout);
      searchTimeout = setTimeout(function () {
        searchQuery = e.target.value.trim();
        applyFilters();
      }, 250);
    });

    // Sort
    sortSelect.addEventListener("change", function (e) {
      sortBy = e.target.value;
      applyFilters();
    });

    // Date filter
    dateFilter.addEventListener("change", function () {
      applyFilters();
    });

    // Theme toggle
    themeToggle.addEventListener("click", toggleTheme);

    // Tab buttons for source switching (Azure vs M365)
    tabButtons.forEach(function (btn) {
      btn.addEventListener("click", function () {
        currentSource = btn.dataset.source;
        
        // Update active state on tab buttons
        tabButtons.forEach(function (b) {
          b.classList.remove("active");
        });
        btn.classList.add("active");
        
        // Update header subtitle
        if (subtitleEl) {
          if (currentSource === "m365") {
            subtitleEl.textContent = "Daily updates from Microsoft 365 · Last 30 days";
          } else {
            subtitleEl.textContent = "Daily updates from Azure · Last 30 days";
          }
        }
        
        // Reset search and category filters when switching sources
        searchInput.value = "";
        searchQuery = "";
        currentCategory = "all";
        currentFilter = "all";
        
        // Re-render filters and articles for the new source
        renderFilters();
        renderBlogPills(currentCategory);
        refreshSourcePanels();
        applyFilters();
      });
    });

    // Category and blog pills (event delegation)
    filterPills.addEventListener("click", function (e) {
      // Category pill click
      var catPill = e.target.closest(".category-pill");
      if (catPill) {
        filterPills.querySelectorAll(".category-pill").forEach(function (p) {
          p.classList.remove("active");
        });
        catPill.classList.add("active");
        currentCategory = catPill.dataset.category;
        currentFilter = "all";
        renderBlogPills(currentCategory);
        applyFilters();
        return;
      }

      // Blog pill click
      var pill = e.target.closest(".pill");
      if (pill) {
        var blogPillsContainer = document.getElementById("blog-filter-pills");
        if (blogPillsContainer) {
          blogPillsContainer.querySelectorAll(".pill").forEach(function (p) {
            p.classList.remove("active");
          });
        }
        pill.classList.add("active");
        currentFilter = pill.dataset.filter;
        applyFilters();
      }
    });

    // Bookmarks toggle
    bookmarksToggle.addEventListener("click", function () {
      showBookmarksOnly = !showBookmarksOnly;
      bookmarksToggle.classList.toggle("active", showBookmarksOnly);
      bookmarksToggle.textContent = showBookmarksOnly
        ? "⭐ Showing Bookmarks"
        : "⭐ Bookmarks";
      applyFilters();
    });

    // Other blogs toggle
    if (otherBlogsToggle) {
      otherBlogsToggle.addEventListener("click", function () {
        var wasLifecycleMode = isAzureLifecyclePillMode();
        showOtherBlogs = !showOtherBlogs;
        localStorage.setItem("cloudplatformfeed-other-blogs", String(showOtherBlogs));
        currentFilter = "all";
        if (wasLifecycleMode !== isAzureLifecyclePillMode()) {
          currentCategory = "all";
        }
        updateOtherBlogsToggleUI();
        renderFilters();
        renderBlogPills(currentCategory);
        applyFilters();
      });
    }

    if (savillVideoEl) {
      savillVideoEl.addEventListener("error", function (e) {
        var target = e.target;
        if (target && target.classList && target.classList.contains("savill-thumb")) {
          var thumbWrap = target.closest(".savill-thumb-wrap");
          if (thumbWrap) {
            thumbWrap.classList.add("thumb-fallback");
          }
        }
      }, true);
    }

    // Article actions (event delegation on grid)
    articlesGrid.addEventListener("click", function (e) {
      var btn = e.target.closest("[data-action]");
      if (!btn) return;

      var encodedLink = btn.dataset.link;
      var article = findArticleByEncodedLink(encodedLink);
      if (!article) return;

      if (btn.dataset.action === "bookmark") {
        toggleBookmark(article.link);
      }
    });

    // Whole-card click for M365 articles (opens DeltaPulse)
    articlesGrid.addEventListener("click", function (e) {
      var card = e.target.closest(".clickable-card");
      if (!card) return;
      // Ignore clicks on interactive elements inside the card
      if (e.target.closest("a, button")) return;
      var href = card.dataset.href;
      if (href) window.open(href, "_blank", "noopener");
    });

    // Keyboard shortcut: Ctrl/Cmd + K to focus search
    document.addEventListener("keydown", function (e) {
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        searchInput.focus();
      }
    });

    window.addEventListener("resize", updateHeaderOffset);
  }

  // ===== Start =====
  document.addEventListener("DOMContentLoaded", init);
})();
