const CACHE_NAME = "cloudplatformfeed-v3";
const STATIC_ASSETS = [
  "/",
  "/index.html",
  "/css/styles.css",
  "/js/app.js",
  "/manifest.json",
  "/icons/atech-192.png",
  "/icons/atech-512.png",
];

const FEED_PATH_TOKENS = [
  "feeds.json",
  "feed.xml",
  "m365_data.json",
  "retirements.json",
  "checksums.json",
  "m365_checksums.json",
];
const ICON_EXTENSIONS = [".ico", ".png", ".svg"];

function shouldCache(response) {
  return response && response.ok;
}

function isSafeRequestUrl(request) {
  const url = new URL(request.url);
  const isHttp = url.protocol === "http:" || url.protocol === "https:";
  return isHttp && url.origin === self.location.origin;
}

function isFeedDataPath(pathname) {
  return (
    FEED_PATH_TOKENS.some((token) => pathname.includes(token)) ||
    pathname.endsWith(".ics")
  );
}

function isIconAssetPath(pathname) {
  return (
    pathname.startsWith("/icons/") ||
    ICON_EXTENSIONS.some((extension) => pathname.endsWith(extension))
  );
}

function isAppShellRequest(pathname, mode) {
  return STATIC_ASSETS.includes(pathname) || mode === "navigate";
}

function putInCache(request, response) {
  if (!shouldCache(response)) {
    return Promise.resolve(response);
  }

  const clone = response.clone();
  return caches.open(CACHE_NAME).then((cache) => {
    cache.put(request, clone);
    return response;
  });
}

function networkFirst(request) {
  if (!isSafeRequestUrl(request)) {
    return Promise.resolve(new Response("Blocked", { status: 400 }));
  }

  return fetch(request)
    .then((response) => putInCache(request, response))
    .catch(() => caches.match(request));
}

function cacheFirst(request) {
  if (!isSafeRequestUrl(request)) {
    return Promise.resolve(new Response("Blocked", { status: 400 }));
  }

  return caches.match(request).then((cached) => {
    if (cached) {
      return cached;
    }

    return fetch(request).then((response) => putInCache(request, response));
  });
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key !== CACHE_NAME)
          .map((key) => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return;
  }

  const url = new URL(event.request.url);
  const isSameOrigin = url.origin === self.location.origin;
  const useNetworkFirst =
    (isSameOrigin && isFeedDataPath(url.pathname)) ||
    (isSameOrigin &&
      (isAppShellRequest(url.pathname, event.request.mode) ||
        isIconAssetPath(url.pathname)));

  // Network-first for feed data, app shell, and icon assets.
  if (useNetworkFirst) {
    event.respondWith(networkFirst(event.request));
    return;
  }

  // Cache-first for other same-origin static assets.
  if (isSameOrigin) {
    event.respondWith(cacheFirst(event.request));
  }
});
