const CACHE_NAME = "cloudplatformfeed-v2";
const STATIC_ASSETS = [
  "/",
  "/index.html",
  "/css/styles.css",
  "/js/clarity.js",
  "/js/app.js",
  "/manifest.json",
  "/icons/atech-192.png",
  "/icons/atech-512.png",
];

function shouldCache(response) {
  return response && response.ok;
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
  return fetch(request)
    .then((response) => putInCache(request, response))
    .catch(() => caches.match(request));
}

function cacheFirst(request) {
  return caches.match(request).then((cached) => {
    if (cached) {
      return cached;
    }

    return fetch(request).then((response) => {
      if (response.type === "opaque") {
        return response;
      }

      return putInCache(request, response);
    });
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

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return;
  }

  var url = new URL(event.request.url);
  var isSameOrigin = url.origin === self.location.origin;
  var isFeedData =
    url.pathname.includes("feeds.json") ||
    url.pathname.includes("feed.xml") ||
    url.pathname.includes("m365_data.json");
  var isIconAsset =
    url.pathname.startsWith("/icons/") ||
    url.pathname.endsWith(".ico") ||
    url.pathname.endsWith(".png") ||
    url.pathname.endsWith(".svg");
  var isAppShell = STATIC_ASSETS.includes(url.pathname) || event.request.mode === "navigate";

  // Network-first for feed data (always get fresh data)
  if (isFeedData) {
    event.respondWith(networkFirst(event.request));
    return;
  }

  // Network-first for the app shell so new deploys are picked up quickly.
  if (isSameOrigin && isAppShell) {
    event.respondWith(networkFirst(event.request));
    return;
  }

  // Network-first for icons so branding updates are visible without hard refresh.
  if (isSameOrigin && isIconAsset) {
    event.respondWith(networkFirst(event.request));
    return;
  }

  // Cache-first for other same-origin static assets.
  if (isSameOrigin) {
    event.respondWith(cacheFirst(event.request));
  }
});
