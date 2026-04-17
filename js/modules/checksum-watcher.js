(function () {
  "use strict";

  var DEFAULT_URL = "data/checksums.json";
  var DEFAULT_POLL_INTERVAL_MS = 5 * 60 * 1000;

  function noop() {}

  function defaultToken(payload) {
    if (!payload) return "";
    return String(payload.generatedAt || "");
  }

  function normalizeOptions(options) {
    var config = options || {};
    return {
      url: config.url ? config.url : DEFAULT_URL,
      pollIntervalMs: config.pollIntervalMs ? config.pollIntervalMs : DEFAULT_POLL_INTERVAL_MS,
      onChange: config.onChange ? config.onChange : noop
    };
  }

  function create(options) {
    var config = normalizeOptions(options);
    var timerId = null;
    var baseline = "";

    async function fetchToken() {
      var response = await fetch(config.url, { cache: "no-store" });
      if (!response.ok) throw new Error("Checksum request failed");
      var payload = await response.json();
      return defaultToken(payload);
    }

    async function check() {
      try {
        var token = await fetchToken();
        if (!token) return;
        if (!baseline) {
          baseline = token;
          return;
        }
        if (token === baseline) return;
        baseline = token;
        config.onChange(token);
      } catch (e) {
        // Silent network failure; next cycle retries.
      }
    }

    function start() {
      if (timerId) return;
      check();
      timerId = window.setInterval(check, config.pollIntervalMs);
    }

    function stop() {
      if (!timerId) return;
      window.clearInterval(timerId);
      timerId = null;
    }

    return {
      start: start,
      stop: stop
    };
  }

  window.CPFeedChecksumWatcher = { create: create };
})();
