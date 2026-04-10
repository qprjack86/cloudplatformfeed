(function () {
  "use strict";

  function defaultToken(payload) {
    if (!payload) return "";
    return String(payload.generatedAt || "");
  }

  function create(options) {
    var url = options && options.url ? options.url : "data/checksums.json";
    var pollIntervalMs = options && options.pollIntervalMs ? options.pollIntervalMs : 5 * 60 * 1000;
    var onChange = options && options.onChange ? options.onChange : function () {};
    var extractToken = options && options.extractToken ? options.extractToken : defaultToken;
    var timerId = null;
    var baseline = "";

    async function fetchToken() {
      var response = await fetch(url, { cache: "no-store" });
      if (!response.ok) throw new Error("Checksum request failed");
      var payload = await response.json();
      return extractToken(payload);
    }

    async function check() {
      try {
        var token = await fetchToken();
        if (!token) return;
        if (!baseline) {
          baseline = token;
          return;
        }
        if (token !== baseline) {
          baseline = token;
          onChange(token);
        }
      } catch (e) {
        // Silent network failure; next cycle retries.
      }
    }

    return {
      start: function () {
        if (timerId) return;
        check();
        timerId = window.setInterval(check, pollIntervalMs);
      },
      stop: function () {
        if (!timerId) return;
        window.clearInterval(timerId);
        timerId = null;
      },
      checkNow: check
    };
  }

  window.CPFeedChecksumWatcher = { create: create };
})();
