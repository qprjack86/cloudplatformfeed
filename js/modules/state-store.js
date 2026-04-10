(function () {
  "use strict";

  var KEY_PREFIX = "cloudplatformfeed-ui";

  function readJson(key, fallback) {
    try {
      var raw = localStorage.getItem(KEY_PREFIX + ":" + key);
      if (!raw) return fallback;
      var parsed = JSON.parse(raw);
      return parsed == null ? fallback : parsed;
    } catch (e) {
      return fallback;
    }
  }

  function writeJson(key, value) {
    try {
      localStorage.setItem(KEY_PREFIX + ":" + key, JSON.stringify(value));
    } catch (e) {
      // Ignore storage failures in private mode / quota pressure.
    }
  }

  window.CPFeedStateStore = {
    readJson: readJson,
    writeJson: writeJson
  };
})();
