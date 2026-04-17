(function () {
  "use strict";

  var KEY_PREFIX = "cloudplatformfeed-ui";

  function buildStorageKey(key) {
    return KEY_PREFIX + ":" + key;
  }

  function readJson(key, fallback) {
    var raw;
    try {
      raw = localStorage.getItem(buildStorageKey(key));
    } catch (e) {
      return fallback;
    }

    if (raw === null) return fallback;

    try {
      var parsed = JSON.parse(raw);
      return parsed == null ? fallback : parsed;
    } catch (e) {
      return fallback;
    }
  }

  function writeJson(key, value) {
    try {
      localStorage.setItem(buildStorageKey(key), JSON.stringify(value));
    } catch (e) {
      // Ignore storage failures in private mode / quota pressure.
    }
  }

  window.CPFeedStateStore = {
    readJson: readJson,
    writeJson: writeJson
  };
})();
