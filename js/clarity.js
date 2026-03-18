(function (window, document) {
  "use strict";

  var clarityId = "vub6kkd744";
  if (!clarityId) {
    return;
  }

  window.clarity = window.clarity || function () {
    (window.clarity.q = window.clarity.q || []).push(arguments);
  };

  var script = document.createElement("script");
  script.async = true;
  script.src = "https://www.clarity.ms/tag/" + clarityId;

  var firstScript = document.getElementsByTagName("script")[0];
  if (firstScript && firstScript.parentNode) {
    firstScript.parentNode.insertBefore(script, firstScript);
  } else {
    document.head.appendChild(script);
  }
})(window, document);