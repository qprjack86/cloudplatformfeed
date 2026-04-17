(function () {
  "use strict";

  function activeCategoryList(selectedSet) {
    if (!selectedSet) return [];
    if (!selectedSet.size) return [];
    if (selectedSet.has("all")) return [];
    return Array.from(selectedSet);
  }

  window.CPFeedFilterHelpers = {
    activeCategoryList: activeCategoryList
  };
})();
