(function () {
  "use strict";

  function activeCategoryList(selectedSet) {
    if (!selectedSet || !selectedSet.size || selectedSet.has("all")) return [];
    return Array.from(selectedSet);
  }

  function firstSelectedOrAll(selectedSet) {
    var list = activeCategoryList(selectedSet);
    return list.length ? list[0] : "all";
  }

  window.CPFeedFilterHelpers = {
    activeCategoryList: activeCategoryList,
    firstSelectedOrAll: firstSelectedOrAll
  };
})();
