function createContainsFilter(execlib) {
  'use strict';
  return function (filter) {
    var findhash = {};
    findhash[filter.field] = {"$regex" : filter.value};
    return [findhash];
  };
}

module.exports = createContainsFilter;
