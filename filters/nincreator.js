function createNinFilter(execlib) {
  'use strict';
  return function (filter) {
    var findhash = {};
    findhash[filter.field] = { $nin: filter.value };
    return [findhash];
  };
}

module.exports = createNinFilter;
