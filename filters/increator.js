function createInFilter(execlib) {
  'use strict';
  return function (filter) {
    var findhash = {};
    findhash[filter.field] = { $in: filter.value };
    return [findhash];
  };
}

module.exports = createInFilter;
