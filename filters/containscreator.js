function createContainsFilter(execlib) {
  'use strict';
  return function (filter) {
    var findhash = {},
      filterobj = {"$regex" : filter.value},
      options = '';
    if (filter.caseinsensitive) {
      options += 'i';
    }
    if (options) {
      filterobj.$options = options;
    }
    findhash[filter.field] = filterobj;
    return [findhash];
  };
}

module.exports = createContainsFilter;
