function createRegexFilter(execlib) {
  'use strict';
  return function (filter) {
    var findhash = {},
      filterobj = {"$regex" : filter.value||''},
      options = '';
    if (filter.flags) {
      options = filter.flags;
    }
    if (options) {
      filterobj.$options = options;
    }
    findhash[filter.field] = filterobj;
    return [findhash];
  };
}

module.exports = createRegexFilter;
