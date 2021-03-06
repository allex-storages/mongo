function createBitMaskAllFilter(execlib, fieldValue) {
  'use strict';
  return function (filter) {
    var findhash = {},
      filterobj = {"$bitsAllSet" : fieldValue(filter.value, filter.field, {})||0},
      options = '';
    if (options) {
      filterobj.$options = options;
    }
    findhash[filter.field] = filterobj;
    return [findhash];
  };
}
module.exports = createBitMaskAllFilter;
