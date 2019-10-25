function createNearFilter(execlib, fieldValue) {
  'use strict';
  return function (filter) {
    var findhash = {},
      filterobj = {"$near" : {
        $geometry: { type: 'Point', coordinates: [filter.value.longitude, filter.value.latitude] },
        $minDistance: filter.minDistance || 0,
        $maxDistance: filter.maxDistance || 10000000
      }},
      options = '';
    if (options) {
      filterobj.$options = options;
    }
    findhash[filter.field] = filterobj;
    return [findhash];
  };
}

module.exports = createNearFilter;
