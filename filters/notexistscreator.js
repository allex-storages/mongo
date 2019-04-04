function createNotexistsFilter(execlib) {
  'use strict';
  return function (filter) {
    var findhash = {},
      filterobj = {"$not": {"$exists" : true, "$ne" : null}}; //our "exists" does not pass nulls

    findhash[filter.field] = filterobj;
    return [findhash];
  };
}

module.exports = createNotexistsFilter;
