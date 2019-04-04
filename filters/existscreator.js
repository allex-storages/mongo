function createExistsFilter(execlib) {
  'use strict';
  return function (filter) {
    var findhash = {},
      filterobj = {"$exists" : true, "$ne" : null}; //our "exists" does not pass nulls

    findhash[filter.field] = filterobj;
    return [findhash];
  };
}

module.exports = createExistsFilter;
