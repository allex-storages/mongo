function createAndFilter(execlib, factory) {
  'use strict';
  var lib = execlib.lib;
  return function (filter) {
    if(lib.isArray(filter.filters)){
      return [{$and: filter.filters.map(function (subfilter) {
        return factory.createFromDescriptor(subfilter)[0];
      })}];
    }
    return [];
  };
}

module.exports = createAndFilter;
