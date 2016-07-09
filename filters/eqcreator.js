function createEqFilter(execlib, fieldValue) {
  'use strict';
  return function (filter, options) {
    //console.log('DataService eq filter', filter);
    var findhash = {};
    findhash[filter.field] = fieldValue(filter.value, filter.field, options);
    return [findhash];
  };
}

module.exports = createEqFilter;
