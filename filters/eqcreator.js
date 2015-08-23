function createEqFilter(execlib) {
  'use strict';
  return function (filter) {
    //console.log('DataService eq filter', filter);
    var findhash = {};
    findhash[filter.field] = filter.value;
    return [findhash];
  };
}

module.exports = createEqFilter;
