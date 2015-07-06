function createEqFilter(execlib) {
  'use strict';
  return function (filter) {
    console.log('DataService eq filter', filter);
  }
}

module.exports = createEqFilter;
