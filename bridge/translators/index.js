function createTranslators (execlib) {
  'use strict';

  var lib = execlib.lib;

  var GeoLocationTranslator = require('./geolocationcreator')(lib);

  function typeFactory (typename) {
    switch (typename) {
      case 'geolocation':
        return GeoLocationTranslator;
      default:
        return null;
    }
  }

  function factory (typename) {
    var c = typeFactory(typename);
    return c ? new c() : null;
  }

  return factory;
}
module.exports = createTranslators;
