function createGeolocationTranslator (lib) {
  'use strict';

  function GeoLocationTranslator () {
  }
  GeoLocationTranslator.prototype.destroy = function () {
  };
  GeoLocationTranslator.prototype.allex2mongo = function (thingy) {
    if (lib.isArray(thingy) && thingy.length===2) {//assume that it's a [long, lat] pair
      return {
        type: 'Point',
        coordinates: thingy
      };
    }
  };


  return GeoLocationTranslator;
}
module.exports = createGeolocationTranslator;
