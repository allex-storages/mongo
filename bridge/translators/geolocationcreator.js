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
    if (lib.isVal(thingy) && lib.isNumber(thingy.longitude) && lib.isNumber(thingy.latitude)) {
      return {
        type: 'Point',
        coordinates: [thingy.longitude, thingy.latitude]
      }
    }
  };
  GeoLocationTranslator.prototype.mongo2allex = function (thingy) {
    if (
      lib.isVal(thingy) &&
      thingy.type === 'Point' &&
      lib.isArray(thingy.coordinates) &&
      thingy.coordinates.length === 2
    ) {
      return {
        longitude: thingy.coordinates[0],
        latitude: thingy.coordinates[1]
      };
    }
  };

  return GeoLocationTranslator;
}
module.exports = createGeolocationTranslator;
