function createAllexMongoBrigde (execlib, ObjectID) {
  'use strict';

  var lib = execlib.lib,
    Map = lib.Map;

  var _translatableTypes = ['GeoJSON'];

  function adder (buildobj, field) {
    if (!(field.name && field.type)) {
      console.error('unusable field')
      return;
    }
    buildobj.hasTranslatableTypes = 
      buildobj.hasTranslatableTypes
      ||
      _translatableTypes.indexOf(field.type)>=0;
    buildobj.map.add(field.name, field.type);
  }
  function buildTranslators (map, fields) {
    var buildobj = {map: map, hasTranslatableTypes: false}, _bo = buildobj;
    if (!lib.isArray(fields)) {
      return;
    }
    fields.forEach(adder.bind(null, _bo));
    _bo = null;
    return buildobj.hasTranslatableTypes;
  }

  function AllexMongoBridge (storagedescriptor) {
    console.log('fields?', storagedescriptor.fields);
    this.translators = new Map();
    buildTranslators(this.translators, storagedescriptor.fields);
    console.log('hasTranslatableTypes', this.hasTranslatableTypes);
  }
  AllexMongoBridge.prototype.destroy = function () {
    this.hasTranslatableTypes = null;
    if (this.fieldTypes) {
      this.fieldTypes.destroy();
    }
    this.fieldTypes = null;
  };
  AllexMongoBridge.prototype.allex2mongo = function (item, skipid) {
  };
  AllexMongoBridge.prototype.mongo2allex = function (item) {
  };

  return AllexMongoBridge;
}
module.exports = createAllexMongoBrigde;
