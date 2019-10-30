function createAllexMongoBrigde (execlib, ObjectID) {
  'use strict';

  var lib = execlib.lib,
    Map = lib.Map,
    translatorFactory = require('./translators')(execlib);

  var _translatableTypes = ['GeoJSON'];

  function adder (map, field) {
    var t;
    if (!(field.name && field.type)) {
      return;
    }
    t = translatorFactory(field.type);
    if (t) {
      map.add(field.name, t);
    }
  }
  function buildTranslators (map, fields) {
    var _m;
    if (!lib.isArray(fields)) {
      return;
    }
    _m = map;
    fields.forEach(adder.bind(null, _m));
    _m = null;
  }

  function AllexMongoBridge (storagedescriptor) {
    console.log('fields?', storagedescriptor.fields);
    this._nativeid = storagedescriptor._nativeid;
    this._idname = storagedescriptor._idname || (lib.isString(storagedescriptor.primaryKey) ? storagedescriptor.primaryKey : null);
    this.translators = new Map();
    buildTranslators(this.translators, storagedescriptor.fields);
  }
  AllexMongoBridge.prototype.destroy = function () {
    if (this.translators) {
      this.translators.destroy();
    }
    this.translators = null;
    this._idname = null;
    this._nativeid = null;
  };
  AllexMongoBridge.prototype.allex2mongo = function (item, skipid) {
    if (!(this._idname || this._nativeid || this.translators.count>0)) {
      return item;
    }
    return this.remap_allex2mongo(item, skipid);
  };
  AllexMongoBridge.prototype.remap_allex2mongo = function (item, skipid) {
    var ret = {}, _ret = ret;
    lib.traverseShallow(hash, this.allex2mongoRemapper.bind(this, skipid, _ret));
    _ret = null;
    //console.log('after remap_db2allex', hash, '=>', ret);
    return ret;
  };
  AllexMongoBridge.prototype.allex2mongoRemapper = function (skipid, ret, item, itemname) {
    if (itemname === this._idname) {
      if (skipid) {
        return;
      }
      ret['_id'] = this._nativeid ? new ObjectID(item) : item;
      return;
    }
    ret[itemname] = item;
    /*
    if(itemname === '_id'){
      return;
    } else if ( (itemname === _idname) && !skipid) {
      ret['_id'] = _nativeid ? new ObjectID(item) : item;
    } else {
      ret[itemname] = item;
    }
    */
  };
  AllexMongoBridge.prototype.mongo2allex = function (item) {
    if(this._idname){
      return this.remap_mongo2allex(item);
    }
    return item;
  };
  AllexMongoBridge.prototype.remap_mongo2allex = function (item) {
    var ret = {}, _r = ret;
    lib.traverseShallow(hash, _id2nameRemapper.bind(null, _r));
    _r = null;
    //console.log('after remap_db2allex', hash, '=>', ret);
    return ret;
  };
  AllexMongoBridge.prototype.mongo2allexRemapper = function (ret, item, itemname) {
    if (itemname === this._idname) {
      return;
    }
    if(itemname === '_id') {
      if (lib.isVal(item)) {
        ret[_idname] = this.translateMongo2Allex(item).toString();
      }
      return;
    }
    ret[itemname] = this.translateMongo2Allex(item);
  };

  AllexMongoBridge.prototype.translateAllex2Mongo = function (item, itemname) {
    var t = this.translators.get(itemname);
    if (!t) {
      return item;
    }
    return t.allex2mongo(item);
  };
  AllexMongoBridge.prototype.translateMongo2Allex = function (item, itemname) {
    var t = this.translators.get(itemname);
    if (!t) {
      return item;
    }
    return t.mongo2allex(item);
  };

  return AllexMongoBridge;
}
module.exports = createAllexMongoBrigde;
