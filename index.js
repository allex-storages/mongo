var MongoClient = require('mongodb').MongoClient;

function createMongoStorage(execlib){
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    dataSuite = execlib.dataSuite,
    StorageBase = dataSuite.StorageBase,
    mongoSuite = {
      filterFactory: require('./filters/factorycreator')(execlib)
    };

  function MongoStorage(storagedescriptor){
    if (!storagedescriptor){
      throw new lib.Error('NO_STORAGEDESCRIPTOR', 'MongoStorage needs a storagedescriptor in constructor');
    }
    if (!storagedescriptor.database) {
      throw new lib.Error('NO_DATABASE_IN_STORAGEDESCRIPTOR', 'MongoStorage needs a storagedescriptor.database name in constructor');
    }
    if (!storagedescriptor.table) {
      throw new lib.Error('NO_TABLE_IN_STORAGEDESCRIPTOR', 'MongoStorage needs a storagedescriptor.table name in constructor');
    }
    StorageBase.call(this,storagedescriptor);
    this.db = null;
    this.dbname = storagedescriptor.database;
    this.collectionname = storagedescriptor.table;
    this._idname = storagedescriptor._idname;
    this.q = new lib.Fifo();
    MongoClient.connect(this.connectionStringOutOf(storagedescriptor), this.onConnected.bind(this));
  }
  lib.inherit(MongoStorage,StorageBase);
  MongoStorage.prototype.destroy = function () {

    if (this.q) {
      this.drainQ();
      this.q.destroy();
    }
    this.q = null;
    this.collectionname = null;
    this.dbname = null;
    if(this.db){
      this.db.close();
    }
    this.db = null;
    StorageBase.prototype.destroy.call(this);
  };
  MongoStorage.prototype.drainQ = function () {
    var qe;
    while (this.q.length) {
      qe = this.q.pop();
      qe[qe.length-1].reject('MongoStorage draining');
    }
  };
  MongoStorage.prototype.satisfyQ = function () {
    var qe, methodname, method;
    while (this.q && this.q.length) {
      qe = this.q.pop();
      methodname = qe.shift();
      method = this[methodname];
      if('function' === typeof method){
        method.apply(this,qe);
      }
    }
  };
  MongoStorage.prototype.connectionStringOutOf = function (storagedescriptor) {
    var cs = 'mongodb://', server = storagedescriptor.server;
    if(lib.isArray(server)){
      cs += server.join(',');
    } else {
      cs += server
    }
    if (storagedescriptor.database) {
      cs += ('/'+storagedescriptor.database);
    }
    //console.log('Connection string,',storagedescriptor,'=>',cs);
    return cs;
  };
  MongoStorage.prototype.onConnected = function (err, db) {
    if (err) {
      //wut
    } else {
      this.db = db;
      this.satisfyQ();
    }
  };
  function _id2nameRemapper(_idname, ret, item, itemname) {
    if(itemname === '_id') {
      ret[_idname] = item;
    } else if (itemname === _idname) {
      return;
    } else {
      ret[itemname] = item;
    }
  }
  MongoStorage.prototype.remap_db2allex = function (hash) {
    var ret = {};
    lib.traverseShallow(hash, _id2nameRemapper.bind(null, this._idname, ret));
    //console.log('after remap_db2allex', hash, '=>', ret);
    return ret;
  };
  MongoStorage.prototype.db2allex = function (item) {
    if(this._idname){
      return this.remap_db2allex(item);
    } else {
      return item;
    }
  };
  function name2_idRemapper(_idname, ret, item, itemname) {
    if(itemname === '_id'){
      return;
    } else if (itemname === _idname) {
      ret['_id'] = item;
    } else {
      ret[itemname] = item;
    }
  }
  MongoStorage.prototype.remap_allex2db = function (hash) {
    var ret = {};
    lib.traverseShallow(hash, name2_idRemapper.bind(null, this._idname, ret));
    //console.log('after remap_db2allex', hash, '=>', ret);
    return ret;
  };
  MongoStorage.prototype.allex2db = function (item) {
    if(this._idname){
      return this.remap_allex2db(item);
    } else {
      return item;
    }
  }
  MongoStorage.prototype.reportItem = function (defer, totalcount, err, item) {
    //console.log('cursor item', item);
    if (err) {
      defer.reject(err);
    } else {
      if (item) {
        defer.notify(this.db2allex(item));
      } else {
        defer.resolve(totalcount);
      }
    }
  }
  MongoStorage.prototype.consumeCursor = function (cursor, defer) {
    cursor.each(this.reportItem.bind(this, defer, cursor.count()));
    /*
    var cc = cursor.count(), t = this;
    cursor.each(function (err, item) {
      console.log('item', item);
      t.reportItem(defer, cc, err, item);
    });
    */
  }
  MongoStorage.prototype.doRead = function (query, defer) {
    var collection,
      findparams,
      findcursor,
      descriptor;
    if (!this.db) {
      this.q.push(['doRead',query,defer]);
      return;
    }
    collection = this.db.collection(this.collectionname);
    if (!collection) {
      defer.reject(new lib.Error('MONGODB_COLLECTION_DOES_NOT_EXIST','MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
      return;
    }
    descriptor = query.filter().descriptor();
    //console.log('descriptor',descriptor);
    if(descriptor && descriptor.field && descriptor.field === this._idname){
      descriptor.field = '_id';
    }
    findparams = mongoSuite.filterFactory.createFromDescriptor(descriptor);
    //console.log(this.collectionname,'mongo doRead',descriptor,'=>',require('util').inspect(findparams, {depth:null}));
    findcursor =  collection.find.apply(collection,findparams);
    try{
      this.consumeCursor(findcursor, defer);
    } catch (e) {
      console.error(e.stack);
      console.error(e);
      defer.reject(e);
    }
  };
  MongoStorage.prototype.doCreate = function (datahash, defer){
    var collection;
    if (!this.db) {
      this.q.push(['doCreate', datahash, defer]);
      return;
    }
    datahash = this.__record.filterHash(datahash);
    collection = this.db.collection(this.collectionname);
    if (!collection) {
      defer.reject(new lib.Error('MONGODB_COLLECTION_DOES_NOT_EXIST','MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
      return;
    }
    //console.log('doCreate produces',datahash,'=>',this.allex2db(datahash));
    collection.insert(this.allex2db(datahash),{},this.onCreated.bind(this, defer));
  };
  MongoStorage.prototype.onCreated = function (defer, err, data){
    if (err) {
      defer.reject(err);
    } else {
      if (data.insertedCount===1) {
        defer.resolve(this.db2allex(data.ops[0]));
      } else {
        throw new lib.Error('MULTI_INSERT_NOT_SUPPORTED', 'Now what?');
      }
    }
  };
  MongoStorage.prototype.doDelete = function (filter, defer) {
    var collection, mfiltertemp, mfilter;
    if (!this.db) {
      this.q.push(['doDelete', filter, defer]);
      return;
    }
    collection = this.db.collection(this.collectionname);
    console.log('doDelete filter.__descriptor', filter.__descriptor);
    console.log('resulting filter array', mongoSuite.filterFactory.createFromDescriptor(filter.__descriptor));
    mfiltertemp = mongoSuite.filterFactory.createFromDescriptor(filter.__descriptor).map(this.allex2db.bind(this));
    mfilter = mfiltertemp[0];
    //console.log(filter,'=>',mfiltertemp,'=>',mfilter);
    collection.remove(mfilter,{fsync:true},function(err, data){
      if (err) {
        defer.reject(err);
      } else {
        defer.resolve(data);
      }
    });
  };

  MongoStorage.prototype.doUpdate = function (filter, updateobj, options, defer) {
    var collection,
      descriptor,
      updateparams;
    collection = this.db.collection(this.collectionname);
    if (!collection) {
      defer.reject(new lib.Error('MONGODB_COLLECTION_DOES_NOT_EXIST','MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
      return;
    }
    descriptor = filter.__descriptor;
    //console.log('descriptor',descriptor);
    var changed = false;
    if(descriptor && descriptor.field && descriptor.field === this._idname){
      descriptor.field = '_id';
      changed = true;
    }
    updateparams = mongoSuite.filterFactory.createFromDescriptor(descriptor);
    updateparams.push(updateobj);
    updateparams.push(options);
    updateparams.push(this.onUpdated.bind(this, defer, filter, changed));
    collection.update.apply(collection, updateparams);
  };
  MongoStorage.prototype.onUpdated = function (defer, filter, changed, err, updateobj) {
    if (changed && filter && filter.__descriptor && filter.__descriptor.field === '_id') {
      filter.__descriptor.field = this._idname;
    }
    if (err) {
      defer.reject(new lib.Error('MONGO_COULD_NOT_UPDATE','No update done'));
    } else {
      defer.resolve({updated: updateobj.result.nModified});
    }
  };

  return MongoStorage;
}

module.exports = createMongoStorage;
