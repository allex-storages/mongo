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
    while (this.q.length) {
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
    console.log('Connection string,',storagedescriptor,'=>',cs);
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
  function reportItem (defer, totalcount, err, item) {
    if (err) {
      defer.reject(err);
    } else {
      if (item) {
        defer.notify(item);
      } else {
        defer.resolve(totalcount);
      }
    }
  }
  function consumeCursor(cursor, defer) {
    cursor.each(reportItem.bind(null, defer, cursor.count()));
  }
  MongoStorage.prototype.doRead = function (query, defer) {
    var collection,
      findparams,
      findcursor;
    if (!this.db) {
      this.q.push(['doRead',query,defer]);
      return;
    }
    collection = this.db.collection(this.collectionname);
    if (!collection) {
      defer.reject(new lib.Error('MONGODB_COLLECTION_DOES_NOT_EXIST','MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
      return;
    }
    findparams = mongoSuite.filterFactory.createFromDescriptor(query.filter().descriptor());
    findcursor =  collection.find.apply(collection,findparams);
    try{
      consumeCursor(findcursor, defer);
    } catch (e) {
      console.error(e.stack);
      console.error(e);
      defer.reject(e);
    }
  };

  return MongoStorage;
}

module.exports = createMongoStorage;
