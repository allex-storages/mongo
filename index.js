var mongodb = require('mongodb'),
  MongoClient = mongodb.MongoClient,
  ObjectID = mongodb.ObjectID;

function createMongoStorage(execlib){
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    dataSuite = execlib.dataSuite,
    StorageBase = dataSuite.StorageBase,
    mongoSuite = {
      filterFactory: require('./filters/factorycreator')(execlib, ObjectID)
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
    this._nativeid = storagedescriptor._nativeid;
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
    this.q.drain(this.drainer.bind(this));
  };
  MongoStorage.prototype.drainer = function (qe) {
    qe[qe.length-1].reject('MongoStorage draining');
  };
  MongoStorage.prototype.satisfyQ = function () {
    if (this.q) {
      this.q.drain(this.satifyDrainer.bind(this));
    }
  };
  MongoStorage.prototype.satifyDrainer = function (qe) {
    var methodname = qe.shift(),
      method = this[methodname];
    if(lib.isFunction(method)){
      method.apply(this,qe);
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
      console.log('ERROR IN CONNECTING TO MONGO DB:', err);
    } else {
      this.db = db;
      this.satisfyQ();
    }
  };
  function _id2nameRemapper(_idname, ret, item, itemname) {
    if(itemname === '_id') {
      if (item) {
        ret[_idname] = item.toString();
      }
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
  function name2_idRemapper(skipid, _nativeid, _idname, ret, item, itemname) {
    if(itemname === '_id'){
      return;
    } else if ( (itemname === _idname) && !skipid) {
      ret['_id'] = _nativeid ? new ObjectID(item) : item;
    } else {
      ret[itemname] = item;
    }
  }
  MongoStorage.prototype.remap_allex2db = function (hash, skipid) {
    var ret = {};
    lib.traverseShallow(hash, name2_idRemapper.bind(null, skipid, this._nativeid, this._idname, ret));
    //console.log('after remap_db2allex', hash, '=>', ret);
    return ret;
  };
  MongoStorage.prototype.allex2db = function (item, skipid) {
    if(this._idname || this._nativeid){
      return this.remap_allex2db(item, skipid);
    } else {
      return item;
    }
  }
  MongoStorage.prototype.reportItem = function (cursor, defer, totalcount, err, item) {
    if (err) {
      //console.log('rejecting with', err);
      defer.reject(err);
    } else {
      if (item) {
        //console.log('notifying with', this.db2allex(item), 'because', item);
        defer.notify(this.db2allex(item));
      } else {
        //console.log('resolving with', totalcount);
        cursor.close();
        defer.resolve(totalcount);
      }
    }
    cursor = null;
    defer = null;
  }
  MongoStorage.prototype.consumeCursor = function (cursor, defer) {
    cursor.count(this.consumeCursorWCount.bind(this, cursor, defer));
  }
  MongoStorage.prototype.consumeCursorWCount = function (cursor, defer, err, count) {
    if (err) {
      defer.reject(err);
      defer = null;
      return;
    }
    cursor.each(this.reportItem.bind(this, cursor, defer, count));
  };
  function remapFilter(_idname, filter) {
    if(filter && filter.field && filter.field === _idname){
      filter.field = '_id';
    }
    if (filter && lib.isArray(filter.filters)) {
      filter.filters.forEach(remapFilter.bind(null, _idname));
    }
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
    remapFilter(this._idname, descriptor);
    //console.log('descriptor',descriptor);
    findparams = mongoSuite.filterFactory.createFromDescriptor(descriptor, this);
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
    var collection, descriptor, mfiltertemp, mfilter, changed;
    if (!this.db) {
      this.q.push(['doDelete', filter, defer]);
      return;
    }
    collection = this.db.collection(this.collectionname);
    descriptor = filter.descriptor();
    //remapFilter(this._idname, descriptor);
    changed = this.maybeChangeDescriptorField(descriptor);
    mfiltertemp = mongoSuite.filterFactory.createFromDescriptor(descriptor, this);//.map(this.allex2db.bind(this));
    console.log('doDelete filter.__descriptor', filter.__descriptor);
    console.log('resulting filter array', mongoSuite.filterFactory.createFromDescriptor(filter.__descriptor, this));
    console.log('delete filter', filter, 'desc', descriptor, '=>', mfiltertemp);
    /*
    */
    mfilter = mfiltertemp[0];
    //console.log(filter,'=>',mfiltertemp,'=>',mfilter);
    collection.remove(mfilter,{fsync:true},this.onDeleted.bind(this, changed, descriptor, defer));
  };
  MongoStorage.prototype.onDeleted = function (changed, descriptor, defer, err, data) {
    if (err) {
      defer.reject(err);
    } else {
      if (changed) {
        this.maybeRevertDescriptorField(descriptor);
      }
      //console.log('onDeleted resolving', data.result, 'with final descriptor', descriptor);
      defer.resolve(data);
    }
    defer = null;
  };

  function updateOptions (options) {
    if (options.upsert) {
      return {
        upsert: options.upsert
      };
    }
    return {};
  }

  MongoStorage.prototype.maybeChangeDescriptorField = function (descriptor) {
    var changed = false;
    if(descriptor && descriptor.field && descriptor.field === this._idname){
      descriptor.field = '_id';
      changed = true;
      if(this._nativeid) {
        descriptor.value = new ObjectID(descriptor.value);
      }
    }
    if (descriptor && lib.isArray(descriptor.filters)) {
      changed = changed || (descriptor.filters.map(this.maybeChangeDescriptorField.bind(this)).indexOf(true)>=0);
    }
    return changed;
  };

  MongoStorage.prototype.maybeRevertDescriptorField = function (descriptor) {
    var changed = false
    if (descriptor && descriptor.field === '_id') {
      descriptor.field = this._idname;
      changed = true;
    }
    if (descriptor && lib.isArray(descriptor.filters)) {
      changed = changed || (descriptor.filters.map(this.maybeRevertDescriptorField.bind(this)).indexOf(true)>=0);
    }
    return changed;
  };

  MongoStorage.prototype.doAggregate = function (aggregation_descriptor) {
    ///TODO: nije to dovoljno ... ima tu jos par stvari ... pogledaj : http://mongodb.github.io/node-mongodb-native/2.2/api/AggregationCursor.html
    var collection = this.db.collection(this.collectionname);

    if (!collection) return q.reject (new lib.Error ('MONGODB_COLLECTION_DOES_NOT_EXIST', 'MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
    var cursor = collection.aggregate(aggregation_descriptor, {cursor : {batchSize : 1}});
    var defer = lib.q.defer();
    defer.notify ({'op': 'start'});
    cursor.each (this._sendAggDoc.bind(this, defer));
    return defer.promise;
  };

  MongoStorage.prototype._sendAggDoc = function (defer, err, doc) {
    if (err) {
      defer.reject (err);
      return;
    }

    if (null === doc) {
      defer.resolve('done');
      return;
    }
    defer.notify ({
      'op' : 'next',
      'data' : doc
    });
  };

  MongoStorage.prototype.doUpdate = function (filter, updateobj, options, defer) {
    var collection = this.db.collection(this.collectionname),
      descriptor,
      updateparams,
      changed,
      defupdobj;
    if (!collection) {
      defer.reject(new lib.Error('MONGODB_COLLECTION_DOES_NOT_EXIST','MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
      return;
    }
    descriptor = filter.__descriptor;
    //console.log('update descriptor',descriptor);
    changed = this.maybeChangeDescriptorField(descriptor);//false;
    
    updateparams = mongoSuite.filterFactory.createFromDescriptor(descriptor, this);
    updateobj = this.allex2db(updateobj);
    options = options || {};
    switch (options.op) {
      case 'push':
        updateparams.push({ $push: updateobj });
        break;
      case 'addtoset':
        updateparams.push({ $addToSet: updateobj });
        break;
      case 'removeallfromset':
        updateparams.push({ $pullAll: updateobj });
        break;
      case 'pull':
        updateparams.push({ $pull: updateobj });
        break;
      case 'set':
        updateparams.push({ $set: updateobj });
        break;
      default:
        //console.log('updateobj will become', updateobj, '=>', this.__record.filterOut(updateobj));
        defupdobj = this.allex2db(this.__record.filterOut(updateobj), true);
        updateparams.push(defupdobj);
        break;
    }
    updateparams.push(updateOptions(options));
    updateparams.push(this.onUpdated.bind(this, defer, filter, updateparams, changed));
    console.log(this.collectionname, 'update', updateparams);
    collection.update.apply(collection, updateparams);
  };
  MongoStorage.prototype.onUpdated = function (defer, filter, updateparams, changed, err, updateobj) {
    //console.log('onUpdated', err, updateobj);
    var up;
    if (changed && filter) {
      this.maybeRevertDescriptorField(filter.__descriptor);
    }
    if (err) {
      console.error(err);
      defer.reject(new lib.Error('MONGO_COULD_NOT_UPDATE','No update done'));
    } else {
      //console.log('onUpdated', updateobj.result);
      if (updateobj.result.upserted && updateobj.result.upserted.length) {
        up = updateparams[updateparams.length-3];
        up._id = updateobj.result.upserted[0]._id;
        up = this.db2allex(up);
        //console.log('notify', up, '?');
        defer.notify([up, null]);
        defer.resolve({upserted: updateobj.result.upserted.length});
      } else {
        defer.resolve({updated: updateobj.result.nModified});
      }
    }
  };

  return MongoStorage;
}

module.exports = createMongoStorage;
