var mongodb = require('mongodb'),
  MongoClient = mongodb.MongoClient,
  ObjectID = mongodb.ObjectID;

function main(execlib) {
  'use strict';
  return execlib.loadDependencies('client', ['allex_dataservice'], createMongoStorage.bind(null, execlib));
}

function createMongoStorage(execlib){
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    dataSuite = execlib.dataSuite,
    StorageBase = dataSuite.StorageBase,
    mongoSuite = {
      filterFactory: require('./filters/factorycreator')(execlib, ObjectID)
    },
    AllexMongoBridge = require('./bridge')(execlib, ObjectID),
    ensureIndices = require('./indices')(execlib);

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
    this.client = null;
    this.db = null;
    this.dbname = storagedescriptor.database;
    this.collectionname = storagedescriptor.table;
    this._idname = storagedescriptor._idname || (lib.isString(storagedescriptor.primaryKey) ? storagedescriptor.primaryKey : null);
    this._nativeid = storagedescriptor._nativeid;
    this.q = new lib.Fifo();
    this.bridge = new AllexMongoBridge(storagedescriptor);
    this.connect(storagedescriptor);
  }
  lib.inherit(MongoStorage,StorageBase);
  MongoStorage.prototype.destroy = function () {
    console.trace();
    console.error(arguments);
    console.error(this.constructor.name, 'dying');
    if (this.bridge) {
      this.bridge.destroy();
    }
    this.bridge = null;
    if (this.q) {
      this.drainQ();
      this.q.destroy();
    }
    this.q = null;
    this._nativeid = null;
    this._idname = null;
    this.collectionname = null;
    this.dbname = null;
    if(this.client){
      this.client.close();
    }
    this.client = null;
    this.db = null;
    StorageBase.prototype.destroy.call(this);
  };
  MongoStorage.prototype.connect = function (storagedescriptor) {
    var _sd = storagedescriptor;
    MongoClient.connect(this.connectionStringOutOf(storagedescriptor), {
      useNewUrlParser: true,
      useUnifiedTopology: true
    },this.onConnected.bind(this, _sd));
    _sd = null;
  };
  MongoStorage.prototype.drainQ = function () {
    this.q.drain(this.drainer.bind(this));
  };
  MongoStorage.prototype.drainer = function (qe) {
    qe[qe.length-1].reject(new lib.Error('MONGO_STORAGE_DRAINING', 'MongoStorage draining'));
  };
  MongoStorage.prototype.satisfyQ = function () {
    if (this.q) {
      this.q.drain(this.satisfyDrainer.bind(this));
    }
  };
  MongoStorage.prototype.satisfyDrainer = function (qe) {
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
    /*
    if (storagedescriptor.database) {
      cs += ('/'+storagedescriptor.database);
    }
    */
    //console.log('Connection string,',storagedescriptor,'=>',cs);
    return cs;
  };
  MongoStorage.prototype.onConnected = function (storagedescriptor, err, client) {
    var _cl;
    if (err) {
      console.log('ERROR IN CONNECTING TO MONGO DB:', err);
    } else {
      _cl = client;
      ensureIndices(storagedescriptor, client.db(this.dbname).collection(this.collectionname)).then(
        this.finalizeConnect.bind(this, _cl),
        this.destroy.bind(this)
      );
      _cl = null;
    }
  };
  MongoStorage.prototype.finalizeConnect = function (client) {
    this.client = client;
    this.db = client.db(this.dbname);
    this.satisfyQ();
  };
  MongoStorage.prototype.handleQ = function (commandarry) {
    if (this.q) {
      this.q.push(commandarry)
    } else {
      console.error('How come handleQ is called with');
      console.error(commandarry);
      console.error('when', this.constructor.name, 'is dead?');
      console.error(this);
    }
  };
  MongoStorage.prototype.db2allex = function (item) {
    if (!this.bridge) {
      return null;
    }
    return this.bridge.mongo2allex(item);
  };
  MongoStorage.prototype.allex2db = function (item, skipid) {
    if (!this.bridge) {
      return null;
    }
    return this.bridge.allex2mongo(item, skipid);
  };
  MongoStorage.prototype.dberr2allexerr = function (err) {
    if (!this.bridge) {
      return err;
    }
    return this.bridge.mongoerror2allexerror(err);
  };
  MongoStorage.prototype.reportItem = function (cursor, defer, totalcount, err, item) {
    if (err) {
      //console.log('rejecting with', err);
      defer.reject(this.dberr2allexerr(err));
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
    var _c = cursor;
    cursor.count(this.consumeCursorWCount.bind(this, _c, defer));
    _c = null;
  }
  MongoStorage.prototype.consumeCursorWCount = function (cursor, defer, err, count) {
    if (err) {
      defer.reject(this.dberr2allexerr(err));
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
      descriptor,
      limit,
      offset;
    if (!this.db) {
      this.handleQ(['doRead',query,defer]);
      return;
    }
    //console.log('doRead', query);
    collection = this.db.collection(this.collectionname);
    if (!collection) {
      defer.reject(new lib.Error('MONGODB_COLLECTION_DOES_NOT_EXIST','MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
      return;
    }
    descriptor = query.filter().descriptor();
    remapFilter(this._idname, descriptor);
    //console.log('descriptor',descriptor);
    findparams = mongoSuite.filterFactory.createFromDescriptor(descriptor, this);
    //console.log(this.collectionname,'mongo doRead',descriptor,'=>',require('util').inspect(findparams, {depth:null, colors:true}));
    findcursor =  collection.find.apply(collection,findparams);
    offset = query.offset();
    if(lib.isNumber(offset) && offset>0) {
      findcursor = findcursor.skip(offset);
    }
    limit = query.limit();
    if (lib.isNumber(limit)) {
      findcursor = findcursor.limit(limit);
    }
    //console.log('limit', limit, 'offset', offset);
    try{
      this.consumeCursor(findcursor, defer);
    } catch (e) {
      console.error(e.stack);
      console.error(e);
      defer.reject(this.dberr2allexerr(e));
    }
  };
  MongoStorage.prototype.doCreate = function (datahash, defer){
    var collection;
    if (!this.db) {
      this.handleQ(['doCreate', datahash, defer]);
      return;
    }
    datahash = this.__record.filterHash(datahash);
    collection = this.db.collection(this.collectionname);
    if (!collection) {
      defer.reject(new lib.Error('MONGODB_COLLECTION_DOES_NOT_EXIST','MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
      return;
    }
    //console.log('doCreate produces',datahash,'=>',this.allex2db(datahash));
    collection.insertOne(this.allex2db(datahash),{},this.onCreated.bind(this, defer));
  };
  MongoStorage.prototype.onCreated = function (defer, err, data){
    if (err) {
      defer.reject(this.dberr2allexerr(err));
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
      this.handleQ(['doDelete', filter, defer]);
      return;
    }
    collection = this.db.collection(this.collectionname);
    descriptor = filter.descriptor();
    //remapFilter(this._idname, descriptor);
    changed = this.maybeChangeDescriptorField(descriptor);
    mfiltertemp = mongoSuite.filterFactory.createFromDescriptor(descriptor, this);//.map(this.allex2db.bind(this));
    //console.log('doDelete filter.__descriptor', filter.__descriptor);
    //console.log('resulting filter array', mongoSuite.filterFactory.createFromDescriptor(filter.__descriptor, this));
    //console.log('delete filter', filter, 'desc', descriptor, '=>', mfiltertemp);
    /*
    */
    if (!lib.isArray(mfiltertemp)) {
      defer.reject(new lib.Error('FILTER_NOT_IMPLEMENTED', JSON.stringify(descriptor||{})+' is not implemented in allex_mongostorage'));
      return;
    }
    mfilter = mfiltertemp[0];
    //console.log(filter,'=>',mfiltertemp,'=>',mfilter);
    collection.removeMany(mfilter,{fsync:true},this.onDeleted.bind(this, changed, descriptor, defer));
  };
  MongoStorage.prototype.onDeleted = function (changed, descriptor, defer, err, data) {
    if (err) {
      defer.reject(this.dberr2allexerr(err));
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
    if (!this.db) {
      return q.reject (new lib.Error('NOT_CONNECTED'));
    }
    var collection = this.db.collection(this.collectionname);

    if (!collection) return q.reject (new lib.Error ('MONGODB_COLLECTION_DOES_NOT_EXIST', 'MongoDB database '+this.dbname+' does not have a collection named '+this.collectionname));
    var cursor = collection.aggregate(aggregation_descriptor, {cursor : {batchSize : 1}});
    var defer = lib.q.defer();
    var uid = lib.uid();
    defer.notify (['rb', uid]);
    cursor.each (this._sendAggDoc.bind(this, defer, uid));
    return defer.promise;
  };

  MongoStorage.prototype._sendAggDoc = function (defer, uid, err, doc) {
    if (err) {
      defer.reject (this.dberr2allexerr(err));
      return;
    }

    if (null === doc) {
      defer.notify(['re', uid]);
      defer.resolve('done');
      return;
    }
    defer.notify (['r1', uid, doc]);
  };

  function eacheradder (obj, val, name) {
    if (!lib.isArray(val)) {
      return;
    }
    obj[name] = {$each: val};
  }
  function eacher (updateobj) {
    var ret = {}, _r = ret;
    lib.traverseShallow(updateobj, eacheradder.bind(null, _r));
    _r = null;
    return ret;
  }

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
      case 'pusharray':
        updateparams.push({ $push: eacher(updateobj) });
        break;
      case 'addtoset':
        updateparams.push({ $addToSet: updateobj });
        break;
      case 'addtosetarray':
        updateparams.push({ $addToSet: eacher(updateobj) });
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
      case 'inc':
        updateparams.push({ $inc: updateobj });
        break;
      default:
        console.log('updateobj will become', updateobj, '=>', this.__record.filterOut(updateobj));
        defupdobj = this.allex2db(this.__record.filterOut(updateobj), true);
        updateparams.push(defupdobj);
        break;
    }
    updateparams.push(updateOptions(options));
    updateparams.push(this.onUpdated.bind(this, defer, filter, updateparams, changed));
    //console.log(this.collectionname, 'update', require('util').inspect(updateparams, {depth:8, colors:true}));
    collection.updateMany.apply(collection, updateparams);
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

module.exports = main;
