function createIndexEnsurer (execlib) {
  'use strict';
  var lib = execlib.lib,
    qlib = lib.qlib,
    JobBase = qlib.JobBase;

  var _autoIndexedTypes = {
    geolocation: '2dsphere'
  };

  function toAscIndex (result, field) {
    if (!result.key) {
      result.key = {};
    }
    result.key[field] = 1;
    return result;
  }
  function indexSpecInterpreter (result, spec) {
    //console.log('indexSpecInterpreter', result, spec);
    var indexobj = {};
    if (!lib.isVal(spec)) {
      return result;
    }
    if (lib.isArray(spec.fields)) {
      spec.fields.reduce(toAscIndex, indexobj);
    }
    if (lib.isArray(spec.desc)) {
      spec.desc.reduce(toAscIndex, indexobj);
    }
    if (spec.unique) {
      indexobj.unique = true;
    }
    if (indexobj.key) {
      result.push(indexobj);
    }
    return result;
  }

  function IndicesEnsurerJob (storagedescriptor, collection, defer) {
    JobBase.call(this, defer);
    this.storagedescriptor = storagedescriptor;
    this.collection = collection;
    this.needed = null;
  }
  lib.inherit(IndicesEnsurerJob, JobBase);
  IndicesEnsurerJob.prototype.destroy = function () {
    this.needed = null;
    this.collection = null;
    this.storagedescriptor = null;
    JobBase.prototype.destroy.call(this);
  };
  IndicesEnsurerJob.prototype.go = function () {
    var ok = this.okToGo();
    if (!ok.ok) {
      return ok.val;
    }
    this.calculateNeededIndicesAndEnsure();
    return ok.val;
  };
  IndicesEnsurerJob.prototype.calculateNeededIndicesAndEnsure = function () {
    this.needed = this.interpretIndexSpecification(this.storagedescriptor.indices);
    this.checkForAuto();
    this.ensure();
  };
  IndicesEnsurerJob.prototype.interpretIndexSpecification = function (specs) {
    if (!lib.isArray(specs)) {
      return [];
    }
    return specs.reduce(indexSpecInterpreter, []);
  };
  IndicesEnsurerJob.prototype.checkForAuto = function () {
    if (!(lib.isVal(this.storagedescriptor) && lib.isVal(this.storagedescriptor.record) && lib.isArray(this.storagedescriptor.record.fields))) {
      return;
    }
    this.storagedescriptor.record.fields.forEach(this.autoCheckerOnField.bind(this));
  };
  IndicesEnsurerJob.prototype.autoCheckerOnField = function (field) {
    var indextype, indexobj;
    if (!(lib.isVal(field) && field.type)) {
      return;
    }
    indextype = _autoIndexedTypes[field.type];
    if (!indextype) {
      return;
    }
    indexobj = {};
    indexobj[field.name] = indextype;
    this.needed.push({key:indexobj});
  };
  IndicesEnsurerJob.prototype.ensure = function () {
    //console.log('have to ensure indices:', this.needed);
    if (!this.collection) {
      this.reject(new lib.Error('NO_COLLECTION', 'There is no MongoDB collection to createIndexes on'));
      return;
    }
    if (!lib.isFunction(this.collection.createIndexes)) {
      this.reject(new lib.Error('NOT_A_COLLECTION', 'The collection provided is not a MongoDB collection at all'));
      return;
    }
    this.collection.createIndexes(this.needed, this.onCreateIndexes.bind(this));
  };
  IndicesEnsurerJob.prototype.onCreateIndexes = function (err, res) {
    /*
    console.log('onCreateIndexes');
    console.log('err', err);
    console.log('result', res);
    */
    if (err) {
      if (!(err.code === 85)) { //support for Mongo >= 4.2, existing keys are reported as an Error
        this.reject(err);
        return;
      }
    }
    this.resolve(true);
  };


  function ensureIndices (storagedescriptor, collection) {
    //return q(true);
    return (new IndicesEnsurerJob(storagedescriptor, collection)).go();
  }

  return ensureIndices;
}
module.exports = createIndexEnsurer;
