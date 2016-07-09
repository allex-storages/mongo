function createFilterFactory(execlib, ObjectID) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    fieldValue = require('./fieldvaluecreator')(ObjectID);

  function Factory(){
    lib.Map.call(this);
  }
  lib.inherit(Factory,lib.Map);
  Factory.prototype.createFromDescriptor = function(filterdescriptor, options){
    if(!filterdescriptor){
      return [{}];
    }
    var op = filterdescriptor.op;
    if(!op){
      return [{}];
    }
    var fn = this.get(op);
    if(!fn){
      console.log('No Filter factory for operator "'+op+'"');
      return null;
    }
    return fn(filterdescriptor, options);
  };

  var factory = new Factory();
  factory.add('eq', require('./eqcreator')(execlib, fieldValue));
  factory.add('in', require('./increator')(execlib));
  factory.add('contains', require('./containscreator')(execlib));
  factory.add('or', require('./orcreator')(execlib, factory));
  factory.add('and', require('./andcreator')(execlib, factory));

  return factory;
}

module.exports = createFilterFactory;
