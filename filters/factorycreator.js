function createFilterFactory(execlib) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q;

  function Factory(){
    lib.Map.call(this);
  }
  lib.inherit(Factory,lib.Map);
  Factory.prototype.createFromDescriptor = function(filterdescriptor){
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
    return fn(filterdescriptor);
  };

  var factory = new Factory();
  factory.add('eq', require('./eqcreator')(execlib));
  factory.add('in', require('./increator')(execlib));

  return factory;
}

module.exports = createFilterFactory;
