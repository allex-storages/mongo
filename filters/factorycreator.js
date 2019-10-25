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
      throw new lib.Error('FILTER_NOT_IMPLEMENTED', 'No Filter factory for operator "'+op+'"');
      return null;
    }
    return fn(filterdescriptor, options);
  };

  var factory = new Factory();
  factory.add('eq', require('./eqcreator')(execlib, fieldValue));
  factory.add('gte', require('./gtecreator')(execlib, fieldValue));
  factory.add('gt', require('./gtcreator')(execlib, fieldValue));
  factory.add('lte', require('./ltecreator')(execlib, fieldValue));
  factory.add('lt', require('./ltcreator')(execlib, fieldValue));
  factory.add('in', require('./increator')(execlib));
  factory.add('nin', require('./nincreator')(execlib));
  factory.add('contains', require('./containscreator')(execlib));
  factory.add('startswith', require('./startswithcreator')(execlib));
  factory.add('endswith', require('./endswithcreator')(execlib));
  factory.add('exists', require('./existscreator')(execlib));
  factory.add('notexists', require('./notexistscreator')(execlib));
  factory.add('or', require('./orcreator')(execlib, factory));
  factory.add('and', require('./andcreator')(execlib, factory));
  factory.add('near', require('./nearcreator')(execlib, factory));

  return factory;
}

module.exports = createFilterFactory;
