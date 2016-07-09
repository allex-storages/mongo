function createFieldValue(ObjectID) {
  function fieldValue(val, name, options) {
    if (options && '_id' === name && options._nativeid) {
      return new ObjectID(val);
    }
    return val;
  }

  return fieldValue;
}

module.exports = createFieldValue;
