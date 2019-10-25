var _storagedescriptor1 = [{
  name: 'username'
}]


describe('Test Bridge', function () {
  it('Load Bridge', function () {
    return setGlobal('BridgeKlass', require('../bridge')(execlib));
  });
  it('Create Bridge1', function () {
    return setGlobal('Bridge1', new BridgeKlass(_storagedescriptor1));
  });
});
