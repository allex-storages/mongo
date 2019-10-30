var _test1 = {
  storagedescriptor: {
    fields: [{
    name: 'username',
    type: 'string'
    }]
  },
  records: [{
    allex2mongo: [{
      username: 'andra'
    },{
      username: 'andra'
    }],
    mongo2allex: [{
      username: 'andra'
    },{
      username: 'andra'
    }]
  }]
};
 
function doTest (testobj, bridgename) {
  bridgename = bridgename||'Bridge';
  it('Create Bridge1', function () {
    return setGlobal(bridgename, new BridgeKlass(testobj.storagedescriptor));
  });
  testobj.records.forEach(function (testrecobj, index) {
    it('Testing record '+index+' allex2mongo', function () {
      expect(getGlobal(bridgename).allex2mongo(testrecobj.allex2mongo[0])).to.deep.equal(testrecobj.allex2mongo[1]);
    });
    it('Testing record '+index+' mongo2allex', function () {
      expect(getGlobal(bridgename).mongo2allex(testrecobj.mongo2allex[0])).to.deep.equal(testrecobj.mongo2allex[1]);
    });
  });
}

describe('Test Bridge', function () {
  it('Load Bridge', function () {
    return setGlobal('BridgeKlass', require('../bridge')(execlib));
  });
  doTest(_test1);
});
