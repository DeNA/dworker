'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Agent = require('..').Agent;
var Promise = require('bluebird');
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');
var sinon = require('sinon');

describe('Broker tests', function () {
    var br;
    var brId = 'br01';
    var sandbox;

    before(function () {
        sandbox = sinon.sandbox.create();
    });

    after(function () {
    });

    beforeEach(function () {
        br = new Broker(brId, {
            retries: {
                initialInterval: 20,
                maxInterval: 100,
                duration: 200
            }
        });
        br.on('log', function (data) {
            debug('LOG(' + data.level + '): ' + data.message);
        });
    });

    afterEach(function () {
        sandbox.restore();
        return br.destroy({ noRecover: true });
    });

    it('#start fails when the state is ACTIVATING', function () {
        br._state = Broker.State.ACTIVATING;
        return br.start()
        .then(function () {
            throw new Error('Should throw');
        }, function (err) {
            assert.equal(err.name, 'Error');
            br._state = Broker.State.INACTIVE;
        });
    });

    it('#start fails when br._pub is null', function () {
        var pub = br._pub;
        br._pub = null;
        return br.start()
        .then(function () {
            throw new Error('Should throw');
        }, function (err) {
            assert.equal(err.name, 'Error');
            br._pub = pub;
        });
    });

    it('#start fails when br._sub is null', function () {
        var sub = br._sub;
        br._sub = null;
        return br.start()
        .then(function () {
            throw new Error('Should throw');
        }, function (err) {
            assert.equal(err.name, 'Error');
            br._sub = sub;
        });
    });

    it('#start fails when Router#listen returns different IP address', function () {
        sandbox.stub(br._router, 'listen', function (inAddr) {
            debug('Router#listen arg=' + inAddr);
            return Promise.resolve({address: '0.0.0.0'});
        });
        return br.start()
        .then(function () {
            throw new Error('Should throw');
        }, function (err) {
            assert.equal(err.name, 'Error');
        });
    });

    it('#start fails addBroker script returns null', function () {
        sandbox.stub(br._pub, 'evalshaAsync', function () {
            return Promise.resolve(null);
        });
        return br.start()
        .then(function () {
            throw new Error('Should throw');
        }, function (err) {
            assert.equal(err.name, 'Error');
        });
    });

    it('#start fails addBroker script returns {}', function () {
        sandbox.stub(br._pub, 'evalshaAsync', function () {
            return Promise.resolve({});
        });
        return br.start()
        .then(function () {
            throw new Error('Should throw');
        }, function (err) {
            debug('Error message: ' + err.message);
            assert.equal(err.name, 'Error');
        });
    });

    it('#start fails addBroker script returns other than 0', function () {
        sandbox.stub(br._pub, 'evalshaAsync', function () {
            return Promise.resolve([1]);
        });
        return br.start()
        .then(function () {
            throw new Error('Should throw');
        }, function (err) {
            debug('Error message: ' + err.message);
            assert.equal(err.name, 'Error');
        });
    });

    describe('#createWorker error tests', function () {
        function MyWorker() {
            Worker.apply(this, arguments);
        }
        util.inherits(MyWorker, Worker);

        MyWorker.prototype.onCreate = function (info, cb) {
            debug('Worker#onCreate called, cause=' + info.cause);
            cb();
        };
        MyWorker.prototype.onAsk = function (method, data) {
            void(data);
            debug('Worker#onAsk called (method=' + method + ')');
            return Promise.resolve();
        };
        MyWorker.prototype.onTell = function (method, data) {
            void(data);
            debug('Worker#onTell called (method=' + method + ')');
        };

        function MyAgent() {
            Worker.apply(this, arguments);
        }
        util.inherits(MyAgent, Agent);

        beforeEach(function () {
            return br.start();
        });

        it('#createWorker fails if the worker does not exist', function () {
            return br.createWorker('None')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#createWorker fails if getLeastLoadedBroker returns null', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve(null);
            });
            return br.createWorker('MyWorker')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#createWorker fails if getLeastLoadedBroker returns {}', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve({});
            });
            return br.createWorker('MyWorker')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#createWorker fails if getLeastLoadedBroker returns [] with first arg being non-string', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([3, 'sigh']);
            });
            return br.createWorker('MyWorker')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#createWorker fails if getLeastLoadedBroker returns [] with second item being non-string', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve(['sigh', 4]);
            });
            return br.createWorker('MyWorker')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#createWorker succeeds with custom Agent class', function () {
            MyWorker.agent = MyAgent;
            br.registerWorker(MyWorker);
            return br.createWorker('MyWorker')
            .then(function (agent) {
                assert.ok(agent instanceof MyAgent);
            });
        });

        it('#createWorker succeeds with custom Agent being the base Agent', function () {
            MyWorker.agent = Agent;
            br.registerWorker(MyWorker);
            return br.createWorker('MyWorker')
            .then(function (agent) {
                assert.strictEqual(agent.constructor, Agent);
            });
        });

    });

    describe('#findWorker error tests', function () {
        function MyWorker() {
            Worker.apply(this, arguments);
        }
        util.inherits(MyWorker, Worker);

        MyWorker.prototype.onCreate = function (info, cb) {
            debug('Worker#onCreate called, cause=' + info.cause);
            cb();
        };
        MyWorker.prototype.onAsk = function (method, data) {
            void(data);
            debug('Worker#onAsk called (method=' + method + ')');
            return Promise.resolve();
        };
        MyWorker.prototype.onTell = function (method, data) {
            void(data);
            debug('Worker#onTell called (method=' + method + ')');
        };

        function MyAgent() {
            Worker.apply(this, arguments);
        }
        util.inherits(MyAgent, Agent);

        beforeEach(function () {
            return br.start();
        });

        it('#findWorker succeeds with a custom Agent', function () {
            MyWorker.agent = MyAgent;
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0, ['br01', 'MyWorker', 'MyWorker#1']]);
            });
            return br.findWorker('MyWorker')
            .then(function (agent) {
                assert.ok(agent instanceof MyAgent);
            });
        });
        it('#findWorker fails if findWorker.lua returns null', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve(null);
            });
            return br.findWorker('MyWorker')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#findWorker fails if findWorker.lua returns res[1] being non-array', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0, {}]);
            });
            return br.findWorker('MyWorker')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#findWorker fails if findWorker.lua returns unknown worker name', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0, ['br01', 'BadWorker', 'BadWorker#1']]);
            });
            return br.findWorker('MyWorker')
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
            });
        });

        it('#findWorker returns null if findWorker.lua returns res[0] being 2', function () {
            br.registerWorker(MyWorker);
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([2]);
            });
            return br.findWorker('MyWorker')
            .then(function (res) {
                assert.strictEqual(res, null);
            });
        });
    });

    describe('#destory error tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('fail when the state is ACTIVATING', function () {
            br._state = Broker.State.ACTIVATING;
            return br.destroy()
            .then(function () {
                throw new Error('Should fail');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
                br._state = Broker.State.ACTIVE;
            });
        });

        it('fail when the state is DESTROYING', function () {
            br._state = Broker.State.DESTROYING;
            return br.destroy()
            .then(function () {
                throw new Error('Should fail');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
                br._state = Broker.State.ACTIVE;
            });
        });

        it('fail when the state is DESTROYED', function () {
            br._state = Broker.State.DESTROYED;
            return br.destroy()
            .then(function () {
                throw new Error('Should fail');
            }, function (err) {
                debug('Error message: ' + err.message);
                assert.equal(err.name, 'Error');
                br._state = Broker.State.ACTIVE;
            });
        });
    });

    describe('#quit tests', function () {
        it('If _pub is null, then quit() will not be called', function () {
            var subQuitCalled = false;
            var fake = {
                _pub: null,
                _sub: {
                    connected: true,
                    quit: function () { subQuitCalled = true; }
                }
            };

            Broker.prototype.quit.call(fake);
            assert.ok(subQuitCalled);
        });
        it('If _pub is null, then quit() will not be called', function () {
            var pubQuitCalled = false;
            var fake = {
                _pub: {
                    connected: true,
                    quit: function () { pubQuitCalled = true; }
                },
                _sub: null
            };

            Broker.prototype.quit.call(fake);
            assert.ok(pubQuitCalled);
        });
        it('If not connected, quit() will not be called', function () {
            var pubQuitCalled = false;
            var subQuitCalled = false;
            var fake = {
                _pub: {
                    connected: false,
                    quit: function () { pubQuitCalled = true; }
                },
                _sub: {
                    connected: false,
                    quit: function () { subQuitCalled = true; }
                }
            };

            Broker.prototype.quit.call(fake);
            assert.ok(!pubQuitCalled);
            assert.ok(!subQuitCalled);
        });
    });

    describe('#_loadScripts tests', function () {
        it('Should throw when loading of script fails', function () {
            var lured = require('lured');
            sandbox.stub(lured, 'create', function () {
                return {
                    load: function (cb) {
                        cb(new Error('fake error'));
                    }
                };
            });

            return br._loadScripts()
            .then(function () {
                throw new Error('Should throw');
            }, function (err) {
                assert.equal(err.name, 'Error');
            });
        });
    });

    describe('#_findBroker tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('Should return null invalidate cache if findBroker script returns null', function () {
            var workerId = 'abc';
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve(null);
            });
            var spyCacheDel = sandbox.spy(br._brokerCache, 'del');

            return br._findBroker(workerId)
            .then(function (res) {
                assert.strictEqual(res, null);
                assert.ok(spyCacheDel.calledOnce);
            });
        });

        it('return null, if findBroker script returns non-array', function () {
            var workerId = 'abc';
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve({});
            });
            var spyCacheDel = sandbox.spy(br._brokerCache, 'del');

            return br._findBroker(workerId)
            .then(function (res) {
                assert.strictEqual(res, null);
                assert.ok(spyCacheDel.calledOnce);
            });
        });

        it('return null, if findBroker script returns first item being non-number', function () {
            var workerId = 'abc';
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve(['hi']);
            });
            var spyCacheDel = sandbox.spy(br._brokerCache, 'del');

            return br._findBroker(workerId)
            .then(function (res) {
                assert.strictEqual(res, null);
                assert.ok(spyCacheDel.calledOnce);
            });
        });

        it('return null, if findBroker script returns first item being other than 0', function () {
            var workerId = 'abc';
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([1]);
            });
            var spyCacheDel = sandbox.spy(br._brokerCache, 'del');

            return br._findBroker(workerId)
            .then(function (res) {
                assert.strictEqual(res, null);
                assert.ok(spyCacheDel.calledOnce);
            });
        });

        it('return null, if findBroker script returns second item being other than an array', function () {
            var workerId = 'abc';
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0], {});
            });
            var spyCacheDel = sandbox.spy(br._brokerCache, 'del');

            return br._findBroker(workerId)
            .then(function (res) {
                assert.strictEqual(res, null);
                assert.ok(spyCacheDel.calledOnce);
            });
        });

        it('Should not set cache if findBroker script returns res[1][2] is not "active"', function () {
            var workerId = 'abc';
            var addr = { host: '127.0.0.2', port: 1234 };
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0, ['arg0', 'arg1', 'invalid', addr.host+':'+addr.port]]);
            });
            var spyCacheDel = sandbox.spy(br._brokerCache, 'del');
            var spyCacheSet = sandbox.spy(br._brokerCache, 'set');

            return br._findBroker(workerId)
            .then(function (res) {
                assert.strictEqual(res.brokerId, 'arg0');
                assert.strictEqual(res.clustername, 'arg1');
                assert.strictEqual(res.status, 'invalid');
                assert.deepEqual(res.address, addr);
                assert.ok(!spyCacheDel.called);
                assert.ok(!spyCacheSet.calledOnce);
            });
        });
    });

    describe('#_recoverBroker tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('Adds option.cause before calling createWorker', function () {
            var workerName = 'TestWorker';
            sandbox.stub(br, 'createWorker', function (name, option) {
                return Promise.resolve()
                .then(function () {
                    assert.strictEqual(name, workerName);
                    assert.strictEqual(option.cause, Worker.CreateCause.RECOVERY);
                });
            });

            return br._recoverWorker(workerName, {});
        });
    });

    describe('#_onSubMessage tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('Invalid json data should not throw', function () {
            var chId = 'mych';
            assert.doesNotThrow(function () {
                br._onSubMessage(chId, "{ badbad");
            }, function (err) {
                assert.ifError(err);
            }, 'Should not throw');
        });

        it('Sigsalvage should succeed', function () {
            var chId = 'mych';
            sandbox.stub(br, '_onSigSalvage', function (cname, brId) {
                return Promise.resolve()
                .then(function () {
                    assert.strictEqual(cname, 'c1');
                    assert.strictEqual(brId, br.id);
                });
            });
            br._onSubMessage(chId, JSON.stringify({
                sig: "salvage",
                clustername: 'c1',
                brokerId: br.id
            }));
        });

        it('Unknown signal simply ignored', function () {
            var chId = 'mych';
            br._onSubMessage(chId, JSON.stringify({
                sig: "supernova",
                clustername: 'c1',
                brokerId: br.id
            }));
        });
    });

    describe('#_onSigRecover tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('If null is returned, it should be out of recovering state', function (done) {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve(null);
            });
            br._onSigRecover();
            setTimeout(function () {
                assert.ok(!br._isRecovering);
                assert.ok(!br._needsRecovery);
                done();
            }, 20);
        });

        it('If non-array is returned, it should be out of recovering state', function (done) {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve({});
            });
            br._onSigRecover();
            setTimeout(function () {
                assert.ok(!br._isRecovering);
                assert.ok(!br._needsRecovery);
                done();
            }, 20);
        });

        it('If the first item is not an array, it should be out of recovering state', function (done) {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([]);
            });
            br._onSigRecover();
            setTimeout(function () {
                assert.ok(!br._isRecovering);
                assert.ok(!br._needsRecovery);
                done();
            }, 20);
        });

        it('If no worker to recover, it should be out of recovering state', function (done) {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([[]]);
            });
            br._onSigRecover();
            setTimeout(function () {
                assert.ok(!br._isRecovering);
                assert.ok(!br._needsRecovery);
                done();
            }, 20);
        });

        it('Error from _recoverWorker should not affect the recovery loop (it should be ignored)', function (done) {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([[JSON.stringify({
                    id: 'myid',
                    name: 'myname',
                    attributes: { static: false }
                })]]);
            });
            sandbox.stub(br, '_recoverWorker', function () {
                return Promise.reject(new Error('This should be ignored'));
            });
            br._onSigRecover();
            setTimeout(function () {
                assert.ok(!br._isRecovering);
                assert.ok(!br._needsRecovery);
                done();
            }, 20);
        });

        it('Remaining > 0 will fetch item again', function (done) {
            var results = [
                [[JSON.stringify({ id: 'wk01', name: 'MyWorker', attributes: { static: false } })], 1],
                [[JSON.stringify({ id: 'wk02', name: 'MyWorker', attributes: { static: false } })], 0]
            ];
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                debug('results.length=' + results.length);
                return Promise.resolve(results.shift());
            });
            sandbox.stub(br, '_recoverWorker', function (name) {
                if (name === 'wk01') {
                    return Promise.reject(new Error('fake error'));
                }
                return Promise.resolve();
            });
            br._onSigRecover();
            setTimeout(function () {
                assert.ok(!br._isRecovering);
                assert.ok(!br._needsRecovery);
                done();
            }, 100);
        });
    });

    describe('#_onBrokerRequest tests', function () {
        beforeEach(function () {
            return br.start();
        });
        afterEach(function () {
            delete br.hahaha;
        });

        it('Non-existing method call', function (done) {
            var requesterId = 777;
            sandbox.stub(br, '_replyToBroker', function (seq, pl, reqId) {
                void(seq);
                assert.equal(pl.err.name, 'Error');
                assert.strictEqual(reqId, requesterId);
                done();
            });
            br._onBrokerRequest({m: 'hahaha'}, requesterId);
        });

        it('Broker method error correctly returned', function (done) {
            var requesterId = 778;
            br.hahaha = function () {};
            sandbox.stub(br, 'hahaha', function (data) {
                void(data);
                return Promise.reject(new Error('failed, hahaha'));
            });
            sandbox.stub(br, '_replyToBroker', function (seq, pl, reqId) {
                void(seq);
                assert.equal(pl.err.name, 'Error');
                assert.strictEqual(reqId, requesterId);
                done();
            });
            br._onBrokerRequest({m: 'hahaha', pl: {}}, requesterId);
        });
    });

    describe('#_onBrokerResponse tests', function () {
        beforeEach(function () {
            return br.start();
        });
        afterEach(function () {
            delete br.hahaha;
        });

        it('data no seq should fail', function () {
            var success = br._onBrokerResponse({pl: {}});
            assert.ok(!success);
        });

        it('data with no method and pl should fail', function () {
            var success = br._onBrokerResponse({seq: 1234});
            assert.ok(!success);
        });

        it('no cb in #_cbs[] should be ignored', function () {
            var success = br._onBrokerResponse({seq: 1234, pl:{}});
            assert.ok(success);
        });

        it('data.pl.err should be reported as an error via cb', function (done) {
            br._cbs[1234] = function (err) {
                assert.ok(err);
                assert.equal(err.name, 'Error');
                assert.equal(err.message, 'bam!');
                done();
            };
            br._onBrokerResponse({seq: 1234, pl:{err: {name:'Error', message: 'bam!'}}});
        });
    });

    describe('#_onWorkerRequest tests', function () {
        beforeEach(function () {
            return br.start();
        });
        afterEach(function () {
            delete br.hahaha;
        });

        it('Worker request with no method property should fail', function () {
            var ok = br._onWorkerRequest({ wid: 33 });
            assert(!ok);
        });

        it('Worker not in ACTIVE should fail', function () {
            br._workers[33] = { state: Worker.State.ACTIVATING };
            sandbox.stub(br, '_replyToWorker', function (seq, pl, src, wid) {
                void(seq);
                void(src);
                void(wid);
                assert.ok(pl.err);
                assert.equal(pl.err.name, 'Error');
                assert.equal(pl.err.message, 'Error: Worker is not active');
            });
            var ok = br._onWorkerRequest({m: 'hahaha', wid: 33});
            assert(!ok);
        });
    });

    describe('#_onWorkerResponse tests', function () {
        it('data with no seq should fail', function () {
            var ok = br._onWorkerResponse({pl: {}});
            assert(!ok);
        });

        it('data with null pl should fail', function () {
            var ok = br._onWorkerResponse({m: 'doIt', seq: 123, pl: null});
            assert(!ok);
        });

        it('data with pl being other than object should fail', function () {
            var ok = br._onWorkerResponse({m: 'doIt', seq: 123, pl: 'duh'});
            assert(!ok);
        });

        it('no cb is #_cbs[] should be ignored', function () {
            return br._onWorkerResponse({seq: 1234, pl:{}});
        });

        it('data.pl.err should be reported as an error via cb', function (done) {
            br._cbs[1234] = function (err) {
                assert.ok(err);
                assert.equal(err.name, 'Error');
                assert.equal(err.message, 'bam!');
                done();
            };
            var ok = br._onWorkerResponse({seq: 1234, pl:{err: {name:'Error', message: 'bam!'}}});
            assert(ok);
        });
    });

    describe('#_request tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('#_request with a bad json-object', function () {
            var data = {};
            data.circular = data;
            return br._request(data, void(0))
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
                debug('err:', err);
            });
        });

        it('#_request should fail when writeQueue.lua returns non-array', function () {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve({} /* bad */);
            });
            return br._request({}, "Broker#123")
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
                debug('err:', err);
            });
        });

        it('#_request should fail when writeQueue.lua returns [1]', function () {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([1]);
            });
            return br._request({}, "Broker#123")
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
                debug('err:', err);
            });
        });

        it('#_request should fail when writeQueue.lua returns [n] where n > 1', function () {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([2]);
            });
            return br._request({}, "Broker#123")
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
                debug('err:', err);
            });
        });
    });

    describe('#_onCreateWorker tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('#_onCreateWorker should fail when non-array is returned', function () {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve({});
            });
            return br._onCreateWorker({})
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
                debug('err:', err);
            });
        });

        it('#_onCreateWorker should fail with res[1] being non-array', function () {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0, {}]);
            });
            return br._onCreateWorker({})
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
                debug('err:', err);
            });
        });

        it('#_onCreateWorker should fail when [n] is returned, where n!=0', function () {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([1]);
            });
            return br._onCreateWorker({})
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
                debug('err:', err);
            });
        });

        it('#_onCreateWorker should ignore rejection by Worker#onCreate', function () {
            function MyWorker() {
                Worker.apply(this, arguments);
            }
            util.inherits(MyWorker, Worker);
            MyWorker.prototype.onCreate = function () {
                return Promise.reject(new Error('Fake error'));
            };
            br.registerWorker(MyWorker);

            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0, [br.id, 'MyWorker', 'MyWorker#123']]);
            });
            return br._onCreateWorker({name:'MyWorker', attributes: {}})
            .then(function (res) {
                assert.strictEqual(res.brokerId, br.id);
                assert.ok(br._workers[res.workerId]);
            });
        });

        it('If createWorker.lua returns other broker, that will be returned', function () {
            var otherId = 'dworker-rules!';
            function MyWorker() {
                Worker.apply(this, arguments);
            }
            util.inherits(MyWorker, Worker);
            MyWorker.prototype.onCreate = function () {};
            br.registerWorker(MyWorker);

            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0, [otherId, 'MyWorker', 'MyWorker#123']]);
            });
            return br._onCreateWorker({name:'MyWorker', attributes: {}})
            .then(function (res) {
                assert.strictEqual(res.brokerId, otherId);
                assert.ok(!br._workers[res.workerId]);
            });
        });
    });

    describe('#_destroyWorker tests', function () {
        function MyWorker() {
            Worker.apply(this, arguments);
        }
        util.inherits(MyWorker, Worker);
        MyWorker.prototype.onCreate = function () {
            return Promise.reject(new Error('Fake error'));
        };

        var worker;

        beforeEach(function () {
            br.registerWorker(MyWorker);
            return br.start()
            .then(function () {
                return br.createWorker('MyWorker')
                .then(function (agent) {
                    worker = br._workers[agent.id];
                });
            });
        });

        it('#_destroyWorker successfully destroy the worker.', function (done) {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve({});
            });
            sandbox.stub(worker, 'onDestroy', function () {
                setImmediate(function () {
                    assert.ok(!br._workers[worker.id]);
                    assert.equal(worker.state, Worker.State.DESTROYED);
                    done();
                });
                return Promise.resolve();
            });
            br._destroyWorker(worker); // this won't return promise
        });

        it('#_destroyWorker successfully destroy the worker.', function (done) {
            sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.reject(new Error('To be ignored'));
            });
            sandbox.stub(worker, 'onDestroy', function () {
                setImmediate(function () {
                    assert.ok(!br._workers[worker.id]);
                    assert.equal(worker.state, Worker.State.DESTROYED);
                    done();
                });
                return Promise.resolve();
            });
            br._destroyWorker(worker); // this won't return promise
        });
    });

    describe('#_updateLoad tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('#_destroyWorker successfully destroy the worker.', function (done) {
            var initialLoad = br._load;
            var delta = 3;
            sandbox.stub(br._pub, 'zadd', function (key, score, member, cb) {
                void(key);
                assert.equal(score, initialLoad + delta);
                assert.equal(member, br.id);
                assert.ok(br._loadUpdated);
                cb(new Error('fake error')); // eslint-disable-line callback-return
                done();
            });
            br._updateLoad({/* not used */}, delta); // this won't return promise
        });
    });

    describe('#_syncRedisTime tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('Failure of time command should not affect anything.', function (done) {
            var initialTimeOffset = br._timeOffset;
            var initialLastTimeSync = br._lastTimeSync;
            sandbox.stub(br._pub, 'time', function (cb) {
                cb(new Error('fake error')); // eslint-disable-line callback-return
                assert.strictEqual(br._timeOffset, initialTimeOffset);
                assert.strictEqual(br._lastTimeSync, initialLastTimeSync);
                done();
            });
            br._syncRedisTime(Date.now()); // this won't return promise
        });
    });

    describe('#_onTimer tests', function () {
        beforeEach(function () {
            return br.start();
        });

        it('Should call #_syncRedisTime after TIME_SYNC_INTERVAL', function (done) {
            var now = Date.now();
            br._lastTimeSync = now - 30000 - 2;
            var stub = sandbox.stub(br, '_syncRedisTime', function () {
                // do nothing
            });
            sandbox.stub(global, 'setTimeout', function () {
                debug('setTimeout called');
                assert.ok(stub.calledOnce);
                return void(done());
            });
            br._onTimer();
        });

        it('Timed out RPC callbacks should be removed', function (done) {
            var now = Date.now();
            var res = [ false, false, false, false ];
            var rpcTimeoutCb = function (seq) {
                res[seq] = true;
            };

            br._cbList = [
                [ now - br.rpcTimeout - 10, 0 ],
                [ now - br.rpcTimeout - 5, 1 ],
                [ now - br.rpcTimeout + 5, 2 ],
                [ now - br.rpcTimeout + 10, 3 ]
            ];

            br._cbs = {
                0: rpcTimeoutCb.bind(null, 0),
                1: rpcTimeoutCb.bind(null, 1),
                2: rpcTimeoutCb.bind(null, 2),
                3: rpcTimeoutCb.bind(null, 3)
            };

            sandbox.stub(br, '_syncRedisTime', function () {
                // do nothing
            });
            sandbox.stub(global, 'setTimeout', function () {
                debug('setTimeout called');
                debug('res:', res);
                assert.deepEqual(res, [true, true, false, false]);
                return void(done());
            });
            br._onTimer();
        });
    });
});

