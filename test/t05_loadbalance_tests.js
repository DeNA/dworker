'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');

describe('Load-balance tests', function () {
    var br;
    var brId = 'br01';
    var client;
    var agent;
    var cz = 'dw:cz:main';

    function MyWorker() {
        Worker.apply(this, arguments);
    }
    util.inherits(MyWorker, Worker);

    MyWorker.prototype.onCreate = function (info, cb) {
        debug('Worker#onCreate called, cause=' + info.cause);
        return Worker.prototype.onCreate.call(this, info, cb);
    };

    MyWorker.prototype.onDestroy = function (info, cb) {
        void(info);
        debug('Worker#onDestroy called');
        return Promise.resolve().nodeify(cb);
    };

    MyWorker.prototype.onAsk = function (method, data) {
        debug('Worker#onAsk called (method=' + method + ')');
        if (method === 'updateLoad') {
            this.load = data.load;
            return Promise.resolve({ success: true });
        }
        else if (method === 'destroy') {
            debug('calling destroy() on worker');
            this.destroy();
            return Promise.resolve();
        }
        return Promise.reject(new Error('Unexpected method: ' + method));
    };
    MyWorker.prototype.onTell = function (method, data) {
        void(data);
        debug('Worker#onTell called (method=' + method + ')');
        assert(!'Unexpected method: ' + method);
    };

    before(function () {
        br = new Broker(brId);
        client = br.pub;
        br.registerWorker(MyWorker);
        return br._pub.hdelAsync(br._keys.bh, br.id)
        .then(function () {
            return br.start();
        });
    });

    after(function () {
        return br.destroy({ noRecover: true });
    });

    it('Initial load should be zero', function (done) {
        client.zscore(cz, brId, function (err, load) {
            assert.ifError(err);
            assert.strictEqual(typeof load, 'string');
            assert.strictEqual(load, '0');
            done();
        });
    });

    it('New worker should increase load by 1', function () {
        return br.createWorker('MyWorker')
        .then(function (_agent) {
            agent = _agent;
            client.zscore(cz, brId, function (err, load) {
                assert.ifError(err);
                assert.strictEqual(load, '1');
            });
        });
    });

    it('Load increase should change the load on redis', function () {
        return agent.ask('updateLoad', { load: 10 })
        .then(function () {
            return new Promise(function (resolve, reject) {
                client.zscore(cz, brId, function (err, load) {
                    if (err) {
                        return reject(err);
                    }
                    assert.strictEqual(load, '10');
                    resolve();
                });
            });
        });
    });

    it('Load decrease should change the load on redis', function () {
        return agent.ask('updateLoad', { load: 2 })
        .then(function () {
            return new Promise(function (resolve, reject) {
                client.zscore(cz, brId, function (err, load) {
                    if (err) {
                        return reject(err);
                    }
                    assert.strictEqual(load, '2');
                    resolve();
                });
            });
        });
    });

    it('Destroy worker should set load to zero', function () {
        // Make sure the worker now has default load 1.
        return new Promise(function (resolve, reject) {
            client.zscore(cz, brId, function (err, load) {
                if (err) {
                    return reject(err);
                }
                assert.strictEqual(load, '2');
                resolve();
            });
        })
        // then destroy
        .then(function () {
            debug('ask: destroy ----------------------------');
            return agent.ask('destroy', {});
        })
        .then(function () {
            debug('ask: destroy done -----------------------');
            return new Promise(function (resolve, reject) {
                client.zscore(cz, brId, function (err, load) {
                    if (err) {
                        return reject(err);
                    }
                    assert.strictEqual(load, '0');
                    resolve();
                });
            });
        });
    });
});
