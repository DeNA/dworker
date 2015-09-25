'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');

describe('findBroker tests', function () {
    var br;
    var brId = 'br01';

    function MyWorker() {
        Worker.apply(this, arguments);
    }
    util.inherits(MyWorker, Worker);

    MyWorker.prototype.onCreate = function (info) {
        debug('Worker#onCreate called, cause=' + info.cause);
        return Promise.resolve();
    };

    MyWorker.prototype.onAsk = function (method, data) {
        void(data);
        debug('Worker#onAsk called (method=' + method + ')');
        return Promise.resolve();
    };
    MyWorker.prototype.onTell = function (method, data) {
        void(data);
        debug('Worker#onTell called (method=' + method + ')');
        return Promise.resolve();
    };

    before(function () {
        br = new Broker(brId);
        br.on('log', function (data) {
            debug('LOG(' + data.level + '): ' + data.message);
        });
        return br.start();
    });

    after(function () {
        br.destroy();
    });

    it('Null is returned when no worker', function () {
        return br._findBroker('dumb')
        .then(function (res) {
            assert.strictEqual(res, null);
        });
    });

    it('Successfully find broker ID', function () {
        br.registerWorker(MyWorker);
        return br.createWorker('MyWorker')
        .then(function (agent) {
            assert.strictEqual(typeof agent, 'object');

            br._brokerCacheHits = 0;
            br._brokerCacheMisses = 0;

            return br._findBroker(agent.id);
        })
        .then(function (res) {
            assert.strictEqual(typeof res, 'object');
            assert.strictEqual(res.brokerId, br.id);
            assert.strictEqual(res.clustername, 'main');
            assert.strictEqual(res.status, 'active');
            assert.strictEqual(br._brokerCacheHits, 0);
            assert.strictEqual(br._brokerCacheMisses, 1);
        });
    });

    it('find broker ID via broker cache', function () {
        var agent;
        br.registerWorker(MyWorker);
        return br.createWorker('MyWorker')
        .then(function (_agent) {
            agent = _agent;

            assert.strictEqual(typeof agent, 'object');

            br._brokerCacheHits = 0;
            br._brokerCacheMisses = 0;

            return br._findBroker(agent.id);
        })
        .then(function (res) {
            assert.strictEqual(typeof res, 'object');
            assert.strictEqual(res.brokerId, br.id);
            assert.strictEqual(res.clustername, 'main');
            assert.strictEqual(res.status, 'active');
            assert.strictEqual(br._brokerCacheHits, 0);
            assert.strictEqual(br._brokerCacheMisses, 1);

            return br._findBroker(agent.id);
        })
        .then(function (res) {
            assert.strictEqual(typeof res, 'object');
            assert.strictEqual(res.brokerId, br.id);
            assert.strictEqual(res.clustername, 'main');
            assert.strictEqual(res.status, 'active');
            assert.strictEqual(br._brokerCacheHits, 1);
            assert.strictEqual(br._brokerCacheMisses, 1);
        });
    });

    it('Null is return if worker entry in wh is corrupted', function () {
        br.registerWorker(MyWorker);
        return br.createWorker('MyWorker')
        .then(function (agent) {
            assert.strictEqual(typeof agent, 'object');

            // Corrupt the worker entry
            return br.pub.hsetAsync(
                br._keys.wh,
                agent.id,
                '{"attributes":{"static":false,"brokerId":"br01","name":"MyWorker"}'
            )
            .then(function () {
                return br._findBroker(agent.id);
            })
            .then(function (res) {
                assert.strictEqual(res, null);
            });
        });
    });

    it('Null is return if broker info in bh is corrupted', function () {
        br.registerWorker(MyWorker);
        return br.createWorker('MyWorker')
        .then(function (agent) {
            assert.strictEqual(typeof agent, 'object');

            // Corrupt the worker entry
            return br.pub.hsetAsync(
                br._keys.bh,
                br.id,
                '{"cn":"main",bad"st":"active"}'
            )
            .then(function () {
                return br._findBroker(agent.id);
            })
            .then(function (res) {
                assert.strictEqual(res, null);
            });
        });
    });
});
