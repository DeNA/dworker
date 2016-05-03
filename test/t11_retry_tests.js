'use strict';

var Broker = require('..').Broker;
var Promise = require('bluebird');
var assert = require('assert');
var debug = require('debug')('dw:test');
var helper = require('./helper');
var sinon = require('sinon');

describe('Recovery tests', function () {
    var br;
    var brId = 'br01';
    var sandbox;

    before(function () {
        sandbox = sinon.sandbox.create();
        function logger(data) {
            debug('LOG(' + data.level + '): ' + data.message);
        }

        br = new Broker(brId, {
            retries: {
                initialInterval: 25,
                maxInterval: 100,
                duration: 400
            }
        });

        br.on('log', logger);
        return helper.initRedis(br);
    });

    beforeEach(function () {
        br._brokerCache.reset();
        br._brokerCacheHits = 0;
        br._brokerCacheMisses = 0;
    });

    afterEach(function () {
        sandbox.restore();
        if (br.state !== Broker.State.DESTROYED) {
            br.destroy({ noRecover: true});
        }
        assert.ok(
            br.state === Broker.State.INACTIVE ||
            br.state === Broker.State.DESTROYED,
            "must be either inactive or destroyed"
        );
    });

    it('#_askWorker success (no retry)', function () {
        var reqCalls = 0;
        br._brokerCache.set('me', {
            brokerId: 'i-am-here',
            clustername: 'test',
            status: 'active'
        });
        sandbox.stub(br, '_request', function (obj, brokerId) {
            debug('stub:_request: obj:', obj);
            debug('stub:_request: brokerId:', brokerId);
            reqCalls++;
            return Promise.resolve('stub');
        });

        return br._askWorker('testme', {}, 'me')
        .then(function (res) {
            assert.strictEqual(res, 'stub');
            assert.strictEqual(reqCalls, 1);
            assert.strictEqual(br._brokerCacheHits, 1);
            assert.strictEqual(br._brokerCacheMisses, 0);
        });
    });

    it('#_askWorker find missed', function () {
        var reqCalls = 0;
        var redisResp = [ 0, ['i-am-here', 'test', 'active', '127.0.0.2:1234' ]];
        sandbox.stub(br._pub, 'evalshaAsync', function () {
            return Promise.resolve(redisResp);
        });
        sandbox.stub(br, '_request', function (obj, brokerId) {
            debug('stub:_request: obj:', obj);
            debug('stub:_request: brokerId:', brokerId);
            reqCalls++;
            return Promise.resolve('stub');
        });

        return br._askWorker('testme', {}, 'me')
        .then(function (res) {
            assert.strictEqual(res, 'stub');
            assert.strictEqual(reqCalls, 1);
            assert.strictEqual(br._brokerCacheHits, 0);
            assert.strictEqual(br._brokerCacheMisses, 1);
        });
    });

    it('#_askWorker hit, fail then invalidate', function () {
        var reqCalls = 0;
        br._brokerCache.set('me', {
            brokerId: 'i-am-here',
            clustername: 'test',
            status: 'active'
        });
        var redisResp = [ 0, ['i-am-here', 'test', 'active', '127.0.0.2:1234' ]];
        sandbox.stub(br._pub, 'evalshaAsync', function () {
            return Promise.resolve(redisResp);
        });
        sandbox.stub(br, '_request', function (obj, brokerId) {
            if (reqCalls === 0) {
                reqCalls++;
                return Promise.reject(new Error('test: Unreachable'));
            }
            debug('stub:_request: obj:', obj);
            debug('stub:_request: brokerId:', brokerId);
            reqCalls++;
            return Promise.resolve('stub');
        });

        return br._askWorker('testme', {}, 'me')
        .then(function (res) {
            assert.strictEqual(res, 'stub');
            assert.strictEqual(reqCalls, 2);
            assert.strictEqual(br._brokerCacheHits, 1);
            assert.strictEqual(br._brokerCacheMisses, 1);
        });
    });

    it('#_tellWorker success (no retry)', function () {
        var reqCalls = 0;
        br._brokerCache.set('me', {
            brokerId: 'i-am-here',
            clustername: 'test',
            status: 'active'
        });
        sandbox.stub(br, '_request', function (obj, brokerId) {
            debug('stub:_request: obj:', obj);
            debug('stub:_request: brokerId:', brokerId);
            reqCalls++;
            return Promise.resolve('stub');
        });

        return br._tellWorker('testme', {}, 'me')
        .then(function (res) {
            assert.strictEqual(res, 'stub');
            assert.strictEqual(reqCalls, 1);
            assert.strictEqual(br._brokerCacheHits, 1);
            assert.strictEqual(br._brokerCacheMisses, 0);
        });
    });

    it('#_tellWorker find error then retry timeout', function () {
        sandbox.stub(br, '_findBroker', function () {
            return Promise.resolve(null);
        });
        return br._tellWorker('testme', {}, 'me')
        .then(function () {
            throw new Error('Should fail after retries');
        }, function (err) {
            assert.ok(err);
            assert.equal(err.name, 'Error');
            debug('err:', err);
        });
    });

    it('#_tellWorker _request failure then retry timeout', function () {
        var redisResp = [ 0, ['i-am-here', 'test', 'active', '127.0.0.2:1234' ]];
        sandbox.stub(br._pub, 'evalshaAsync', function () {
            return Promise.resolve(redisResp);
        });
        sandbox.stub(br, '_request', function () {
            return Promise.reject(new Error('Fake request error'));
        });

        return br._tellWorker('testme', {}, 'me')
        .then(function () {
            throw new Error('Should fail after retries');
        }, function (err) {
            assert.ok(err);
            assert.equal(err.name, 'Error');
            assert.equal(err.message, 'Fake request error');
            debug('err:', err);
        });
    });
});
