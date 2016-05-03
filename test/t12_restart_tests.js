'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');
var helper = require('./helper');
var sinon = require('sinon');

describe('Restart tests', function () {
    var brs = [];
    var numCreated = 0;
    var numRecovered = 0;
    var numDestroyed = 0;
    var sandbox;

    function MyWorker() {
        Worker.apply(this, arguments);
    }
    util.inherits(MyWorker, Worker);

    MyWorker.prototype.onCreate = function (info) {
        debug('Worker(' + this.id + ') onCreate called, info=' + JSON.stringify(info));
        numCreated++;
        if (info.cause === Worker.CreateCause.RECOVERY) {
            numRecovered++;
        }
        return Promise.resolve();
    };

    MyWorker.prototype.onDestroy = function (info) {
        debug('Worker(' + this.id + ') onDestroy called, info=' + JSON.stringify(info));
        numDestroyed++;
        return Promise.resolve();
    };

    MyWorker.prototype.onAsk = function (method, data) {
        debug('Worker#onAsk called (method=' + method + ')');
        if (method === 'echo') {
            return this.echo(data);
        }
        return Promise.reject(new Error('Unexpected method: ' + method));
    };
    MyWorker.prototype.echo = function (data) {
        debug('Worker#echo called.');
        return Promise.resolve(data);
    };

    before(function () {
        function logger(data) {
            debug('LOG(' + data.level + '): ' + data.message);
        }

        sandbox = sinon.sandbox.create();

        for (var i = 0; i < 2; ++i) {
            brs.push(new Broker('br0' + i, {
                retries: {
                    initialInterval: 25,
                    maxInterval: 100,
                    duration: 400
                }
            }));
        }

        brs.forEach(function (br) {
            br.on('log', logger);
            br.on('state', function (fromState, toState) {
                debug('[%s] statechange %d -> %d', br.id, fromState, toState);
            });
        });

        return helper.initRedis(brs[0]);
    });

    afterEach(function () {
        var p = [];
        brs.forEach(function (br) {
            if (br.state !== Broker.State.DESTROYED) {
                p.push(br.destroy({ noRecover: true}));
            }
        });
        sandbox.restore();
        return Promise.all(p)
        .then(function () {
            brs.forEach(function (br) {
                assert.ok(
                    br.state === Broker.State.INACTIVE ||
                    br.state === Broker.State.DESTROYED,
                    "must be either inactive or destroyed"
                );
            });
        });
    });

    it('#restart', function () {
        brs[0].registerWorker(MyWorker);
        return Promise.resolve()
        .then(function () {
            return brs[0].start();
        })
        .then(function () {
            return brs[0].restart();
        })
        .then(function () {
            assert.equal(brs[0].state, Broker.State.ACTIVE);
        });
    });

    it('#restartAll', function () {
        var spyPub = sandbox.spy(brs[0]._pub, 'publish');
        var spyRestart0 = sandbox.spy(brs[0], 'restart');
        var spyRestart1 = sandbox.spy(brs[1], 'restart');
        brs[0].registerWorker(MyWorker);
        brs[1].registerWorker(MyWorker);
        return Promise.resolve(brs)
        .each(function (br) {
            return br.start();
        })
        .then(function () {
            brs[0].restartAll();
            return Promise.delay(100);
        })
        .then(function () {
            assert.equal(brs[0].state, Broker.State.ACTIVE);
            assert.equal(brs[1].state, Broker.State.ACTIVE);
            assert.ok(spyPub.calledOnce);
            assert.ok(spyRestart0.calledOnce);
            assert.ok(spyRestart1.calledOnce);
        });
    });

    it('#restartAll failure test', function () {
        var spy = sandbox.spy(brs[0]._pub, 'publish');
        brs[0].quit();
        return Promise.delay(100)
        .then(function () {
            brs[0].restartAll();
            assert.ok(!spy.called); // never publish
        });
    });
});
