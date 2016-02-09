'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var assert = require('assert');
var util = require('util');
var debug = require('debug')('dw:test');
var sinon = require('sinon');

describe('Healthcheck tests', function () {
    var sandbox;

    before(function () {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
        sandbox.restore();
    });

    describe('Detect dead broker', function () {
        var br01 = null;
        var br02 = null;
        var workerId;

        function MyWorker() {
            Worker.apply(this, arguments);
        }
        util.inherits(MyWorker, Worker);

        MyWorker.prototype.onCreate = function (info, cb) {
            debug('Worker#onCreate called, cause=' + info.cause);
            assert.strictEqual(info.cause, Worker.CreateCause.NEW);
            cb();
        };

        MyWorker.prototype.onAsk = function (method, data, cb) {
            debug('Worker#onAsk called (method=' + method + ')');
            if (method === 'echo') {
                this.echo(data, cb);
            } else {
                assert(!'Unexpected method: ' + method);
            }
        };
        MyWorker.prototype.echo = function (data, cb) {
            debug('Worker#echo called.');
            cb(null, data);
        };

        before(function () {
            var setting = {
                retries: {
                    initialInterval: 20,
                    maxInterval: 200,
                    duration: 500
                }
            };
            br01 = new Broker('br01', setting);
            br01.on('log', function (data) {
                debug('LOG[' + br01.id + '](' + data.level + '): ' + data.message);
            });
            br01.registerWorker(MyWorker);

            br02 = new Broker('br02', setting);
            br02.on('log', function (data) {
                debug('LOG[' + br02.id + '](' + data.level + '): ' + data.message);
            });
            br02.registerWorker(MyWorker);

            // start br01 only
            return br01.pub.flushdbAsync()
            .then(function () {
                return br01.start();
            })
            .then(function () {
                return br01.createWorker('MyWorker', { attributes: { recoverable: true }})
                .then(function (agent) {
                    workerId = agent.id;
                    return agent.ask('echo', {msg: 'yahoo'});
                })
                .then(function (res) {
                    assert.strictEqual(res.msg, 'yahoo');
                });
            })
            .then(function () {
                // Now br01 holds the worker.
                // start br02 now
                return br02.start();
            });
        });

        after(function () {
            //return br.destroy({ noRecover: true });
        });

        it('Death of br01 should be detect by br02', function () {
            debug("### shutting down br01...");
            clearTimeout(br01._timer);
            br01._timer = null;
            br01._router.close();
            br01.quit();

            debug("### wait 3 seconds...");
            return Promise.delay(3000)
            .then(function () {
                debug("### findWorker() on br02");
                return br02.findWorker(workerId)
                .then(function (agent) {
                    return agent.ask('echo', {msg: 'awesome!'})
                    .then(function (res) {
                        assert.strictEqual(res.msg, 'awesome!');
                    });
                });
            });
        });
    });

    describe('Interval control tests', function () {
        it('healthCheckInterval = 0 disables it', function () {
            var br = new Broker('br00', {healthCheckInterval: 0});
            sandbox.stub(br, '_syncRedisTime', function () {});
            sandbox.stub(global, 'setTimeout', function () {});
            var stubEvalsha = sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0]);
            });

            assert.strictEqual(br._option.healthCheckInterval, 0);
            assert.strictEqual(br._healthCheckIn, 0);
            br._onTimer();
            assert.strictEqual(br._healthCheckIn, 0);
            assert.equal(stubEvalsha.callCount, 0);
        });

        it('if healthCheckInterval = 2, first _onTimer() should not do healthCheck', function () {
            var br = new Broker('br00', {healthCheckInterval: 2});
            sandbox.stub(br, '_syncRedisTime', function () {});
            sandbox.stub(global, 'setTimeout', function () {});
            var stubEvalsha = sandbox.stub(br._pub, 'evalshaAsync', function () {
                return Promise.resolve([0]);
            });

            assert.strictEqual(br._option.healthCheckInterval, 2);
            assert.strictEqual(br._healthCheckIn, 2);
            br._onTimer();
            assert.strictEqual(br._healthCheckIn, 1);
            assert.equal(stubEvalsha.callCount, 0);
            br._onTimer();
            assert.strictEqual(br._healthCheckIn, 2);
            assert.equal(stubEvalsha.callCount, 1);
        });
    });
});
