'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var assert = require('assert');
var util = require('util');
var debug = require('debug')('dw:test');

describe('Broker tests', function () {
    describe('Redis connection failure', function () {
        var br;
        var brId = 'br01';
        beforeEach(function () {
            br = new Broker(brId);
            br.on('log', function (data) {
                debug('LOG(' + data.level + '): ' + data.message);
            });
            return br.start();
        });

        it('#destroy while no redis connection', function () {
            br.quit();
            return br.destroy(null)
            .then(assert.fail, function (err) {
                debug(err);
                assert.ok(!br._timer, 'timer must be cleared');
            });
        });
    });

    describe('Remote broker disconnected', function () {
        var br = [];

        function MyWorker() {
            Worker.apply(this, arguments);
        }
        util.inherits(MyWorker, Worker);

        MyWorker.prototype.onCreate = function (info) {
            debug('Worker#onCreate called, cause=' + info.cause);
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
            [ 'br00', 'br01' ].forEach(function (brId) {
                var b = new Broker(brId);
                b.registerWorker(MyWorker);
                b.on('log', function (data) {
                    debug('LOG(' + data.level + '): ' + data.message);
                });
                br.push(b);
            });
        });

        it('Remote broker disconnect should trigger router to emit `disconnect` event', function () {
            var workerId;
            // listen on router's disconect event
            var br0DisconnectedBy;
            br[0]._router.on('disconnect', function (remote) {
                br0DisconnectedBy = remote;
            });

            // Start br[1] first to create a worker on it.
            return br[1].start()
            .then(function () {
                return br[1].createWorker('MyWorker', { attributes: { recoverable: true }})
                .then(function (agent) {
                    workerId = agent.id;
                });
            })
            .then(function () {
                // Now start br[0]
                return br[0].start();
            })
            .then(function () {
                // Find worker on br[1] from br[0]
                return br[0].findWorker(workerId)
                .then(function (agent) {
                    var br1Addr;
                    // loopback message to make sure there's a connection to br[1].
                    return agent.ask('echo', { ping: 1 })
                    .then(function () {
                        // record its local address.
                        br1Addr = br[1]._addr;
                        debug('br1Addr:', br1Addr);
                        // Now destroy br[1]
                        return br[1].destroy();
                    })
                    .then(function () {
                        // Send an echo to the worker again
                        return agent.ask('echo', { ping: 2 });
                    })
                    .then(function () {
                        debug('br0 disconnected by:', br0DisconnectedBy);
                        assert.equal('object', typeof(br0DisconnectedBy));
                        var disconnBy = br0DisconnectedBy.host + ':' + br0DisconnectedBy.port;
                        assert.strictEqual(br1Addr, disconnBy);
                    });
                });
            });
        });
    });
});

