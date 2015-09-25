'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');
var helper = require('./helper');

describe('Recovery tests', function () {
    var brs = [];
    var numCreated = 0;
    var numRecovered = 0;
    var numDestroyed = 0;

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
        if (method === 'die') {
            return this.die(data);
        }
        return Promise.reject(new Error('Unexpected method: ' + method));
    };
    MyWorker.prototype.onTell = function (method, data) {
        debug('Worker#onTell called (method=' + method + ')');
        if (method === 'greeting') {
            this._greeting = data; // just cache
        } else {
            assert(!'Unexpected method: ' + method);
        }
    };
    MyWorker.prototype.echo = function (data) {
        debug('Worker#echo called.');
        return Promise.resolve(data);
    };
    MyWorker.prototype.die = function (data) {
        debug('Worker#die called.');
        this.destroy({noRecover: data.noRecover});
        return Promise.resolve(data);
    };

    before(function () {
        function logger (data) {
            debug('LOG(' + data.level + '): ' + data.message);
        }

        for (var i = 0; i < 2; ++i) {
            brs.push(new Broker('br0' + i, {
                retries: {
                    initialInterval: 25,
                    maxInterval: 100,
                    duration: 400
                }
            }));

            brs[i].on('log', logger);
        }

        return helper.initRedis(brs[0]);
    });

    afterEach(function () {
        var p = [];
        brs.forEach(function (br) {
            if (br.state !== Broker.State.DESTROYED) {
                p.push(br.destroy({ noRecover: true}));
            }
        });
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

    describe('Broker destroy & recover test', function () {
        beforeEach(function () {
            // Start brs[0] first
            return brs[0].start();
        });

        it('Create worker -> destroy -> findWorker from another broker', function () {
            var workerId;

            brs[0].registerWorker(MyWorker);
            brs[1].registerWorker(MyWorker);
            return brs[0].createWorker('MyWorker', { attributes: { recoverable: true }})
            .then(function (agent) {
                assert(typeof agent === 'object');
                assert(typeof agent.ask === 'function');
                assert(typeof agent.tell === 'function');

                workerId = agent.id;

                return agent.ask('echo', {msg: 'yahoo'})
                .then(function (res) {
                    assert.strictEqual(res.msg, 'yahoo');
                });
            })
            .then(function () {
                return brs[1].start()
                .then(function () {
                    return brs[0].destroy();
                });
            })
            .then(function () {
                return brs[1].findWorker(workerId)
                .then(function (agent) {
                    assert.ok(agent !== null, 'Valid agent must be returned');
                    assert.strictEqual(agent.id, workerId);
                    return agent.ask('echo', { msg: 'yahoo!' });
                })
                .then(function (data) {
                    debug('echoed data:', data);
                });
            });
        });

        it('Create worker -> quit -> findWorker from other broker', function () {
            var workerId;

            brs[0].registerWorker(MyWorker);
            brs[1].registerWorker(MyWorker);
            return brs[0].createWorker('MyWorker', { attributes: { recoverable: true }})
            .then(function (agent) {
                assert(typeof agent === 'object');
                assert(typeof agent.ask === 'function');
                assert(typeof agent.tell === 'function');

                workerId = agent.id;

                return agent.ask('echo', {msg: 'yahoo'})
                .then(function (res) {
                    assert.strictEqual(res.msg, 'yahoo');
                });
            })
            .then(function () {
                return brs[1].start()
                .then(function () {
                    brs[0].quit();
                    brs[0] = new Broker('br00');
                    /*
                    return new Promise(function (resolve) {
                        brs[0]._sub.unsubscribe(brs[0]._chBc, brs[0]._chUc);
                        brs[0]._sub.on('unsubscribe', function (ch, count) {
                            debug('on unsubscribe: ch=' + ch + ' count=' + count);
                            if (count === 0) {
                                brs[0]._sub.removeAllListeners('unsubscribe');
                                resolve();
                            }
                        });
                    });
                    */
                });
            })
            .then(function () {
                return brs[1].findWorker(workerId)
                .then(function (agent) {
                    assert.ok(agent !== null, 'Valid agent must be returned');
                    assert.strictEqual(agent.id, workerId);
                    return agent.ask('echo', { msg: 'yahoo!' });
                })
                .then(function (data) {
                    debug('echoed data:', data);
                });
            });
        });
    });

    describe('Worker destroy & recover test', function () {
        beforeEach(function () {
            numCreated = 0;
            numRecovered = 0;
            numDestroyed = 0;
            // Use one broker
            return brs[0].start();
        });

        it('Go-Die-Go', function () {
            brs[0].registerWorker(MyWorker);
            return brs[0].createWorker('MyWorker', { attributes: { recoverable: true }})
            .then(function (agent) {
                debug('New agent: id=' + agent.id);
                assert(typeof agent === 'object');
                assert(typeof agent.ask === 'function');
                assert(typeof agent.tell === 'function');


                return agent.ask('echo', {msg: 'yahoo'})
                .then(function (res) {
                    assert.strictEqual(res.msg, 'yahoo');
                })
                .then(function () {
                    return agent.ask('die', {noRecover: false});
                })
                .then(function () {
                    return agent.ask('echo', {msg: 'still there?'});
                })
                .then(function (res) {
                    assert.strictEqual(res.msg, 'still there?');
                    assert.equal(numCreated, 2);
                    assert.equal(numRecovered, 1);
                    assert.equal(numDestroyed, 1);
                });
            });
        });
    });

    describe('Worker destroy & recover test', function () {
        beforeEach(function () {
            // Use one broker
            return brs[0].start();
        });

        it('Ask() after remote worker died', function () {
            brs[0].registerWorker(MyWorker);
            return brs[0].createWorker('MyWorker', { attributes: { recoverable: true }})
            .then(function (agent) {
                debug('New agent: id=' + agent.id);
                assert(typeof agent === 'object');
                assert(typeof agent.ask === 'function');
                assert(typeof agent.tell === 'function');

                return agent.ask('die', {noRecover: true})
                .then(function () {
                    // Echo against dead worker.
                    return agent.ask('echo', {msg: 'yahoo'});
                })
                .then(function () {
                    throw new Error('The ask should fail');
                }, function (err) {
                    debug('Ask after remote worker died: err:', err);
                    assert.equal(err.name, 'Error');
                });
            });
        });
    });
});
