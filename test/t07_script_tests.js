'use strict';

var Broker = require('..').Broker;
var Promise = require('bluebird');
var scripts = require('../lib/scripts');
var assert = require('assert');
var debug = require('debug')('dw:test');
var helper = require('./helper');
var redis = require('redis');

describe('Lua script tests', function () {
    var br;
    var client; // redis client
    var keys;   // redis keys

    before(function () {
        br = new Broker('br01');
        br.on('log', function (data) {
            debug('LOG(' + data.level + '): ' + data.message);
        });
        client = br._pub;
        keys = br._keys;
        return br._loadScripts();
    });

    after(function () {
        //return helper.initRedis(br)
        return Promise.resolve()
        .then(function () {
            br.destroy();
        });
    });

    beforeEach(function () {
        return helper.initRedis(br);
    });

    afterEach(function () {
    });

    it('All keys should be present', function () {
        assert.ok(keys.gh);
        assert.ok(keys.wh);
        assert.ok(keys.bh);
        assert.ok(keys.cz);
        assert.ok(keys.wz);
        assert.ok(keys.rz);
    });

    describe('addBroker.lua', function () {
        it('Successfully update tables', function () {
            var clustername = 'main';
            var chPrefix = 'dw:ch';
            var addr = '127.0.0.2:1234';
            var load = 23;
            var workerId = "myworker";
            return client.zaddAsync(keys.wz+':'+br.id, 12, workerId)
            .then(function () {
                return client.evalshaAsync(
                    scripts.addBroker.sha,
                    6,
                    keys.gh,
                    keys.wh,
                    keys.bh,
                    keys.cz + ':main',
                    keys.wz + ':' + br.id,
                    keys.rz,
                    br.id,
                    br._chPrefix,
                    load,
                    clustername,
                    addr
                );
            })
            .then(function (res) {
                assert.ok(Array.isArray(res));
                assert.strictEqual(res[0], 0);

                return client.hgetAsync(keys.gh, 'chPrefix')
                .then(function (val) {
                    assert.strictEqual(val, chPrefix);
                    return client.hgetAsync(keys.bh, br.id);
                })
                .then(function (val) {
                    assert.strictEqual(typeof val, 'string');
                    var info = JSON.parse(val);
                    assert.strictEqual(info.cn, clustername);
                    assert.strictEqual(info.st, 'active');
                    assert.strictEqual(info.addr, addr);

                    return client.zscoreAsync(keys.cz+':main', br.id);
                })
                .then(function (val) {
                    assert.strictEqual(typeof val, 'string');
                    var _load = parseInt(val);
                    assert.strictEqual(_load, load);
                    return client.zscoreAsync(keys.wz+':'+br.id, workerId);
                })
                .then(function (val) {
                    assert.strictEqual(val, null);
                });
            });
        });

        it('Stale broker with active state', function () {
            var clustername = 'main';
            var chPrefix = 'dw:ch';
            var addr = '127.0.0.3:5678';
            var load = 23;
            var workerId = "myworker";
            var brInfo = {cn:clustername, st:'active'};
            var i;
            var numWorkers = 100;
            var numRecoverable = Math.floor(numWorkers/2);
            var hmsetArgs = [keys.wh];
            var zaddArgs = [keys.wz + ':' + br.id];
            var workers = [];
            for (i = 0; i < numWorkers; ++i) {
                var wid = 'worker-' + i;
                hmsetArgs.push(wid);
                var winfo = {
                    name: 'worker',
                    brokerId: br.id,
                    attributes: {
                        static:false,
                        /* jshint ignore:start */
                        recoverable: !!(i%2)
                        /* jshint ignore:end */
                    }
                };
                hmsetArgs.push(JSON.stringify(winfo));
                zaddArgs.push(i);
                zaddArgs.push(wid);
                workers.push({ id: wid, info: winfo });
            }
            return client.hsetAsync(keys.bh, br.id, JSON.stringify(brInfo))
            .then(function () {
                return client.hmsetAsync(hmsetArgs);
            })
            .then(function () {
                return client.zaddAsync(zaddArgs);
            })
            .then(function () {
                return client.evalshaAsync(
                    scripts.addBroker.sha,
                    6,
                    keys.gh,
                    keys.wh,
                    keys.bh,
                    keys.cz + ':main',
                    keys.wz + ':' + br.id,
                    keys.rz,
                    br.id,
                    br._chPrefix,
                    load,
                    clustername,
                    addr
                );
            })
            .then(function (res) {
                assert.ok(Array.isArray(res));
                assert.strictEqual(res[0], 0);

                return client.hgetAsync(keys.gh, 'chPrefix')
                .then(function (val) {
                    assert.strictEqual(val, chPrefix);
                    return client.hgetAsync(keys.bh, br.id)
                    .then(function (val) {
                        assert.strictEqual(typeof val, 'string');
                        var info = JSON.parse(val);
                        assert.strictEqual(info.cn, clustername);
                        assert.strictEqual(info.st, 'active');
                        assert.strictEqual(info.addr, addr);
                    });
                })
                .then(function () {
                    return client.zscoreAsync(keys.cz+':main', br.id)
                    .then(function (val) {
                        assert.strictEqual(typeof val, 'string');
                        var _load = parseInt(val);
                        assert.strictEqual(_load, load);
                    });
                })
                .then(function() {
                    return client.zscoreAsync(keys.wz+':'+br.id, workerId)
                    .then(function (val) {
                        assert.strictEqual(val, null);
                    });
                })
                .then(function () {
                    // size of rz must be half of the numWorkers.
                    return client.zcardAsync(keys.rz)
                    .then(function (val) {
                        assert.strictEqual(val, numRecoverable);
                    });
                })
                .then(function () {
                    // The rz must only have recoverable workers.
                    return Promise.resolve(workers)
                    .each(function (worker) {
                        return client.zscoreAsync(keys.rz, worker.id)
                        .then(function (score) {
                            if (worker.info.attributes.recoverable) {
                                // It should be present.
                                assert.strictEqual(typeof score, 'string');
                            } else {
                                // It should NOT be present.
                                assert.strictEqual(score, null);
                            }
                        });
                    });
                })
                .then(function () {
                    // The wh must only have recoverable workers, and
                    // brokerId property must have been removed.
                    return Promise.resolve(workers)
                    .each(function (worker) {
                        return client.hgetAsync(keys.wh, worker.id)
                        .then(function (data) {
                            if (worker.info.attributes.recoverable) {
                                assert.strictEqual(typeof data, 'string');
                                var winfo = JSON.parse(data);
                                assert(winfo.attributes.recoverable);
                                assert(!winfo.brokerId);
                            } else {
                                assert.strictEqual(data, null);
                            }
                        });
                    });
                });
            });
        });
    });

    describe('getLeastLoadedBroker.lua', function () {

        var sub;

        beforeEach(function () {
            return client.hsetAsync(keys.gh, 'chPrefix', br._chPrefix);
        });

        afterEach(function () {
            if (sub) {
                sub.quit();
                sub = null;
            }
        });

        it('Null is returned with empty cluster', function () {
            return client.evalshaAsync(
                scripts.getLeastLoadedBroker.sha,
                4,
                keys.gh,
                keys.wh,
                keys.bh,
                keys.cz + ':main'
            )
            .then(function (res) {
                assert.strictEqual(res, null);
            });
        });

        it('Successfully choose the least loaded broker out of 3', function () {
            var clustername = 'main';

            return new Promise(function (resolve) {
                sub = redis.createClient();
                sub.subscribe(br._chPrefix+':br02');
                sub.on('subscribe', resolve);
            })
            .then(function () {
                return client.zaddAsync([
                    keys.cz+':'+clustername,
                    13, 'br01',
                    12, 'br02',
                    15, 'br03'
                ])
                .then(function () {
                    return client.hsetAsync(keys.bh, 'br02', JSON.stringify({
                        cn: clustername,
                        st: 'active'
                    }));
                });
            })
            .then(function () {
                return client.evalshaAsync(
                    scripts.getLeastLoadedBroker.sha,
                    4,
                    keys.gh,
                    keys.wh,
                    keys.bh,
                    keys.cz + ':main'
                )
                .then(function (res) {
                    assert.ok(Array.isArray(res));
                    assert.strictEqual(res.length, 2);
                    assert.strictEqual(res[0], 'br02');
                    assert.strictEqual(res[1], clustername);
                });
            });
        });

        it('Successfully choose the second least loaded broker out of 3', function () {
            var clustername = 'main';

            return new Promise(function (resolve) {
                sub = redis.createClient();
                sub.subscribe(br._chPrefix+':br01');
                sub.on('subscribe', function (ch, count) {
                    void(ch);
                    if (count === 1) {
                        resolve();
                    }
                });
            })
            .then(function () {
                return client.zaddAsync([
                    keys.cz+':'+clustername,
                    13, 'br01', // the second least in good shape
                    12, 'br02', // least loaded, but not listening
                    15, 'br03'
                ])
                .then(function () {
                    return client.hmsetAsync([
                        keys.bh,
                        'br01', JSON.stringify({ cn: clustername, st: 'active' }),
                        'br02', JSON.stringify({ cn: clustername, st: 'active' })
                    ]);
                });
            })
            .then(function () {
                return client.evalshaAsync(
                    scripts.getLeastLoadedBroker.sha,
                    4,
                    keys.gh,
                    keys.wh,
                    keys.bh,
                    keys.cz + ':main'
                )
                .then(function (res) {
                    debug('res=', res);
                    assert.ok(Array.isArray(res));
                    assert.strictEqual(res.length, 2);
                    assert.strictEqual(res[0], 'br01');
                    assert.strictEqual(res[1], clustername);
                })
                .then(function () {
                    // Check if 'br02' is correctly invalidated
                    return client.hgetAsync(keys.bh, 'br02')
                    .then(function (res) {
                        var brInfo = JSON.parse(res);
                        assert.strictEqual(brInfo.st, 'invalid');
                    });
                });
            });
        });

        it('Fail if all 3 brokers are unreachable', function () {
            var clustername = 'main';
            var signals = [];

            return new Promise(function (resolve) {
                sub = redis.createClient();
                sub.subscribe(br._chPrefix+':*');
                sub.on('subscribe', function (ch, count) {
                    void(ch);
                    if (count === 1) {
                        resolve();
                    }
                });
                sub.on('message', function (chId, text) {
                    void(chId);
                    debug('signal received: ' + text);
                    signals.push(JSON.parse(text));
                });
            })
            .then(function () {
                return client.zaddAsync([
                    keys.cz+':'+clustername,
                    13, 'br01', // the second least in good shape
                    12, 'br02', // least loaded, but not listening
                    15, 'br03'
                ])
                .then(function () {
                    return client.hmsetAsync([
                        keys.bh,
                        'br01', JSON.stringify({ cn: clustername, st: 'active' }),
                        'br02', JSON.stringify({ cn: clustername, st: 'active' }),
                        'br03', JSON.stringify({ cn: clustername, st: 'active' })
                    ]);
                });
            })
            .then(function () {
                return client.evalshaAsync(
                    scripts.getLeastLoadedBroker.sha,
                    4,
                    keys.gh,
                    keys.wh,
                    keys.bh,
                    keys.cz + ':main'
                )
                .then(function (res) {
                    debug('res=', res);
                    assert.strictEqual(res, null);
                })
                .then(function () {
                    // Check if all brokers are correctly invalidated
                    return client.hmgetAsync(keys.bh, 'br01', 'br02', 'br03')
                    .then(function (res) {
                        assert.ok(Array.isArray(res));
                        assert.equal(res.length, 3);
                        res.forEach(function (s) {
                            debug('s=', s);
                            var brInfo = JSON.parse(s);
                            assert.strictEqual(brInfo.st, 'invalid');
                        });
                    });
                })
                .then(function () {
                    // Check if all brokers are correctly removed from cz
                    return Promise.resolve(['br01', 'br02', 'br03'])
                    .each(function (brId) {
                        return client.zscoreAsync(keys.cz+':main', brId)
                        .then(function (res) {
                            assert.strictEqual(res, null);
                        });
                    })
                    .then(function () {
                        // Then check if 3 signals of 'salvage' are received'
                        var remaining = {'br01':true, 'br02':true, 'br03':true};
                        assert.equal(signals.length, 3);
                        signals.forEach(function (signal) {
                            assert.strictEqual(signal.sig, 'salvage');
                            delete remaining[signal.brokerId];
                        });
                        assert.equal(Object.keys(remaining).length, 0);
                    });
                });
            });
        });
    });

    describe('findOrCreateWorker.lua', function () {
        var sub;

        beforeEach(function () {
            return client.hsetAsync(keys.gh, 'chPrefix', br._chPrefix);
        });

        afterEach(function () {
            if (sub) {
                sub.quit();
                sub = null;
            }
        });

        it('creating a worker of unreachable broker should kick salvage', function () {
            var clustername = 'main';
            var signals = [];

            return new Promise(function (resolve) {
                sub = redis.createClient();
                sub.subscribe(br._chPrefix+':*');
                sub.on('subscribe', function (ch, count) {
                    void(ch);
                    if (count === 1) {
                        resolve();
                    }
                });
                sub.on('message', function (chId, text) {
                    void(chId);
                    debug('signal received: ' + text);
                    signals.push(JSON.parse(text));
                });
            })
            .then(function () {
                return client.zaddAsync(keys.cz+':'+clustername, 10, 'br01')
                .then(function () {
                    return client.hmsetAsync([
                        keys.bh,
                        'br01', JSON.stringify({ cn: clustername, st: 'active' }),
                        'br02', JSON.stringify({ cn: clustername, st: 'active' })
                    ]);
                })
                .then(function () {
                    return client.hmsetAsync([
                        keys.wh,
                        'wk01', JSON.stringify({
                            name: 'MyWorker',
                            brokerId: 'br01',
                            attributes: { static: false, recoverable: true }
                        })
                    ]);
                });
            })
            .then(function () {
                return client.evalshaAsync(
                    scripts.findOrCreateWorker.sha,
                    6,
                    keys.gh,
                    keys.wh,
                    keys.bh,
                    keys.cz + ':' + clustername,
                    keys.wz + ':br02',
                    keys.rz,
                    'br02',
                    'MyWorker',
                    'wk01',
                    JSON.stringify({ static: false, recoverable: true }),
                    Date.now(),
                    0,
                    8
                )
                .then(function (res) {
                    assert.ok(Array.isArray(res));
                    assert.equal(res.length, 1);
                    assert.equal(res[0], 1); // 1 = "retry later"
                })
                .then(function () {
                    // The worker `wk01` should still have brokerId property
                    return client.hgetAsync(keys.wh, 'wk01')
                    .then(function (res) {
                        assert.strictEqual(typeof res, 'string');
                        var winfo = JSON.parse(res);
                        assert.strictEqual(winfo.brokerId, 'br01');
                    });
                })
                .then(function () {
                    // The broker `br01` has status of "invalid"
                    return client.hgetAsync(keys.bh, 'br01')
                    .then(function (res) {
                        assert.strictEqual(typeof res, 'string');
                        var brInfo = JSON.parse(res);
                        assert.strictEqual(brInfo.st, 'invalid');
                    });
                })
                .then(function () {
                    // It should has sent a broadcast signal, 'salvage'
                    assert.equal(signals.length, 1);
                    assert.strictEqual(signals[0].sig, 'salvage');
                    assert.strictEqual(signals[0].brokerId, 'br01');
                });
            });
        });

        it('creating a worker under recovery will successfully recover it from rz', function () {
            var clustername = 'main';
            var signals = [];
            var createdAt = new Date(Date.now() - 60000).getTime();

            return new Promise(function (resolve) {
                sub = redis.createClient();
                sub.subscribe(br._chPrefix+':*');
                sub.on('subscribe', function (ch, count) {
                    void(ch);
                    if (count === 1) {
                        resolve();
                    }
                });
                sub.on('message', function (chId, text) {
                    void(chId);
                    debug('signal received: ' + text);
                    signals.push(JSON.parse(text));
                });
            })
            .then(function () {
                return client.zaddAsync(keys.cz+':'+clustername, 10, 'br01')
                .then(function () {
                    return client.hmsetAsync([
                        keys.bh,
                        'br02', JSON.stringify({ cn: clustername, st: 'active' })
                    ]);
                })
                .then(function () {
                    return client.hmsetAsync([
                        keys.wh,
                        'wk01', JSON.stringify({
                            name: 'MyWorker',
                            attributes: { static: false, recoverable: true }
                        })
                    ]);
                })
                .then(function () {
                    return client.zaddAsync([
                        keys.rz,
                        createdAt,
                        'wk01'
                    ]);
                });
            })
            .then(function () {
                return client.evalshaAsync(
                    scripts.findOrCreateWorker.sha,
                    6,
                    keys.gh,
                    keys.wh,
                    keys.bh,
                    keys.cz + ':' + clustername,
                    keys.wz + ':br02',
                    keys.rz,
                    'br02',
                    'MyWorker',
                    'wk01',
                    JSON.stringify({ static: false, recoverable: true }),
                    Date.now(),
                    0,
                    8
                )
                .then(function (res) {
                    assert.ok(Array.isArray(res));
                    assert.equal(res.length, 2);
                    assert.strictEqual(res[0], 0); // 0 = "successfully found one"
                    assert.ok(Array.isArray(res[1]));
                    assert.strictEqual(res[1].length, 3);
                    assert.strictEqual(res[1][0], 'br02');
                    assert.strictEqual(res[1][1], 'MyWorker');
                    assert.strictEqual(res[1][2], 'wk01');
                    assert.equal(signals.length, 0); // No signal is made
                })
                .then(function () {
                    // The created time must be the original one
                    return client.zscoreAsync(keys.wz + ':br02', 'wk01')
                    .then(function (score) {
                        debug('score=' + score + ' createdAt=' + createdAt);
                        assert.strictEqual(parseInt(score), createdAt);
                    });
                });
            });
        });
    });
});
