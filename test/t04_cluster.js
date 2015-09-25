'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');

/*
 * Test plan:
 * 1. Create four clusters, c1, c2, c3 and c4
 * 2. Create four brokers:
 *     broker b1 - for cluster c1
 *     broker b2 - for cluster c2
 *     broker b3 - for cluster c3
 *     broker b4 - for cluster c4
 * 3. Register four Worker classes
 *     Worker1 - for cluster c1
 *     Worker2 - for cluster c2
 *     Worker3 - for cluster c3
 *     Worker4 - for cluster c4
 * 3. Create one worker for each class.
 * 4. Verify workers are instantiated on the expect brokers
 *     Worker1 - on b1
 *     Worker2 - on b2
 *     Worker3 - on b3
 *     Worker4 - on b4
 *    Retrieve these info via an RPC method: getInfo().
 */

describe('Cluster tests', function () {
    var brs = [
        { bid: 'b1', cln: 'c1' },
        { bid: 'b2', cln: 'c2' },
        { bid: 'b3', cln: 'c3' },
        { bid: 'b4', cln: 'c4' }
    ];
    var wks = [
        { wkn: 'Worker1', cln: 'c1' },
        { wkn: 'Worker2', cln: 'c2' },
        { wkn: 'Worker3', cln: 'c3' },
        { wkn: 'Worker4', cln: 'c4' }
    ];

    before(function () {
        brs.forEach(function(i) {
            i.br = new Broker(i.bid, { clustername: i.cln });
        });
        wks.forEach(function(i) {
            i.wkr = (function () {
                var wkr = function () {
                    Worker.apply(this, arguments);
                };
                util.inherits(wkr, Worker);
                wkr.classname = i.wkn;
                wkr.clustername = i.cln;
                wkr.prototype.onAsk = function (method, data) {
                    assert(typeof this[method], 'function');
                    return this[method](data);
                };
                wkr.prototype.getInfo = function (data) {
                    void(data);
                    debug('getInfo() called on Worker');
                    var info = {
                        wkn: this.constructor.classname,
                        cln: this.constructor.clustername,
                        bid: this.__priv.br.id
                    };
                    return Promise.resolve(info);
                };
                return wkr;
            })();
            brs.forEach(function (j) {
                j.br.registerWorker(i.wkr);
            });
        });

        // Now, start all brokers
        var promises = [];
        brs.forEach(function(i) {
            promises.push(
                i.br._pub.hdelAsync(i.br._keys.bh, i.br.id)
                .then(function () {
                    return i.br.start();
                })
                .then(function () {
                    debug('Broker ' + i.br.id + ' started');
                })
            );
        });
        return Promise.all(promises);
    });

    after(function () {
        brs.forEach(function(i) {
            i.br.destroy();
        });
    });

    it('Check if all are at the correct location', function () {
        // Instantiate all workers from brs[0].
        var promises = [];
        wks.forEach(function (i) {
            promises.push(brs[0].br.createWorker(i.wkn)
            .then(function (agent) {
                // Call RPC;getInfo();
                return agent.ask('getInfo', {});
            })
            .then(function (data) {
                debug('data:', data);
                assert.equal(data.wkn, i.wkn);
                assert.equal(data.cln, i.cln);
                assert.equal(data.bid, i.cln.replace(/c/,'b'));
            }));
        });
        return Promise.all(promises);
    });
});
