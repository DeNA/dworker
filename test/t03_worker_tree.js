'use strict';

/* Worker tree test
 * This test will create a binary tree in which each node will have two child
 * nodes, with a depth of 10. (there will be 1023 nodes total)
 * The root worker will perform getSum() operation, which will recur all the
 * way down to all the leaf nodes, then each node will return a sum direct
 * children values plus 1 (for self). The resulting number must be identical
 * to the number of total nodes.
 */

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');
var async = require('async');
var Promise = require('bluebird');

describe('Worker tree test', function () {
    var br;
    var brId = 'br01';
    var numNodesCreated = 0;
    var numNodesDestroyed = 0;

    function Node() {
        Worker.apply(this, arguments);
        numNodesCreated++;
    }
    util.inherits(Node, Worker);
    Node.prototype.onCreate = function (info) {
        assert.strictEqual(info.cause, Worker.CreateCause.NEW);
        var self = this;
        if (this.attributes.depth < this.attributes.maxDepth) {
            // This is the parent. Create two children.
            var option = {
                attributes: {
                    parentId: this.id,
                    depth: this.attributes.depth + 1,
                    maxDepth: this.attributes.maxDepth
                }
            };

            debug('Node#onCreate: creating children');
            return Promise.all([
                br.createWorker('Node', option),
                br.createWorker('Node', option)
            ])
            .then(function (results) {
                self._children = results;
                self._onChildrenReady();
            }, function (err) {
                debug('Error:', err);
                process.exit(1);
            })
            .finally(function () {
                debug('Node#onCreate: done');
            });
        }

        return Promise.resolve();
    };

    Node.prototype.onDestroy = function (info) {
        void(info);
        numNodesDestroyed++;
        return Promise.resolve();
    };

    Node.prototype._onChildrenReady = function () {
        if (!this._pending) {
            return;
        }
        this._doGetSum(this._pending[0], this._pending[1]);
        delete this._pending;
    };

    Node.prototype.onAsk = function (method, data, cb) {
        assert.equal(typeof this[method], 'function');
        this[method](data, cb);
    };

    Node.prototype.getSum = function (data, cb) {
        var self = this;
        if (this.attributes.depth < this.attributes.maxDepth) {
            if (!this._children) {
                this._pending = [data, function (err, res) {
                    self.destroy();
                    cb(err, res);
                }];
            } else {
                this._doGetSum(data, function (err, res) {
                    self.destroy();
                    cb(err, res);
                });
            }
        } else {
            this.destroy();
            cb(null, 1);
        }
    };

    Node.prototype._doGetSum = function (data, cb) {
        var self = this;
        async.parallel([
            function (done) {
                self._children[0].ask('getSum', {}, done);
            },
            function (done) {
                self._children[1].ask('getSum', {}, done);
            }
        ], function (err, res) {
            if (err) {
                return void(cb(err));
            }
            cb(null, (res[0] + res[1] + 1 /* count self */));
        });
    };

    before(function () {
        br = new Broker(brId);
        br.registerWorker('Node', Node);
        return br._pub.hdelAsync(br._keys.bh, br.id)
        .then(function() {
            return br.start();
        });
    });

    after(function () {
        br.destroy();
    });

    it('Create worker', function (done) {
        function sum(depth) {
            var val = 0;
            for(var i = 0; i < depth; ++i) {
                val += Math.pow(2, i);
            }
            return val;
        }

        var maxDepth = 10;
        var expectedSum = sum(maxDepth);

        var option = {
            attributes: {
                parentId: null, // is parent
                depth: 1,
                maxDepth: maxDepth
            }
        };
        
        br.createWorker('Node', option, function (err, agent) {
            if (!agent) {
                return;
            }
            agent.ask('getSum', {}, function (err, sum) {
                assert.ifError(err);
                debug('SUM=' + sum);
                setTimeout(function () {
                    debug('num nodes created   :' + numNodesCreated);
                    debug('num nodes destroyed :' + numNodesDestroyed);
                    assert.strictEqual(sum, expectedSum);
                    assert.strictEqual(numNodesCreated, expectedSum);
                    assert.strictEqual(numNodesDestroyed, expectedSum);
                    done();
                }, 500);
            });
        });
    });
});

