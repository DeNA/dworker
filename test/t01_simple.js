'use strict';

var Broker = require('..').Broker;
var redis = require('redis');
var assert = require('assert');

describe('Simple tests', function () {
    var br;
    var brId = 'br01';

    it('#pub/#sub must newly be created', function () {
        br = new Broker(brId);
        assert(br.pub);
        assert(br.sub);
    });

    it('#pub/#sub must be reused', function () {
        var _pub = redis.createClient(6379, '127.0.0.1');
        var _sub = redis.createClient(6379, '127.0.0.1');
        br = new Broker(brId, {
            redis: {
                pub: _pub,
                sub: _sub
            }
        });
        assert(br.pub === _pub);
        assert(br.sub === _sub);
    });
});

describe('#_setState tests', function () {

    function transition(oldState, newState) {
        var br = { _state: oldState, emit: function (ev, oldSt, newSt) {
            assert.equal(ev, 'state');
            assert.strictEqual(oldSt, oldState);
            assert.strictEqual(newSt, newState);
        }};
        return Broker.prototype._setState.call(br, newState);
    }

    it('INACTIVE -> ACTIVATING should log error message', function () {
        assert(transition(Broker.State.INACTIVE, Broker.State.ACTIVATING));
    });
    it('ACTIVATING -> ACTIVE should log error message', function () {
        assert(transition(Broker.State.ACTIVATING, Broker.State.ACTIVE));
    });
    it('ACTIVE -> DESTROYING should log error message', function () {
        assert(transition(Broker.State.ACTIVE, Broker.State.DESTROYING));
    });
    it('DESTROYING -> DESTROYED should log error message', function () {
        assert(transition(Broker.State.DESTROYING, Broker.State.DESTROYED));
    });
    it('DESTROYED -> ACTIVATING should log error message', function () {
        assert(transition(Broker.State.DESTROYED, Broker.State.ACTIVATING));
    });
    it('ACTIVE -> ACTIVATING should log error message', function () {
        assert(!transition(Broker.State.ACTIVE, Broker.State.ACTIVATING));
    });
    it('DESTROYING -> ACTIVE should log error message', function () {
        assert(!transition(Broker.State.DESTROYING, Broker.State.ACTIVE));
    });
    it('DESTROYED -> DESTROYING should log error message', function () {
        assert(!transition(Broker.State.DESTROYED, Broker.State.DESTROYING));
    });
    it('ACTIVE -> DESTROYED should log error message', function () {
        assert(!transition(Broker.State.ACTIVE, Broker.State.DESTROYED));
    });
    it('ACTIVE -> <INVALID> should log error message', function () {
        assert(!transition(Broker.State.INACTIVE, 99));
    });
});
