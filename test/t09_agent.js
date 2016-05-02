'use strict';

var Agent = require('..').Agent;
var Promise = require('bluebird');
var assert = require('assert');
//var debug = require('debug')('dw:test');

describe('Agent tests', function () {
    var dummyBroker = {
        _askWorker: function (method, data, id) {
            return Promise.resolve({
                method: method,
                data: data,
                id: id
            });
        },
        _tellWorker: function (method, data, id) {
            return Promise.resolve({
                method: method,
                data: data,
                id: id
            });
        }
    };

    it('Constructor test', function () {
        var agent = new Agent('me', dummyBroker);
        assert.strictEqual(agent.id, 'me');
        assert.equal(agent.__priv.br, dummyBroker);
    });

    it('#ask test', function () {
        var agent = new Agent('me', dummyBroker);
        var method = 'listen';
        var data = { msg: 'be nice' };
        return agent.ask(method, data)
        .then(function (res) {
            assert.strictEqual(res.method, method);
            assert.strictEqual(res.data, data);
            assert.strictEqual(res.id, agent.id);
        });
    });

    it('#ask test with callback', function (done) {
        var agent = new Agent('me', dummyBroker);
        var method = 'listen';
        var data = { msg: 'be nice' };
        agent.ask(method, data, function (err, res) {
            assert.ifError(err);
            assert.strictEqual(res.method, method);
            assert.strictEqual(res.data, data);
            assert.strictEqual(res.id, agent.id);
            done();
        });
    });

    it('#tell test', function () {
        var agent = new Agent('me', dummyBroker);
        var method = 'listen';
        var data = { msg: 'be nice' };
        return agent.tell(method, data)
        .then(function (res) {
            assert.strictEqual(res.method, method);
            assert.strictEqual(res.data, data);
            assert.strictEqual(res.id, agent.id);
        });
    });

    it('#tell test with callback', function (done) {
        var agent = new Agent('me', dummyBroker);
        var method = 'listen';
        var data = { msg: 'be nice' };
        agent.tell(method, data, function (err, res) {
            assert.ifError(err);
            assert.strictEqual(res.method, method);
            assert.strictEqual(res.data, data);
            assert.strictEqual(res.id, agent.id);
            done();
        });
    });

    it('Constructor throws when agent ID is non-string', function () {
        assert.throws(function () {
            void(new Agent(123, dummyBroker));
        }, function (e) {
            assert.equal(e.name, 'Error');
            return true;
        }, 'Should throw');
    });

    it('Constructor throws when broker is null', function () {
        assert.throws(function () {
            void(new Agent('me', null));
        }, function (e) {
            assert.equal(e.name, 'Error');
            return true;
        }, 'Should throw');
    });

    it('#ask throws when broker is null', function () {
        var agent = new Agent('me', dummyBroker);
        agent.__priv.br = null;
        return agent.ask('throw', dummyBroker)
        .then(function () {
            throw new Error('Should throw');
        }, function (e) {
            assert.equal(e.name, 'Error');
        });
    });

    it('#tell throws when broker is null', function () {
        var agent = new Agent('me', dummyBroker);
        agent.__priv.br = null;
        return agent.tell('throw', dummyBroker)
        .then(function () {
            throw new Error('Should throw');
        }, function (e) {
            assert.equal(e.name, 'Error');
        });
    });
});
