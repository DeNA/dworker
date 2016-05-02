'use strict';

var Router = require('..').Router;
var Promise = require('bluebird');
var assert = require('assert');
var debug = require('debug')('dw:test');
var sinon = require('sinon');
var semver = require('semver');
var net = require('net');

describe('Router tests', function () {
    var sandbox;

    before(function () {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
        sandbox.restore();
    });

    describe('#listen tests', function () {
        var router;

        before(function () {
            router = new Router();
        });

        afterEach(function () {
            router.close();
        });

        it('default host name must be 0.0.0.0', function () {
            return router.listen()
            .then(function (addr) {
                assert.equal(addr.address, '0.0.0.0');
            });
        });

        it('Incorrect listening address should fail', function () {
            return router.listen('b.a.d.1')
            .then(function () {
                throw new Error('should fail');
            }, function (err) {
                assert.ok(err);
            });
        });

        if (semver.lt(process.version, '0.12.0')) {
            it('listen(port, host, cb) signature should be used (default host)', function () {
                var host = '0.0.0.0';
                sandbox.stub(semver, 'lt', function  () {
                    return false;
                });
                sandbox.stub(net.Server.prototype, 'listen', function (_option, cb) {
                    assert.strictEqual(_option.port, 0);
                    assert.strictEqual(_option.host, host);
                    assert.strictEqual(_option.exclusive, true);
                    cb();
                });
                sandbox.stub(net.Server.prototype, 'address', function () {
                    return {
                        address: host,
                        family: 'IPv4',
                        port: 8888
                    };
                });
                return router.listen()
                .then(function (addr) {
                    assert.equal(addr.address, host);
                    assert.equal(addr.port, 8888);
                });
            });
            it('listen(port, host, cb) signature should be used', function () {
                var host = '1.2.3.4';
                sandbox.stub(semver, 'lt', function  () {
                    return false;
                });
                sandbox.stub(net.Server.prototype, 'listen', function (_option, cb) {
                    assert.strictEqual(_option.port, 0);
                    assert.strictEqual(_option.host, host);
                    assert.strictEqual(_option.exclusive, true);
                    cb();
                });
                sandbox.stub(net.Server.prototype, 'address', function () {
                    return {
                        address: host,
                        family: 'IPv4',
                        port: 8888
                    };
                });
                return router.listen(host)
                .then(function (addr) {
                    assert.equal(addr.address, host);
                    assert.equal(addr.port, 8888);
                });
            });
        } else {
            it('listen(option) signature should be used', function () {
                var host = '1.2.3.4';
                sandbox.stub(semver, 'lt', function  () {
                    return true;
                });
                sandbox.stub(net.Server.prototype, 'listen', function (_port, _host, cb) {
                    assert.strictEqual(_port, 0);
                    assert.strictEqual(_host, host);
                    cb();
                });
                sandbox.stub(net.Server.prototype, 'address', function () {
                    return {
                        address: host,
                        family: 'IPv4',
                        port: 8888
                    };
                });
                return router.listen(host)
                .then(function (addr) {
                    assert.equal(addr.address, host);
                    assert.equal(addr.port, 8888);
                });
            });
        }
    });

    describe('#close tests', function () {
        var router;

        before(function () {
            router = new Router();
        });

        afterEach(function () {
            router.close();
        });

        it('close() in CLOSED state with be fine', function () {
            router.close();
        });
    });

    describe('#request tests', function () {
        var router0;
        var router1;
        var port0;
        var port1;
        var loopback = '127.0.0.1';
        var received0 = [];
        var received1 = [];
        var onRecv0 = null;
        var onRecv1 = null;

        beforeEach(function () {
            router0 = new Router({ socTimeout: 50 });
            router1 = new Router({ socTimeout: 50 });

            router0.on('request', function (data) {
                debug('router0 received:', data);
                received0.push(data);
                if (onRecv0) {
                    onRecv0();
                }
            });

            router1.on('request', function (data) {
                debug('router1 received:', data);
                received1.push(data);
                if (onRecv1) {
                    onRecv1();
                }
            });

            return Promise.resolve()
            .then(function () {
                return router0.listen(loopback)
                .then(function (addr) {
                    port0 = addr.port;
                    debug('port0 = ' + port0);
                });
            })
            .then(function () {
                return router1.listen(loopback)
                .then(function (addr) {
                    port1 = addr.port;
                    debug('port1 = ' + port1);
                });
            });
        });

        afterEach(function () {
            router0.close();
            router1.close();
            received0 = [];
            received1 = [];
            onRecv0 = null;
            onRecv1 = null;
        });

        it('Send one message', function (done) {
            var data = { msg: 'hello' };
            onRecv1 = function () {
                assert.equal(received1.length, 1);
                assert.deepEqual(received1[0], data);
                done();
            };
            router0.request({
                host: loopback,
                port: port1
            }, data);
        });

        it('Send two in a raw', function (done) {
            var data0 = { msg: 'hello' };
            var data1 = { msg: 'world' };
            var numRecv1 = 0;
            onRecv1 = function () {
                numRecv1++;
                if (numRecv1 === 2) {
                    assert.equal(received1.length, 2);
                    assert.deepEqual(received1[0], data0);
                    assert.deepEqual(received1[1], data1);
                    return done();
                }
            };
            router0.request({
                host: loopback,
                port: port1
            }, data0);
            router0.request({
                host: loopback,
                port: port1
            }, data1);
        });

        it('Send two in a raw (the second sends after connection opened)', function (done) {
            var data0 = { msg: 'hello' };
            var data1 = { msg: 'world' };
            var numRecv1 = 0;
            onRecv1 = function () {
                numRecv1++;
                if (numRecv1 === 2) {
                    assert.equal(received1.length, 2);
                    assert.deepEqual(received1[0], data0);
                    assert.deepEqual(received1[1], data1);
                    return done();
                }
            };
            router0.request({
                host: loopback,
                port: port1
            }, data0)
            .then(function () {
                router0.request({
                    host: loopback,
                    port: port1
                }, data1);
            });
        });

        it('Socket timeout test', function () {
            var data = { msg: 'hello' };
            onRecv1 = function () {
                assert.equal(received1.length, 1);
                assert.deepEqual(received1[0], data);
            };
            return router0.request({
                host: loopback,
                port: port1
            }, data)
            .then(function () {
                return Promise.delay(100);
            })
            .then(function () {
                assert.ok(!router0._clientSockets[loopback+':'+port1]);
            });
        });

        it('#Send two in a raw with interval > timeout', function (done) {
            var data0 = { msg: 'hello' };
            var data1 = { msg: 'world' };
            var numRecv1 = 0;
            onRecv1 = function () {
                numRecv1++;
                if (numRecv1 === 2) {
                    assert.equal(received1.length, 2);
                    assert.deepEqual(received1[0], data0);
                    assert.deepEqual(received1[1], data1);
                    return done();
                }
            };
            router0.request({
                host: loopback,
                port: port1
            }, data0);
            Promise.delay(100)
            .then(function () {
                assert.ok(!router0._clientSockets[loopback+':'+port1]);
                router0.request({
                    host: loopback,
                    port: port1
                }, data1);
            });
        });

        it('Connection error test', function () {
            var data = { msg: 'hello' };
            onRecv1 = function () {
                assert.equal(received1.length, 1);
                assert.deepEqual(received1[0], data);
            };
            return router0.request({
                host: loopback,
                port: 9 /* port that does not exist */
            }, data)
            .then(function () {
                throw new Error('Connection should fail');
            }, function (err) {
                assert.equal(err.code, 'ECONNREFUSED');
                assert.ok(!router0._clientSockets[loopback+':9']);
            });
        });

        it('Send to itself', function (done) {
            var data = { msg: 'hello' };
            onRecv0 = function () {
                assert.equal(received0.length, 1);
                assert.deepEqual(received0[0], data);
                assert.equal(received1.length, 0);
                done();
            };
            router0.request({
                host: loopback,
                port: port0
            }, data);
        });

        it('Send to the other and self', function (done) {
            // This tests if Router correctly detect the change of remote address
            // of the same destination broker ID.
            var data0 = { msg: 'hello' };
            var data1 = { msg: 'world' };
            var numRecv = 0;
            function complete() {
                assert.equal(received0.length, 1);
                assert.equal(received1.length, 1);
                assert.deepEqual(received0[0], data1);
                assert.deepEqual(received1[0], data0);
                done();
            }
            onRecv0 = function () {
                numRecv++;
                if (numRecv === 2) {
                    complete();
                }
            };
            onRecv1 = function () {
                numRecv++;
                if (numRecv === 2) {
                    complete();
                }
            };
            router0.request({
                host: loopback,
                port: port1
            }, data0)
            .then(function () {
                router0.request({
                    host: loopback,
                    port: port0
                }, data1);
            });
        });

        it('Send to itself', function (done) {
            var data = { msg: 'hello' };
            onRecv0 = function () {
                assert.equal(received0.length, 1);
                assert.deepEqual(received0[0], data);
                assert.equal(received1.length, 0);
                done();
            };
            router0.request({
                host: loopback,
                port: port0
            }, data);
        });

        it('The address must not be null', function () {
            return router0.request(null, { msg: 'hello' })
            .then(function () {
                throw new Error('Should fail');
            }, function (err) {
                assert.equal(err.name, 'Error');
            });
        });

        it('The address must be an object', function () {
            return router0.request(['127.0.0.1', 9090], { msg: 'hello' })
            .then(function () {
                throw new Error('Should fail');
            }, function (err) {
                assert.equal(err.name, 'Error');
            });
        });

        it('address.host must be a string', function () {
            return router0.request({ host: 127, port: 9090 }, { msg: 'hello' })
            .then(function () {
                throw new Error('Should fail');
            }, function (err) {
                assert.equal(err.name, 'Error');
            });
        });

        it('address.host must be a string', function () {
            return router0.request({ host: '127.0.0.1', port: '9090' }, { msg: 'hello' })
            .then(function () {
                throw new Error('Should fail');
            }, function (err) {
                assert.equal(err.name, 'Error');
            });
        });

        it('If the socket entry is in CLOSING state, it should create a new connection', function (done) {
            var data = { msg: 'hello' };
            var staleSocDestoryed = false;
            router0._clientSockets[loopback+':'+port0] = {
                state: 3, // ConnectionState.CLOSING
                soc: {
                    destroy: function () {
                        staleSocDestoryed = true;
                    }
                }
            };
            onRecv0 = function () {
                assert(staleSocDestoryed);
                assert.equal(received0.length, 1);
                assert.deepEqual(received0[0], data);
                assert.equal(received1.length, 0);
                done();
            };
            router0.request({
                host: loopback,
                port: port0
            }, data);
        });

        it('If the socket entry is in CLOSED state, it should create a new connection', function (done) {
            var data = { msg: 'hello' };
            var staleSocDestoryed = false;
            router0._clientSockets[loopback+':'+port0] = {
                state: 0, // ConnectionState.CLOSED
                soc: {
                    destroy: function () {
                        staleSocDestoryed = true;
                    }
                }
            };
            onRecv0 = function () {
                assert(staleSocDestoryed);
                assert.equal(received0.length, 1);
                assert.deepEqual(received0[0], data);
                assert.equal(received1.length, 0);
                done();
            };
            router0.request({
                host: loopback,
                port: port0
            }, data);
        });
    });

    describe('#respond tests', function () {
        var router0;
        var router1;
        var port0;
        var port1;
        var loopback = '127.0.0.1';
        var received0 = [];
        var received1 = [];
        var onRecv0 = null;
        var onRecv1 = null;

        beforeEach(function () {
            // router0: requester
            // router1: responder
            router0 = new Router({ socTimeout: 50 });
            router1 = new Router({ socTimeout: 50 });

            router0.on('response', function (data) {
                debug('router0 received:', data);
                received0.push(data);
                if (onRecv0) {
                    onRecv0();
                }
            });

            router1.on('request', function (data, requesterId) {
                debug('router1 received:', data);
                received1.push(data);
                if (onRecv1) {
                    onRecv1();
                }
                router1.respond(requesterId, data);
            });

            return Promise.resolve()
            .then(function () {
                return router0.listen(loopback)
                .then(function (addr) {
                    port0 = addr.port;
                    debug('port0 = ' + port0);
                });
            })
            .then(function () {
                return router1.listen(loopback)
                .then(function (addr) {
                    port1 = addr.port;
                    debug('port1 = ' + port1);
                });
            });
        });

        afterEach(function () {
            router0.close();
            router1.close();
            received0 = [];
            received1 = [];
            onRecv0 = null;
            onRecv1 = null;
        });

        it('Send a message, then the other respond', function (done) {
            var data = { msg: 'hello' };
            onRecv0 = function () {
                assert.equal(received1.length, 1);
                assert.equal(received0.length, 1);
                assert.deepEqual(received1[0], data);
                assert.deepEqual(received0[0], data);
                done();
            };
            router0.request({
                host: loopback,
                port: port1
            }, data);
        });

        it('#respond with no socket does nothing', function () {
            router0.respond(567, {});
        });
    });


    describe('Error tests', function () {
        var router;
        var port;
        var loopback = '127.0.0.1';
        var received = [];
        var socTimeout = 100;
        var net = require('net');

        beforeEach(function () {
            router = new Router({ socTimeout: socTimeout });

            router.on('request', function (data, requesterId) {
                debug('request received:', data);
                router.respond(requesterId, { loopback: true });
            });

            router.on('response', function (data) {
                debug('response received:', data);
                received.push(data);
            });

            return Promise.resolve()
            .then(function () {
                return router.listen(loopback)
                .then(function (addr) {
                    port = addr.port;
                    debug('port = ' + port);
                });
            });
        });

        afterEach(function () {
            router.close();
            received = [];
        });

        it('Bad JSON should cause disconnect by remote', function (done) {
            var soc = net.createConnection(port, loopback);
            soc.on('connect', function () {
                var pl = '{"bad":"JSON';
                var plBytes = Buffer.byteLength(pl);
                var buf = new Buffer(2 + plBytes);
                buf.writeUInt16BE(plBytes, 0);
                buf.write(pl, 2);
                soc.write(buf);
            });
            soc.on('end', function () {
                debug('test client socket: end event');
            });
            soc.on('timeout', function () {
                debug('test client socket: timeout event');
            });
            soc.on('error', function (err) {
                debug('test client socket:  error event: %s', err.message);
            });
            soc.on('close', function (hadError) {
                debug('test client socket: close event: hadError=%s', hadError);
                done();
            });
        });

        it('Server side socket timeout', function (done) {
            var since;
            var soc = net.createConnection(port, loopback);
            soc.on('connect', function () {
                since = Date.now();
                // Do nothing and wait for server side timeout.
            });
            soc.on('end', function () {
                debug('test client socket: end event');
            });
            soc.on('timeout', function () {
                debug('test client socket: timeout event');
            });
            soc.on('error', function (err) {
                debug('test client socket:  error event: %s', err.message);
            });
            soc.on('close', function (hadError) {
                debug('test client socket: close event: hadError=%s', hadError);
                var elapsed = Date.now() - since;
                debug('elapsed=' + elapsed);
                assert.ok(elapsed > socTimeout*2 - 10);
                assert.ok(elapsed < socTimeout*2 + 10);
                done();
            });
        });

        it('JSON parse error on client recieved should disconnect', function () {
            var data = { msg: 'hello' };
            var _JSON = JSON;
            var _parse = JSON.parse;
            sandbox.stub(JSON, 'parse', function (s) {
                var obj = _parse.call(_JSON, s);
                if (!obj.loopback) {
                    return obj;
                }
                throw new Error('Fake JSON parse error');
            });
            return router.request({
                host: loopback,
                port: port
            }, data)
            .then(function () {
                return Promise.delay(20);
            })
            .then(function () {
                assert.equal(received.length, 0);
                var nClientSockets = Object.keys(router._clientSockets).length;
                debug('nClientSockets = ' + nClientSockets);
                assert(!nClientSockets);
            });
        });
    });
});
