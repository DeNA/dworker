'use strict';

var Broker = require('..').Broker;
var Worker = require('..').Worker;
var Promise = require('bluebird');
var util = require('util');
var assert = require('assert');
var debug = require('debug')('dw:test');

describe('Worker tests', function () {
    var br;
    var brId = 'br01';

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
        } else if (method === 'getGreeting') {
            this.getGreeting(data, cb);
        } else if (method === 'die') {
            this.die(data, cb);
        } else {
            assert(!'Unexpected method: ' + method);
        }
    };
    MyWorker.prototype.onTell = function (method, data) {
        debug('Worker#onTell called (method=' + method + ')');
        if (method === 'greeting') {
            this._greeting = data; // just cache
        } else {
            assert(!'Unexpected method: ' + method);
        }
    };
    MyWorker.prototype.echo = function (data, cb) {
        debug('Worker#echo called.');
        cb(null, data);
    };
    MyWorker.prototype.getGreeting = function (data, cb) {
        debug('Worker#getGreeting called.');
        cb(null, this._greeting);
    };
    MyWorker.prototype.die = function (data, cb) {
        debug('Worker#die called.');
        this.destroy();
        cb(null, {});
    };


    before(function () {
        br = new Broker(brId, {
            retries: {
                initialInterval: 20,
                maxInterval: 200,
                duration: 500
            }
        });
        br.on('log', function (data) {
            debug('LOG(' + data.level + '): ' + data.message);
        });
        return br.start();
    });

    after(function () {
        return br.destroy({ noRecover: true });
    });

    beforeEach(function () {
    });

    afterEach(function () {
        delete MyWorker.classname;
        delete MyWorker.clustername;
    });

    describe('#registerWorker', function () {
        it('Worker name by ctor name', function () {
            br.registerWorker(MyWorker);
            assert(br._workerReg.get('MyWorker') === MyWorker);
        });

        it('Worker name by explicit name', function () {
            br.registerWorker('YourWorker', MyWorker);
            assert(br._workerReg.get('YourWorker') === MyWorker);
        });

        it('Worker name by $classname', function () {
            MyWorker.classname = 'HisWorker';
            br.registerWorker(MyWorker);
            assert(br._workerReg.get('HisWorker') === MyWorker);
        });

        it('Worker name by $clustername', function () {
            MyWorker.clustername = 'app';
            br.registerWorker(MyWorker);
            assert(br._workerReg.get('MyWorker').clustername === 'app');
        });

        it('Worker name by option.clustername', function () {
            MyWorker.clustername = 'app';
            br.registerWorker(MyWorker, { clustername:'gs' });
            assert(br._workerReg.get('MyWorker').clustername === 'gs');
        });

        it('Worker name by default', function () {
            br.registerWorker(MyWorker);
            assert(br._workerReg.get('MyWorker').clustername === 'main');
        });

        it('Worker name by default', function () {
            br.registerWorker(MyWorker);
            assert(br._workerReg.get('MyWorker').clustername === 'main');
        });

        it('Throws if the first arg is not string nor function', function () {
            assert.throws(function () {
                br.registerWorker(null);
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });

        it('Throws if the first arg is a function with no classname nor its name', function () {
            assert.throws(function () {
                br.registerWorker(function () {});
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });

        it('Throws if the first arg is an empty string', function () {
            assert.throws(function () {
                br.registerWorker('');
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });

        it('Throws if the second arg is not a ctor', function () {
            assert.throws(function () {
                br.registerWorker('foo', {});
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });

        it('Throws if the ctor is not a subclass of Worker', function () {
            assert.throws(function () {
                br.registerWorker('foo', function foo() {});
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });

        it('Throws if the ctor has agent property but is not a subclass of Agent', function () {
            function BadWorker() { Worker.apply(this, arguments); }
            util.inherits(BadWorker, Worker);
            BadWorker.agent = function () {};
            assert.throws(function () {
                br.registerWorker('BadWorker', BadWorker);
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });
    });

    describe('#setWorkerRegistry', function () {
        var stash;
        before(function () {
            stash = br._workerReg;
        });
        after(function () {
            br._workerReg = stash;
        });
        it('Use custom registry', function () {
            var _reg = {'PresetWorker': function() {}};
            var customReg = {
                add: function (name, ctor) {
                    _reg[name] = ctor;
                },
                get: function (name) {
                    return _reg[name];
                }
            };
            br.setWorkerRegistry(customReg);
            assert(typeof br._workerReg.get('PresetWorker') === 'function');
            br.registerWorker(MyWorker);
            assert(_reg['MyWorker'] === MyWorker);
        });

        it('Throws if the registry is not an object', function () {
            assert.throws(function () {
                br.setWorkerRegistry('bad');
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });

        it('Throws if the registry has no add function', function () {
            assert.throws(function () {
                br.setWorkerRegistry({});
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });

        it('Throws if the registry has no get function', function () {
            assert.throws(function () {
                br.setWorkerRegistry({ add: function () {} });
            }, function (err) {
                assert.strictEqual(err.name, 'Error');
                return true;
            }, 'should throw');
        });
    });

    describe('Create / Find worker', function () {
        it('Create worker', function (done) {
            br.registerWorker(MyWorker);
            br.createWorker('MyWorker', function (err, worker) {
                assert.ifError(err);
                assert(typeof worker === 'object');
                assert(typeof worker.ask === 'function');
                assert(typeof worker.tell === 'function');

                worker.ask('echo', {msg: 'yahoo'}, function (err, res) {
                    assert.ifError(err);
                    assert.strictEqual(res.msg, 'yahoo');
                    done();
                });
            });
        });

        it('Create worker 2', function (done) {
            var attributes = {
                magicwd: 'please',
                info: {
                    age: 29,
                    single: true
                }
            };
            br.registerWorker(MyWorker);
            br.createWorker('MyWorker', { attributes: attributes }, function (err, worker) {
                assert.ifError(err);
                assert(typeof worker === 'object');
                assert(typeof worker.ask === 'function');
                assert(typeof worker.tell === 'function');

                // We know the actual worker is created on this sole broker.
                // Test wether the attributes have been passed to the new worker.
                var actualWorker = br._workers[worker.id];
                assert(actualWorker);
                assert.deepEqual(attributes, actualWorker.attributes);
                done();
            });
        });

        it('Create worker with option.id', function (done) {
            var id = 'my-worker-123';
            br.registerWorker(MyWorker);
            br.createWorker('MyWorker', { id: id }, function (err, worker) {
                assert.ifError(err);
                assert.strictEqual(worker.id, id);
                assert(typeof worker === 'object');
                assert(typeof worker.ask === 'function');
                assert(typeof worker.tell === 'function');

                var actualWorker = br._workers[worker.id];
                assert(actualWorker);
                assert.strictEqual(actualWorker.id, id);
                done();
            });
        });

        it('Find worker', function (done) {
            br.registerWorker(MyWorker);
            br.createWorker('MyWorker', function (err, worker) {
                assert.ifError(err);
                assert(typeof worker === 'object');
                assert(typeof worker.ask === 'function');
                assert(typeof worker.tell === 'function');

                br.findWorker(worker.id, function (err, worker2) {
                    assert.ifError(err);
                    assert(typeof worker2 === 'object');
                    assert(typeof worker2.ask === 'function');
                    assert(typeof worker2.tell === 'function');

                    worker2.ask('echo', {msg: 'yahoo'}, function (err, res) {
                        assert.ifError(err);
                        assert.strictEqual(res.msg, 'yahoo');
                        done();
                    });
                });
            });
        });

        it('Tell test', function () {
            var agent;
            br.registerWorker(MyWorker);
            return br.createWorker('MyWorker')
            .then(function (_agent) {
                agent = _agent;
                return agent.tell('greeting', {msg:'hi'});
            })
            .then(function () {
                return agent.ask('getGreeting', {});
            })
            .then(function (res) {
                assert.equal(res.msg, 'hi');
            });
        });

        it('findWorker for non-existing worker should return null', function () {
            return br.findWorker('FakeWorker')
            .then(function (agent) {
                assert.strictEqual(agent, null);
            });
        });

        it('Find already destroyed worker should return null', function (done) {
            br.registerWorker(MyWorker);
            br.createWorker('MyWorker', function (err, worker) {
                assert.ifError(err);
                assert(typeof worker === 'object');
                assert(typeof worker.ask === 'function');
                assert(typeof worker.tell === 'function');

                worker.ask('die', {}, function (err) {
                    assert.ifError(err);
                    worker.ask('echo', {}, function (err, res) {
                        void(res);
                        assert.strictEqual(err.name, 'Error');
                        br.findWorker(worker.id, function (err, agent) {
                            assert.ifError(err);
                            assert.strictEqual(agent, null);
                            done();
                        });
                    });
                });
            });
        });
    });

    describe('Lifecycle events', function () {
        var transition = [];
        var onInitHandler = null;
        var onCreateHandler = null;
        var onDestroyHandler = null;

        function HisWorker() {
            Worker.apply(this, arguments);
            onInitHandler.call(this);
        }

        util.inherits(HisWorker, Worker);

        HisWorker.prototype.onCreate = function (info, cb) {
            debug('onCreate called');
            transition.push('create');
            onCreateHandler.call(this);
            cb();
        };
        HisWorker.prototype.onDestroy = function (info, cb) {
            debug('onDestroy called:', transition);
            transition.push('destroy');
            debug('onDestroy called2:', transition);
            onDestroyHandler.call(this);
            debug('onDestroy called3:', transition);
            cb();
        };

        beforeEach(function () {
            transition = [];
        });

        it('destroy immediately after instantiation', function (done) {
            var workerId;
            // Setup handler
            onInitHandler = function () {
                workerId = this.id;
                debug('instantiated');
                this.destroy();
            };
            onCreateHandler = function () {
                debug('onCreateHandler called');
            };
            onDestroyHandler = function () {
                debug('onDestroyHandler called');
                assert.deepEqual(transition, ['create', 'destroy']);
                done();
            };

            // Register, then create a worker
            br.registerWorker(HisWorker);
            debug('Calling createWorker');
            br.createWorker('HisWorker', function (err, agent) {
                assert.ifError(err);
                assert(typeof agent === 'object');
                assert.strictEqual(agent.id, workerId);
            });
        });

        it('destroy immediately after onCreate', function (done) {
            var workerId;
            // Setup handler
            onInitHandler = function () {
                workerId = this.id;
            };
            onCreateHandler = function () {
                this.destroy();
                debug('onCreateHandler called');
            };
            onDestroyHandler = function () {
                debug('onDestroyHandler called');
                assert.deepEqual(transition, ['create', 'destroy']);
                done();
            };

            // Register, then create a worker
            br.registerWorker(HisWorker);
            br.createWorker('HisWorker', function (err, agent) {
                assert.ifError(err);
                assert(typeof agent === 'object');
                assert.strictEqual(agent.id, workerId);
            });
        });

        it('destroy immediately while destroying', function (done) {
            var workerId;
            // Setup handler
            onInitHandler = function () {
                workerId = this.id;
            };
            onCreateHandler = function () {
                this.destroy();
                this.destroy(); // calling twice
                debug('onCreateHandler called');
            };

            onDestroyHandler = function () {
                debug('onDestroyHandler called');
            };

            // Register, then create a worker
            br.registerWorker(HisWorker);
            br.createWorker('HisWorker', function (err, agent) {
                assert.ifError(err);
                assert(typeof agent === 'object');
                assert.strictEqual(agent.id, workerId);
                setTimeout(function () {
                    assert.deepEqual(transition, ['create', 'destroy']);
                    done();
                }, 100);
            });
        });
    });

    describe('Worker error tests', function () {
        var agent;
        var transition = [];

        function HerWorker() {
            Worker.apply(this, arguments);
        }
        util.inherits(HerWorker, Worker);
        HerWorker.prototype.onAsk = function (method, data) {
            void(method);
            void(data);
            return Promise.reject(new Error("Don't ask me"));
        };

        beforeEach(function () {
            transition = [];
            br.registerWorker(HerWorker);
            return br.createWorker('HerWorker', function (err, _agent) {
                assert.ifError(err);
                agent = _agent;
            });
        });

        it('check error return', function () {
            return agent.ask('what', {})
            .then(function () {
                throw new Error('should fail');
            },
            function (err) {
                assert(err);
                assert.strictEqual(err.name, 'Error');
                assert.strictEqual(err.message, "Don't ask me");
            });
        });

        it('#onAsk (base) should throw if not overridden', function () {
            return Worker.prototype.onAsk('laugh', {})
            .then(function () {
                throw new Error('Should throw');
            },function (e) {
                assert.equal(e.name, 'Error');
            });
        });

        it('#onTell (base) does nothing ', function () {
            Worker.prototype.onTell('laugh', {});
        });

        it('Construct with not attributes put {}', function () {
            var w = new MyWorker('me', {
                _updateLoad: function () {
                }
            });
            assert.ok(w.attributes !== null);
            assert.ok(typeof w.attributes, 'object');
        });

        it('#load() will be ignored when newLoad is negative', function () {
            var numCalled = 0;
            var w = new MyWorker('me', { _updateLoad: function () {
                numCalled++;
            }});
            w.load = -1;
            assert.equal(numCalled, 0);
        });

        it('#load() will be ignored if state is DESTROYED', function () {
            var numCalled = 0;
            var w = new MyWorker('me', { _updateLoad: function () {
                numCalled++;
            }});
            w.__priv.state = Worker.State.DESTROYED;
            w.load = 10;
            assert.equal(numCalled, 0);
        });
    });
});
