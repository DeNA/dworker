'use strict';

var util = require('util');
var events = require('events');
var _ = require('lodash');
var scripts = require('./scripts');
var debug = require('debug')('dw:broker');
var redis = require('redis');
var Worker = require('./worker').Worker;
var Agent = require('./agent').Agent;
var Router = require('./router').Router;
var Const = require('./common').Const;
var SafeCyclicCounter = require('./common').SafeCyclicCounter;
var generateHashKey = require('./common').generateHashKey;
var NOOP = require('./common').NOOP;
var Promise = require('bluebird');

/**
 * An enumeration of {@link Broker} states
 * @memberof Broker
 * @enum {number}
 * @constant
 */
var State = {
    /** Broker is inactive */
    INACTIVE: 0,
    /** Broker is activating */
    ACTIVATING: 1,
    /** Broker is active */
    ACTIVE: 2,
    /** Broker is being destroyed */
    DESTROYING: 3,
    /** Broker has been destroyed */
    DESTROYED: 4
};

// Signal ID via redis pubsub
var SIG_RECOVER  = 'recover';
var SIG_SALVAGE  = 'salvage';
var SIG_RESTART  = 'restart';

// Max number of message to read from the queue.
var DEFAULT_BATCH_READ_SIZE = 1000;

// Periodic timer for #_onTimer() call.
var TIMER_INTERVAL = 1000;

// Redis time sync interval.
var TIME_SYNC_INTERVAL = 30000;

// defaults
var defaults = {
    ns: 'dw',
    rpcTimeout: 3000,     // in milli-seconds
    batchReadSize: DEFAULT_BATCH_READ_SIZE,
    clustername: 'main',
    redis: {
        host: '127.0.0.1',
        port: 6379
    },
    brokerCache: {
        max: 1000,
        maxAge: 30000
    },
    ttl: 0, // Worker's max lifetime. Defaults to 0 (unlimited)
    retries: {
        initialInterval: 40,
        maxInterval: 800,
        duration: 2000
    },
    socTimeout: 15000,
    healthCheckInterval: 1 // in seconds, 0 to dsiable.
};

/** @private */
function defaultWorkerRegistry() {
    var _registry = {};
    return {
        add: function add(name, ctor) {
            _registry[name] = ctor;
        },
        get: function get(name) {
            return _registry[name];
        }
    };
}

/** @private */
function promisifyCb(func, thisObj) {
    return function () {
        var args = arguments;
        return new Promise(function (resolve, reject) {
            function tmpCb(err, res) {
                if (err) {
                    return reject(err);
                }
                resolve(res);
            }
            args[args.length++] = tmpCb;
            var p;
            try {
                p = func.apply(thisObj, args);
            } catch (e) {
                return reject(e);
            }
            if (p) {
                /* istanbul ignore else */
                if (typeof p.then === 'function') {
                    p.nodeify(tmpCb);
                }
                else {
                    debug('promisifyCb: unexpected non-promise value returned');
                }
            }
        });
    };
}

/**
 * A function to be retried by {@link backoffRetry}.
 * With this function, the `func` passed in will be retried until the given
 * function succeeds, or hit the retry timeout condition specified via option.
 * The retries take place with a back-off algorithm (doubling the previous
 * interval time) until the given function succeeds, or the retries were
 * attempted for more than the time specified by option.duration. If all
 * retries fail, then this function return the last error returned by the
 * given function, `func`.
 * @callback backoffFunc
 * @param {number} nRetries Current ordinal number of this function call. The
 * number for the initial call will be 0.
 */
/**
 * Retries func call for specified duration with backoff.
 * @private
 * @param {object} option
 * @param {number} option.initialInterval Initial interval time (msec) for retries.
 * @param {number} option.maxInterval Max interval time (msec) for retries.
 * @param {number} option.duration Max duration (msec) of the retries.
 * @param {backoffFunc} func User-defined function that will be retried.
 * @example
 * var option = {
 *   initialInterval: 20,
 *   maxInterval: 1000,
 *   duration: 5000
 * };
 */
function backoffRetry(option, func) {
    var retries = 0;
    debug('backoffRetry: option:', option);
    var interval = option.initialInterval;
    var since = Date.now();
    function loop() {
        return func(retries)
        .catch(function (e) {
            var elapsed = Date.now() - since;
            if (elapsed >= option.duration) {
                throw e; // return the last error
            }
            retries++;
            var msec = Math.min(interval, elapsed + 10);
            interval = Math.min(interval*2, option.maxInterval);
            debug('since          : ' + since);
            debug('elapsed        : ' + elapsed);
            debug('interval       : ' + interval);
            debug('retry wait time: ' + msec);
            return Promise.delay(msec).then(function () { return loop(); });
        });
    }
    return loop();
}

////////////////////////////////////////////////////////////////////////////////
// Public section

/**
 * Broker class.
 * @constructor
 * @param {string} id A unique broker ID.
 * @param {object} [option] Options.
 * @param {string} [option.ns] Namespace used for redis keys. Default is 'dw'.
 * @param {string} [option.clustername] Name of cluster this broker will belong to. Defaults to 'main'.
 * @param {object} [option.redis] Redis configuration.
 * @param {object} [option.redis.pub] Redis client instance for standard operation.
 * @param {object} [option.redis.sub] Redis client instance for subscription.
 * @param {number} [option.redis.port] Redis server's port number (defaults to 6379).
 * @param {string} [option.redis.host] Redis server's IP address (defaults to '127.0.0.1').
 * @param {number} [option.rpcTimeout] RPC transaction timeout. Defaults to 3000 msec.
 * @param {number} [option.ttl] Recovery timeout. Defaults to 0 (unlimited). This value
 * does not affect active workers. It is only referenced during the recovery process.
 * A worker, even with recoverable flag set to true, won't be recovered if it's beyond its TTL.
 * @param {number} [option.batchReadSize] The number of messages to read from message queue at a time. Defaults to 1000.
 * @param {object} [option.brokerCache] Broker cache (LRU) configuration.
 * @param {number} [option.brokerCache.max] Max number of workers LRU cache can remember
 * to resolve broker ID (by worker ID).
 * @param {number} [option.brokerCache.maxAge] Max number of milliseconds in which an entry
 * in the cache will be invalidated.
 * @param {object} [option.retries] Retry behavior configuration.
 * @param {number} [option.retries.initialInterval] Initial interval in msec. Defaults to 40.
 * @param {number} [option.retries.maxInterval] Max interval in msec. Defaults to 800.
 * @param {number} [option.retries.duration] Max duration in msec, in which the retries will stop.
 * @param {number} [option.healthCheckInterval] Health-check interval in seconds. Defaults to 1.
 */
function Broker(id, option) {
    var self = this;
    this._id = id;
    this._option = _.defaults(option || {}, defaults);
    this._state = State.INACTIVE;
    this._workerReg = defaultWorkerRegistry();
    this._workers = {};
    this._load = 0;
    this._loadUpdated = false;
    // RPC sequence number originated from this broker
    this._seqCounter = new SafeCyclicCounter(Math.floor(Math.random() * (Const.MAX_SAFE_INTEGER+1)));
    this._cbs = {}; // RPC callback list (key: sequence number)
    this._cbList = []; // RPC callback list (ordered by time, each item: [ msec, seq ]
    this._timer = null;
    this._timeOffset = 0;
    this._lastTimeSync = 0;
    this._chPrefix = this._option.ns + ':ch'; // subscriber channel prefix
    this._healthCheckIn = this._option.healthCheckInterval;

    if (this._option.redis.pub) {
        this._pub = this._option.redis.pub;
    } else {
        this._pub = redis.createClient(this._option.redis.port, this._option.redis.host, {});
    }
    if (this._option.redis.sub) {
        this._sub = this._option.redis.sub;
    } else {
        this._sub = redis.createClient(this._option.redis.port, this._option.redis.host, {});
    }

    // Promisify redis client
    Promise.promisifyAll(this._pub);

    this._sub.on("message", this._onSubMessage.bind(this));

    this._keys = {
        gh: this._option.ns + ':gh', // global hash
        wh: this._option.ns + ':wh', // worker hash
        bh: this._option.ns + ':bh', // broker hash
        cz: this._option.ns + ':cz', // followed by ':'+clustername
        bz: this._option.ns + ':bz', // followed by ':'+clustername
        wz: this._option.ns + ':wz', // followed by ':'+brId
        rz: this._option.ns + ':rz'  // recovery list
    };
    this._chBc = self._chPrefix + ':*';
    this._chUc = self._chPrefix + ':' + id;
    this._subscribed = [];

    // Broker info cache ('lru-cache')
    this._brokerCache = require('lru-cache')(this._option.brokerCache);
    this._brokerCacheHits = 0;
    this._brokerCacheMisses = 0;

    // Instantiate Router
    this._router = new Router({ socTimeout: this._option.socTimeout });
    this._router.on('request', this._onRequest.bind(this));
    this._router.on('response', this._onResponse.bind(this));
    this._router.on('disconnect', this._onRemoteDisconnect.bind(this));

    // Define getter 'id'
    this.__defineGetter__("id", function () {
        return this._id;
    });

    // Define getter 'state'
    this.__defineGetter__("state", function () {
        return this._state;
    });
    // Define getter 'rpcTimeout'
    this.__defineGetter__("rpcTimeout", function () {
        return this._option.rpcTimeout;
    });
    // Define getter 'clustername'
    this.__defineGetter__("clustername", function () {
        return this._option.clustername;
    });
    // Define getter 'pub'
    this.__defineGetter__("pub", function () {
        return this._pub;
    });
    // Define getter 'sub'
    this.__defineGetter__("sub", function () {
        return this._sub;
    });
    // Define getter 'load'
    this.__defineGetter__("load", function () {
        return this._load;
    });
    // Getter for redis time.
    this.__defineGetter__("redisTime", function () {
        return Date.now() + this._timeOffset;
    });

    // Logger
    this.logger = {};
    ['debug', 'warn', 'info', 'error'].forEach(function (level) {
        self.logger[level] = function () {
            self.emit('log', { id: self.id, level: level, message: util.format.apply(self, arguments) });
        };
    });
    this._router.on('log', function (logMsg) {
        logMsg.id = self.id;
        self.emit('log', logMsg );
    });
}

util.inherits(Broker, events.EventEmitter);

/**
 * Callback function for {@link Broker.prototype.start}.
 * @callback Broker~startCallback
 * @param {Error} err Error object.
 * @param {object} data Request data.
 */
/**
 * Start this broker.
 * @param {Broker~startCallback} [cb] Callback function.
 * @returns {Promise} Returns a promise if `cb` is not provided.
 * See {@link Broker~startCallback}.
 */
Broker.prototype.start = function (cb) {
    if (this._state !== State.INACTIVE && this._state !== State.DESTROYED) {
        return Promise.reject(new Error('start() not allowed: state=' + this._state))
        .nodeify(cb);
    }

    var self = this;
    this.logger.info('Broker (' + this.id + ') is starting');

    if (!this._pub) {
        return Promise.reject(new Error('Can not start without redis client (pub)'))
        .nodeify(cb);
    }
    if (!this._sub) {
        return Promise.reject(new Error('Can not start without redis client (sub)'))
        .nodeify(cb);
    }

    this._syncRedisTime(Date.now());

    this._setState(State.ACTIVATING);

    return this._loadScripts()
    .then(function () {
        // Obtain local IP address from redis connection.
        var localAddr = self._pub.stream.address().address;
        // Pass the address to router to listen for it.
        return self._router.listen(localAddr)
        .then(function (addr) {
            if (addr.address !== localAddr) {
                self._router.close();
                throw new Error('Failed to open TCP listener');
            }
            // Construct <ip-address>:<port> for later use.
            self._addr = localAddr + ':' + addr.port;
            self.logger.info('Broker (' + self.id + ') is listening on ' + self._addr);
        });
    })
    .then(function () {
        return new Promise(function (resolve) {
            self._sub.subscribe(self._chBc, self._chUc, NOOP);
            self._sub.on('subscribe', function (ch, count) {
                self._subscribed.push(ch);
                if (count === 2) {
                    self._sub.removeAllListeners('subscribe');
                    resolve();
                }
            });
        });
    })
    .then(function () {
        return self._pub.evalshaAsync(
            scripts.addBroker.sha,
            7,
            self._keys.gh,
            self._keys.wh,
            self._keys.bh,
            self._keys.cz + ':' + self.clustername,
            self._keys.bz + ':' + self.clustername,
            self._keys.wz + ':' + self.id,
            self._keys.rz,
            self.id,
            self._chPrefix,
            self._load,
            self.clustername,
            self._addr,
            generateHashKey(self.id)
        );
    })
    .then(function (res) {
        if (!res || !Array.isArray(res)) {
            debug('addBroker result must be an array');
            throw new Error('Internal error');
        }

        if (res[0] !== 0) {
            debug('addBroker: unsupported result code ' + res[0]);
            throw new Error('Internal error');
        }

        self._timer = setTimeout(self._onTimer.bind(self), TIMER_INTERVAL);
    })
    .then(function () {
        self._setState(State.ACTIVE);
        self.logger.info('Broker (' + self.id + ') started');
    }, function (err) {
        self.logger.error('Failed to start: ' + err.message);
        self._setState(State.INACTIVE);
        return new Promise(function (resolve, reject) {
            void(resolve);
            if (self._subscribed.length === 0) {
                return reject(err);
            }
            self._sub.unsubscribe(self._subscribed, NOOP);
            self._sub.on('unsubscribe', function (ch, count) {
                debug('on unsubscribe: ch=' + ch + ' count=' + count);
                if (count === 0) {
                    self._sub.removeAllListeners('unsubscribe');
                    reject(err);
                }
            });
            self._subscribed = [];
        });
    })
    .nodeify(cb);
};

/**
 * Set a custom worker class registry.
 * @param {object} registry Custom registry to be used. The object
 * must support the following methods:
 * - add(name, constructor)
 * - get(name, constructor)
 * @returns {void}
 */
Broker.prototype.setWorkerRegistry = function (registry) {
    if (typeof registry !== 'object') {
        throw new Error('Invalid argument');
    }
    if (typeof registry.add !== 'function') {
        throw new Error('Custom registry must support `add` method');
    }
    if (typeof registry.get !== 'function') {
        throw new Error('Custom registry must support `get` method');
    }
    this._workerReg = registry;
};

/**
 * Register a worker class.
 * @param {string} [name] Name of the worker class. If omitted, the name
 * will be ctor.classname or ctor.name (the function name of the ctor).
 * @param {function} ctor Constructor of the worker to be registered.
 * @param {object} [option] Option.
 * @param {string} [option.clustername] Name of cluster the worker belongs to.
 * If this option is omitted, it defaults to 'main'.
 * @returns {void}
 */
Broker.prototype.registerWorker = function (name, ctor, option) {
    if (typeof name !== 'string') {
        if (typeof name !== 'function') {
            throw new Error('Invalid argument');
        }
        option = ctor;
        ctor = name;
        name = ctor.classname || ctor.name;
        if (!name) {
            throw new Error('Invalid worker name');
        }
    } else {
        if (!name) {
            throw new Error('Worker name cannot be empty');
        }
        if (typeof ctor !== 'function') {
            throw new Error('Invalid worker class');
        }
    }

    // Check if the given worker is a subclass of Worker.
    if (!(ctor.prototype instanceof Worker)) {
        throw new Error('The given constructor is not a subclass of Worker');
    }

    if (ctor.agent) {
        if (ctor.agent !== Agent) {
            if (!(ctor.agent.prototype instanceof Agent)) {
                throw new Error('The given `agent` is not a subclass of Agent');
            }
        }
    }

    if (option && option.clustername) {
        ctor.clustername = option.clustername;
    } else {
        if (!ctor.clustername) {
            ctor.clustername = 'main';
        }
    }

    // Notice: if already exists, this overwrites the old.
    this._workerReg.add(name, ctor);
};

/**
 * Callback function for Broker#createWorker.
 * @callback Broker~createWorkerCallback
 * @param {Error} err Error object.
 * @param {Agent} agent An instance for remote Worker (called 'Agent') via which you can
 * access remote worker that has just been instantiated.
 */
/**
 * Create a worker.
 * @param {string} workerName Name of the worker to be created.
 * @param {object} [option] Options.
 * @param {string} [option.id] Worker ID to be assigned. If not specified and the worker
 * is dynamic (option.static is set to false), then dworker will assign a unique ID for
 * the new worker.
 * @param {string} [option.static] If "static", then this method will create
 * a static worker (only one instance in the cluster).
 * @param {object} [option.attributes] A bag of parameters passed to the new worker
 * created on another process (broker). Defaults to false.
 * which can be referenced by Worker#attributes.<param-name>.
 * @param {boolean} [option.attributes.recoverable] Mark the new worker as
 * recoverable. When a process (broker) dies, workers marked as recoverable
 * may be reconstructed if condition allows.
 * @param {Broker~createWorkerCallback} [cb] Callback.
 * @returns {Promise} Returns a promise if `cb` is not provided.
 * See {@link Broker~createWorkerCallback}.
 */
Broker.prototype.createWorker = function (workerName, option, cb) {
    var self = this;

    if (typeof option !== 'object') {
        cb = option;
        option = {};
    }

    // The option.cause is set to Worker.CreateCause.New by default.
    if (typeof option.cause !== 'number') {
        option.cause = Worker.CreateCause.NEW;
    }

    // Make sure attributes object is always present.
    if (!option.attributes) {
        option.attributes = {};
    }

    // Make sure `id` of type string is always present..
    if (typeof option.id !== 'string') {
        option.id = '';
    }

    // Make sure attributes.static property is always present.
    option.attributes.static = !!option.static;

    // Check if the worker class is registered.
    var kWorker = self._workerReg.get(workerName);
    if (!kWorker) {
        return Promise.reject(new Error('Worker, ' + workerName + ' is not registered'))
        .nodeify(cb);
    }

    var clustername = option.cn || kWorker.clustername;

    return self._pub.evalshaAsync(
        scripts.getLeastLoadedBroker.sha,
        5,
        self._keys.gh,
        self._keys.wh,
        self._keys.bh,
        self._keys.cz+':'+ clustername,
        self._keys.bz+':'+ clustername
    )
    .then(function (res) {
        if (!res) {
            throw new Error('No broker is available in the cluster ' + clustername);
        }
        if (!Array.isArray(res)) {
            throw new Error('Internal error');
        }
        if (typeof res[0] !== 'string' ||
            typeof res[1] !== 'string' ||
            typeof res[2] !== 'string') {
            throw new Error('Internal error');
        }
        option.name = workerName;
        // createWorker won't use res[1] (clustername)

        var addr = res[2].split(':');

        return self._requestToBroker(
            '_onCreateWorker',
            option,
            { host: addr[0], port: parseInt(addr[1]) }
        );
    })
    .then(function (winfo) {
        var Ctor = kWorker.agent? kWorker.agent:Agent;
        return new Ctor(winfo.workerId, self, winfo.brokerId);
    })
    .nodeify(cb);
};

/**
 * Callback function for Broker#findWorker.
 * @callback Broker~findWorkerCallback
 * @param {Error} err Error object.
 * @param {Agent} agent An instance for remote Worker (called 'Agent') via which you can
 * access remote worker that has just been instantiated. If the worker does not exist,
 * the returned agent will be null.
 */
/**
 * Find worker.
 * @param {string} workerId ID of a worker to find. If the worker is a dynamic worker,
 * then the ID should look like: "MyWorker#12" as in the form of <worker-name>#<instance-num>.
 * If the worker is static, workerId is identical to the worker name. (no "#<instance-num>.
 * @param {Broker~findWorkerCallback} [cb] Callback.
 * @returns {Promise} Returns a promise if `cb` is not provided.
 * See {@link Broker~findWorkerCallback}.
 */
Broker.prototype.findWorker = function (workerId, cb) {
    var self = this;
    return backoffRetry(this._option.retries, function (nRetries) {
        debug('findWorker(): retries=' + nRetries);
        return self._pub.evalshaAsync(
            scripts.findOrCreateWorker.sha,
            7,
            self._keys.gh,
            self._keys.wh,
            self._keys.bh,
            self._keys.cz + ':' + self.clustername,
            self._keys.bz + ':' + self.clustername,
            self._keys.wz + ':' + self.id,
            self._keys.rz,
            '', // empty broker ID => only performes 'find'.
            '', // empty worker name => wont' be used.
            workerId,
            '', // empty attributes => won't be used.
            0, // redis time => won't be used.
            0, // `ttl` => won't be used.
            0  // `forRecovery` => won't be used.
        )
        .then(function (res) {
            debug('findWorker: findOrCreateWorker result:', res);
            if (!res || !Array.isArray(res)) {
                // This should never happen.
                self.logger.error('Unexpected result from findOrCreateWorker.lua:', res);
                throw new Error('Internal error (invalid result from lua)');
            }

            var ret;

            switch (res[0]) {
                case 0:
                    // Successful case.
                    if (!res[1]) {
                        ret = null; // The worker not found
                    } else if (!Array.isArray(res[1]) || res[1].length < 2) {
                        self.logger.error('Unexpected result from findOrCreateWorker.lua:', res);
                        throw new Error('Internal error (invalid result value from lua)');
                    } else {
                        var brokerId = res[1][0];
                        var workerName = res[1][1];

                        // Check if you have the class registered.
                        var k = self._workerReg.get(workerName);
                        if (!k) {
                            throw new Error('Worker, ' + workerName + ' is not registered');
                        }
                        var Ctor = k.agent? k.agent:Agent;
                        ret = new Ctor(workerId, self, brokerId);
                    }
                    break;

                case 1:
                    throw new Error('Try again');

                default:
                    self.logger.error("Unexpected result code: " + res[0]);
                    ret = null;
            }

            return ret;
        });
    })
    .nodeify(cb);
};

/**
 * Callback function for Broker#destroyWorker.
 * @callback Broker~destroyWorkerCallback
 * @param {Error} err Error object.
 */
/**
 * Destroy the broker.
 * @param {object} [option] Options.
 * @param {boolean} [option.noRecover] Tell the broker not to recover any
 * workers (regardless of 'recoverable' option) this broker current have.
 * @param {Broker~destroyWorkerCallback} [cb] Callback.
 * @returns {Promise} Returns a promise if `cb` is not provided.
 * See {@link Broker~destroyWorkerCallback}.
 */
Broker.prototype.destroy = function (option, cb) {
    this.logger.info('Broker (' + this.id + ') is being destroyed: state=' + this._state);

    if (typeof option !== 'object') {
        cb = option; // assume cb
        option = {};
    }

    if (this._state === State.INACTIVE) {
        return Promise.resolve()
        .nodeify(cb);
    }
    if (this._state === State.ACTIVATING) {
        return Promise.reject(new Error('Cannot destroy while broker is starting'))
        .nodeify(cb);
    }
    if (this._state === State.DESTROYING) {
        return Promise.reject(new Error('Already destroying'))
        .nodeify(cb);
    }
    if (this._state === State.DESTROYED) {
        return Promise.reject(new Error('Already destroyed'))
        .nodeify(cb);
    }

    option = _.defaults(option || {}, { noRecover: false });

    this._setState(State.DESTROYING);

    // Call onDestroy() on all the workers first.
    var self = this;
    var wIds = Object.keys(this._workers);
    var promises = [];
    wIds.forEach(function (id) {
        var worker = self._workers[id];
        promises.push(promisifyCb(worker.onDestroy, worker)({
            cause: Worker.DestroyCause.SYSTEM
        }));
    });

    // Close router first.
    this._router.close();

    return Promise.all(promises)
    .catch(function () {}) // ignore the errors onDestroy returned
    .then(function () {
        // Lay off all workers and get them garbage-collected
        // (as the broker instance may be hanging around until process exits)
        self._workers = [];

        var promises = [];
        /* istanbul ignore else */
        if (self._pub && self._pub.connected) {
            promises.push(self._pub.zremAsync(self._keys.cz+':'+self.clustername, self.id));
            promises.push(self._pub.zremAsync(self._keys.bz+':'+self.clustername, self.id));
        }
        /* istanbul ignore else */
        if (self._sub && self._sub.connected) {
            promises.push(new Promise(function (resolve) {
                self._sub.unsubscribe(self._chBc, self._chUc);
                self._sub.on('unsubscribe', function (ch, count) {
                    debug('on unsubscribe: ch=' + ch + ' count=' + count);
                    if (count === 0) {
                        self._sub.removeAllListeners('unsubscribe');
                        resolve();
                    }
                });
            }));
        }

        // Remove self from cz table so that others won't find this broker
        // Also, unsubscribe pubsub channels at the same time
        return Promise.all(promises);
    })
    .then(function () {
        /* istanbul ignore if */
        if (!self._pub || !self._pub.connected) {
            return;
        }

        // Kick salvage process so that others can find the worker info
        // in rz table right away during their findWorker operation.
        return self._pub.evalshaAsync(
            scripts.salvageWorkers.sha,
            7,
            self._keys.gh,
            self._keys.wh,
            self._keys.bh,
            self._keys.cz + ':' + self.clustername,
            self._keys.bz + ':' + self.clustername,
            self._keys.wz + ':' + self.id,
            self._keys.rz,
            self.id,
            option.noRecover? 2:1
        );
    })
    .finally(function () {
        if (self._timer) {
            clearTimeout(self._timer);
            self._timer = null;
        }
        self._brokerCache.reset();

        self._setState(State.DESTROYED);
        self.logger.info('Broker (' + self.id + ') has been destroyed');
    })
    .nodeify(cb);
};

/**
 * Callback function for {@link Broker.prototype.restart}.
 * @callback Broker~restartCallback
 * @param {Error} err Error object.
 * @param {object} data Request data.
 */
/**
 * Restart dworker. (Adminstrative purpose only)
 * All active workers will be removed with no recovery.
 * @param {Broker~restartCallback} [cb] Callback function.
 * @returns {Promise} Returns a promise if `cb` is not provided.
 * See {@link Broker~restartCallback}.
 */
Broker.prototype.restart = function (cb) {
    var self = this;
    return this.destroy({ noRecover: true })
    .then(function () {
        return self.start();
    })
    .nodeify(cb);
};

/**
 * Trigger restart of all active brokers under the same namespace.
 * This method does not wait for completion of actual restart. This
 * method just signals `restart` command to all active brokers.
 */
Broker.prototype.restartAll = function () {
    if (!this._pub || !this._pub.connected) {
        this.logger.error('No connection to redis-server');
        return;
    }

    this._pub.publish(this._chBc, JSON.stringify({
        sig: SIG_RESTART
    }));
};

/**
 * Quit redis clients.
 * This method is provided so that we can make sure to quit redis connections,
 * or to do so for testing. However, the redis clients' lifetime may be
 * managed by other place. Therefore, use this method with a care.
 */
Broker.prototype.quit = function () {
    if (this._pub && this._pub.connected) {
        this._pub.quit();
    }
    if (this._sub && this._sub.connected) {
        this._sub.quit();
    }
};


////////////////////////////////////////////////////////////////////////////////
// Private section

Broker.prototype._loadScripts = function () {
    var self = this;
    return new Promise(function (resolve, reject) {
        if (!self._lur) {
            self._lur = require('lured').create(self._pub, scripts);
            self._lur.load(function (err) {
                if (err) {
                    self.logger.error('Lua loading failed: ' + err.message);
                    return void(reject(err));
                }
                debug(self.id +': lua loading successful');
                debug(self.id +': addBroker.lua: ', scripts.addBroker.sha);
                debug(self.id +': getLeastLoadedBroker.lua: ', scripts.getLeastLoadedBroker.sha);
                debug(self.id +': findOrCreateWorker.lua: ', scripts.findOrCreateWorker.sha);
                debug(self.id +': findBroker.lua: ', scripts.findBroker.sha);
                debug(self.id +': salvageWorkers.lua: ', scripts.salvageWorkers.sha);
                debug(self.id +': fetchWorkersToRecover.lua: ', scripts.fetchWorkersToRecover.sha);
                debug(self.id +': destroyWorker.lua: ', scripts.destroyWorker.sha);
                resolve();
            });
        } else {
            resolve();
        }
    });
};

/**
 * Find broker by worker ID.
 * @private
 * @param {string} workerId Worker ID.
 * @returns {Promise} Returns a promise. Resolved value will be an object that has
 * the following properties:
 * - {string} brokerId Broker ID that has the worker with the worker ID.
 * - {string} clustername A name of cluster that the broker belongs to.
 * - {string} status Status of the broker: 'active', 'invalid' or 'migrating'
 * If the broker is not found, the resolved value will be null.
 * The promise will be rejected if failed with communicating with redis.
 */
Broker.prototype._findBroker = function (workerId) {

    var brInfo = this._brokerCache.get(workerId);
    if (brInfo) {
        this._brokerCacheHits++;
        return Promise.resolve(brInfo);
    }

    this._brokerCacheMisses++;

    var self = this;
    return self._pub.evalshaAsync(
        scripts.findBroker.sha,
        3,
        self._keys.gh,
        self._keys.wh,
        self._keys.bh,
        self.id,
        workerId
    )
    .then(function (res) {
        if (!res) {
            self._brokerCache.del(workerId);
            return null;
        }

        if (!Array.isArray(res)) {
            debug('Error: non-null result must be an array');
            self._brokerCache.del(workerId);
            return null;
        }

        if (typeof res[0] !== 'number') {
            debug('Error: non-null result must be an array');
            self._brokerCache.del(workerId);
            return null;
        }

        if (res[0] !== 0) {
            // 1: no broker
            // 2: retry
            self._brokerCache.del(workerId);
            return null;
        }

        if (!Array.isArray(res[1])) {
            debug('Error: non-null result must be an array');
            self._brokerCache.del(workerId);
            return null;
        }

        var addr = res[1][3].split(':');

        brInfo = {
            brokerId: res[1][0],
            clustername: res[1][1],
            status: res[1][2],
            address: {
                host: addr[0],
                port: parseInt(addr[1])
            }
        };

        // Update broker cache
        if (res[1][2] === 'active') {
            self._brokerCache.set(workerId, brInfo);
        }

        return brInfo;
    });
};

/**
 * Recover a worker.
 * This function is called internally when a worker needs a recovery. This function is identical
 * to createWorker() except that it always return a Promise, and it sets option.cause to
 * Worker.CreateCause.RECOVERY.
 * @private
 * @param {string} workerName Name of the worker to be recovered. (A class name, not worker ID)
 * @param {object} [option] Options.
 * @param {string} [option.id] Worker ID to be assigned. If not specified and the worker
 * is dynamic (option.static is set to false), then dworker will assign a unique ID for
 * the new worker.
 * @param {string} [option.static] If "static", then this method will create
 * a static worker (only one instance in the cluster).
 * @param {object} [option.attributes] A bag of parameters passed to the new worker
 * created on another process (broker). Defaults to false.
 * which can be referenced by Worker#attributes.<param-name>.
 * @param {boolean} [option.attributes.recoverable] Mark the new worker as
 * recoverable. When a process (broker) dies, workers marked as recoverable
 * may be reconstructed if condition allows.
 * @param {string} [option.cn] Name of cluster the worker belonged to.
 * @returns {Promise} Always returns a promise.
 * See {@link Broker~createWorkerCallback}.
 */
Broker.prototype._recoverWorker = function (workerName, option) {
    // Make sure attributes object is always present.
    if (!option.attributes) {
        option.attributes = {};
    }
    option.cause = Worker.CreateCause.RECOVERY;

    return this.createWorker(workerName, option);
};

/** @private */
Broker.prototype._onSubMessage = function (chId, data) {
    void(chId);

    debug('_onSubMessage: data=' + data + ' type=' + typeof(data));

    if (data.length === 0) {
        debug('ping received by broker ' + this.id);
        return;
    }

    try {
        var msg = JSON.parse(data);
    } catch (e) {
        this.logger.error('Malformed pubsub signal: ' + data);
        return;
    }

    if (msg.sig === SIG_RECOVER) {
        this._onSigRecover();
        return;
    }
    else if (msg.sig === SIG_SALVAGE) {
        this._onSigSalvage(msg.clustername, msg.brokerId);
        return;
    }
    else if (msg.sig === SIG_RESTART) {
        this._onSigRestart();
        return;
    }

    debug('Error: unknown pubsub signal: ' + data);
};

Broker.prototype._onRequest = function (data, requesterId) {
    debug('_onRequest:', data);

    if (!data.wid) {
        // Message to a broker
        this._onBrokerRequest(data, requesterId);
    } else {
        // Message to a worker
        this._onWorkerRequest(data, requesterId);
    }
};

Broker.prototype._onResponse = function (data) {
    debug('_onResponse:', data);

    if (!data.wid) {
        // Message to a broker
        this._onBrokerResponse(data);
    } else {
        // Message to a worker
        this._onWorkerResponse(data);
    }
};

Broker.prototype._onRemoteDisconnect = function (remote) {
    debug('[br:%s] _onRemoteDisconnect:', this.id, remote);
    var self = this;
    var workerIds = [];
    this._brokerCache.forEach(function (value, key) {
        /* istanbul ignore else */
        if (remote.host === value.address.host && remote.port === value.address.port) {
            workerIds.push(key);
        }
    });
    workerIds.forEach(function (workerId) {
        debug('invalidating cached address for worker ' + workerId);
        self._brokerCache.del(workerId);
    });
};

Broker.prototype._onSigRecover = function () {
    debug('_onSigRecover() called: isRecovering=' + this._isRecovering + ' needsRecovery=' + this._needsRecovery);
    if (this._isRecovering) {
        this._needsRecovery = true;
        return;
    }

    // Fetch one worker at a time then recreate it using _recoverWorker(), until
    // rz table gets emptied.
    var self = this;

    this._isRecovering = true;


    function recoveryLoop() {
        self._needsRecovery = false;
        // Fetch up to N worker to recover at a time.
        // N = 1 for now..
        return self._pub.evalshaAsync(
            scripts.fetchWorkersToRecover.sha,
            4,
            self._keys.gh,
            self._keys.wh,
            self._keys.bh,
            self._keys.rz,
            self.redisTime,
            self._option.ttl,
            1 // fetch 1 worker
        )
        .then(function (res) {
            if (!res || !Array.isArray(res)) {
                self._isRecovering = false;
                return;
            }
            if (!Array.isArray(res[0])) {
                self._isRecovering = false;
                return;
            }
            if (!res[0].length) {
                self._isRecovering = false;
                return;
            }

            var promises = res[0].map(function (item) {
                var info = JSON.parse(item);
                var option = {
                    id: info.id,
                    name: info.name,
                    cn: info.cn,
                    static: info.attributes.static,
                    attributes: info.attributes
                };
                return self._recoverWorker(info.name, option)
                .catch(function (err) {
                    self.logger.error('Worker recovery failed: workerId=' + info.id + ' err=' + JSON.stringify(err));
                    // Swallow the error, the continue the recovery loop.
                });
            });

            return Promise.all(promises)
            .then(function () {
                if (res[1] > 0 || self._needsRecovery) {
                    return recoveryLoop();
                }
            });
        })
        .finally(function () {
            self._isRecovering = false;
        });
    }

    recoveryLoop().done();
};

Broker.prototype._onSigSalvage = function (clustername, targetBrokerId) {
    debug(this.id + ':_onSigSalvage() called: cn=' + clustername + ' target=' + targetBrokerId);
    var self = this;
    return self._pub.evalshaAsync(
        scripts.salvageWorkers.sha,
        7,
        self._keys.gh,
        self._keys.wh,
        self._keys.bh,
        self._keys.cz + ':' + clustername,
        self._keys.bz + ':' + clustername,
        self._keys.wz + ':' + targetBrokerId,
        self._keys.rz,
        targetBrokerId,
        0
    )
    .then(function () {
        // Now, kick the recovery loop
        debug(self.id + ':_onSigSalvage() complete');
        self._onSigRecover();
    });
};

Broker.prototype._onSigRestart = function () {
    debug(this.id + ':_onSigRestart() called');
    this.restart().done();
};

Broker.prototype._onBrokerRequest = function (data, requesterId) {
    debug('RPC broker request');
    // RPC request
    if (!this[data.m]) {
        debug('Error: Broker does not have such method: ' + data.m);
        var pl = {
            err: { name: "Error", message: "Broker does not have such method: " + data.m }
        };
        this._replyToBroker(data.seq, pl, requesterId);
        return;
    }

    var self = this;
    this[data.m](data.pl)
    .then(function (res) {
        return { res: res };
    }, function (err) {
        /* istanbul ignore else */
        if (err instanceof Error) {
            err = { name: err.name, message: err.message };
        } else {
            err = { name: 'Error', message: 'unknown' };
        }
        return { err: err };
    }).then(function (pl) {
        self._replyToBroker(data.seq, pl, requesterId);
    });
};

Broker.prototype._onBrokerResponse = function (data) {
    debug('RPC broker response:', data);
    // RPC response
    if (typeof data.seq !== 'number') {
        debug('Internal error: the data must have a sequence number');
        return false;
    }
    if (!data.pl || typeof data.pl !== 'object') {
        debug('Internal error: data must have a payload of type object');
        return false;
    }

    var cb = this._cbs[data.seq];
    delete this._cbs[data.seq];
    if (cb) {
        if (data.pl.err) {
            data.pl.err = new Error(data.pl.err.message);
        }
        cb(data.pl.err, data.pl.res); // eslint-disable-line callback-return
    }
    return true;
};

Broker.prototype._onWorkerRequest = function (data, requesterId) {
    debug('RPC worker request');
    var self = this;
    var pl;

    if (!data.m) {
        debug('Error: Worker request must have a method property, m');
        return false;
    }

    // Find worker instance
    var worker = this._workers[data.wid];
    if (!worker) {
        debug('Error: Worker instance not found');
        pl = {
            err: { name: "Error", message: "Error: Worker instance not found" }
        };
        this._replyToWorker(data.seq, pl, data.wid, requesterId);
        return false;
    }
    if (worker.state !== Worker.State.ACTIVE) {
        debug('Error: Worker is not active');
        pl = {
            err: { name: "Error", message: "Error: Worker is not active" }
        };
        this._replyToWorker(data.seq, pl, data.wid, requesterId);
        return false;
    }

    if (typeof data.seq === 'number') {
        promisifyCb(worker.onAsk, worker)(data.m, data.pl)
        .then(function (res) {
            return { res: res };
        }, function (err) {
            /* istanbul ignore else */
            if (err instanceof Error) {
                err = { name: err.name, message: err.message };
            } else {
                err = { name: 'Error', message: 'unknown' };
            }
            return { err: err };
        })
        .then(function (pl) {
            self._replyToWorker(data.seq, pl, data.wid, requesterId);
        });
        return true;
    }

    worker.onTell(data.m, data.pl);
    return true;
};

Broker.prototype._onWorkerResponse = function (data) {
    debug('RPC worker response');
    // RPC response
    if (typeof data.seq !== 'number') {
        debug('Internal error: the data must have a sequence number');
        return false;
    }
    if (!data.pl || typeof data.pl !== 'object') {
        debug('Internal error: data must have a payload of type object');
        return false;
    }
    var cb = this._cbs[data.seq];
    delete this._cbs[data.seq];
    if (cb) {
        if (data.pl.err) {
            data.pl.err = new Error(data.pl.err.message);
        }
        cb(data.pl.err, data.pl.res); // eslint-disable-line callback-return
    }
    return true;
};

Broker.prototype._requestToBroker = function (method, data, dstAddress) {
    var obj = {
        m: method,
        seq: this._seqCounter.get(),
        pl: data
    };
    return this._request(obj, dstAddress);
};

Broker.prototype._replyToBroker = function (seq, data, requesterId) {
    var obj = {
        seq: seq,
        pl: data
    };
    this._router.respond(requesterId, obj);
};

Broker.prototype._askWorker = function (method, data, workerId) {
    var self = this;

    return backoffRetry(this._option.retries, function (nRetries) {
        debug('_askWorker(): retries=' + nRetries);
        return self._findBroker(workerId)
        .then(function (brInfo) {
            if (!brInfo) {
                throw new Error('Broker not found or unreachable (workerId =' + workerId + ')');
            }
            var obj = {
                m: method,
                wid: workerId,
                seq: self._seqCounter.get(),
                pl: data
            };
            return self._request(obj, brInfo.address)
            .catch(function (err) {
                self._brokerCache.del(workerId);
                throw err;
            });
        });
    });
};

Broker.prototype._tellWorker = function (method, data, workerId) {
    var self = this;

    return backoffRetry(this._option.retries, function (nRetries) {
        debug('_tellWorker(): retries=' + nRetries);
        return self._findBroker(workerId)
        .then(function (brInfo) {
            if (!brInfo) {
                throw new Error('Broker not found or unreachable (workerId =' + workerId + ')');
            }
            var obj = {
                m: method,
                wid: workerId,
                pl: data
            };
            return self._request(obj, brInfo.address)
            .catch(function (err) {
                self._brokerCache.del(workerId);
                throw err;
            });
        });
    });
};

Broker.prototype._replyToWorker = function (seq, data, workerId, requesterId) {
    var obj = {
        wid: workerId,
        seq: seq,
        pl: data
    };
    this._router.respond(requesterId, obj);
};

Broker.prototype._request = function (data, dstAddress) {
    var seq = data.seq;
    var self = this;

    return this._router.request(dstAddress, data)
    .then(function () {
        if (!seq) {
            // This is `tell` operation. No confirmation from remote is necessary.
            return;
        }

        // Note: The remote broker may crash before returning responses for
        // whatever reasons, we need to set a timeout for this callback.
        return new Promise(function (resolve, reject) {
            var ts = Date.now();
            self._cbs[seq] = function (err, res) {
                if (err) {
                    return void(reject(err));
                }
                return void(resolve(res));
            };
            self._cbList.push([ts, seq]);
        });
    });
};

Broker.prototype._onCreateWorker = function (data) {
    debug('_onCreateWorker() called');
    var self = this;
    var brokerId;
    var workerId;

    return self._pub.evalshaAsync(
        scripts.findOrCreateWorker.sha,
        7,
        self._keys.gh,
        self._keys.wh,
        self._keys.bh,
        self._keys.cz + ':' + self.clustername,
        self._keys.bz + ':' + self.clustername,
        self._keys.wz + ':' + self.id,
        self._keys.rz,
        self.id,
        data.name,
        data.id,
        JSON.stringify(data.attributes),
        self.redisTime,
        0, // TODO: get this from config. 0 means forever.
        (data.cause === Worker.CreateCause.RECOVERY)? 1:0
    )
    .then(function (res) {
        if (!Array.isArray(res)) {
            self.logger.error('The result from findOrCreateWorker must be an array');
            throw new Error('Internal error');
        }

        if (res[0] !== 0) {
            throw new Error('Unexpected result code ' + res[0]);
        }

        if (!Array.isArray(res[1]) || res[1].length < 3) {
            self.logger.error('Unexpected result from findOrCreateWorker.lua:', res);
            throw new Error('Internal error (invalid result value from lua)');
        }

        brokerId = res[1][0];
        workerId = res[1][2];
        var worker = null;

        if (brokerId === self.id && !self._workers[workerId]) {
            var Ctor = self._workerReg.get(data.name);
            worker = new Ctor(workerId, self, data.attributes);
            self._workers[workerId] = worker;

            worker.__priv.state = Worker.State.ACTIVATING;
            self.logger.debug('Broker(' + self.id + '): calling onCreate on worker ' + worker.id);
            return promisifyCb(worker.onCreate, worker)({ cause: data.cause })
            .catch(function (err) {
                // The rejection from Worker#onCreate() is ignored by design.
                self.logger.warn('Worker#onCreate() threw an error but will be ignored:', err);
            })
            .then(function () {
                self.logger.debug('processing further for Broker(' + self.id + '): and worker ', worker.id);
                if (worker.load === 0) {
                    worker.load = Worker.DEFAULT_LOAD;
                }
                worker.__priv.state = Worker.State.ACTIVE;

                if (worker.__priv.mark === 'destroy') {
                    debug('_onCreateWorker: perform marked destroy');
                    self._destroyWorker(worker);
                }
                return {brokerId: brokerId, workerId: workerId};
            });
        }

        return {brokerId: brokerId, workerId: workerId};
    });
};

Broker.prototype._destroyWorker = function (worker, option) {
    var self = this;
    option = _.defaults(option || {}, {
        // Self-destroyed worker won't be recovered by default.
        noRecover: true
    });

    if (worker.state !== Worker.State.ACTIVE) {
        // Leave a reminder then resolve
        debug('_destroyWorker: mark destroy');
        worker.__priv.mark = 'destroy';
        return;
    }

    worker.__priv.mark = '';
    worker.__priv.state = Worker.State.DESTROYING;

    this._pub.evalshaAsync(
        scripts.destroyWorker.sha,
        4,
        self._keys.gh,
        self._keys.wh,
        self._keys.wz + ':' + self.id,
        self._keys.rz,
        worker.id,
        option.noRecover? 0:1
    )
    .catch(function (err) {
        void(err); // we need to move on regardless...
    })
    .then(function () {
        return promisifyCb(worker.onDestroy, worker)({
            cause: Worker.DestroyCause.SELF
        });
    })
    .then(function () {
        worker.load = 0; // just in case
        delete self._workers[worker.id];
        worker.__priv.state = Worker.State.DESTROYED;
        worker.__priv.br = null;
        debug('total load= ' + self.load);
    }).done();
};

Broker.prototype._updateLoad = function (worker, delta) {
    void(worker); // currently now used
    if (!delta) {
        return;
    }

    var self = this;
    this._load += delta;
    this._loadUpdated = true;

    // FIXME: This adds the element if the broker does not exist. Ideally,
    // it should check the existence first. (If redis version >= 3.0.2,
    // we would use XX option.)
    // Consider converting this section to lua.
    this._pub.zadd(
        this._keys.cz+':'+this.clustername,
        this._load,
        this.id,
        function (err) {
            if (err) {
                self.logger.error('Failed to update load value on redis');
            }
        }
    );
};

Broker.prototype._syncRedisTime = function (now) {
    var self = this;
    this._pub.time(function (err, result) {
        if (err) {
            self.logger.error('Failed to get time on redis');
            // Ignore this for now because what affected is growing
            // discrepancy in between redis time and local time which
            // should be very slow.
            return;
        }
        // convert from [ seconds, micro-seconds ] to milliseconds
        var redisTime = result[0] * 1000 + Math.floor(result[1] / 1000);
        self._timeOffset = redisTime - now;
        self._lastTimeSync = now;
        debug('Redis time offset: ' + self._timeOffset + ' [msec]');
    });
};

Broker.prototype._setState = function (newState) {
    var oldState = this._state;
    var ok = true;

    switch (newState) {
        case State.INACTIVE:
            break;
        case State.ACTIVATING:
            if (oldState !== State.INACTIVE && oldState !== State.DESTROYED) {
                debug('State transition error: %d -> %d', oldState, newState);
                ok = false;
            }
            break;
        case State.ACTIVE:
            if (oldState !== State.ACTIVATING) {
                debug('State transition error: %d -> %d', oldState, newState);
                ok = false;
            }
            break;
        case State.DESTROYING:
            if (oldState !== State.ACTIVE) {
                debug('State transition error: %d -> %d', oldState, newState);
                ok = false;
            }
            break;
        case State.DESTROYED:
            if (oldState !== State.DESTROYING) {
                debug('State transition error: %d -> %d', oldState, newState);
                ok = false;
            }
            break;
        default:
            debug('State transition error: %d -> %d (invalid)', oldState, newState);
            ok = false;
    }

    this._state = newState;
    this.emit('state', oldState, newState);
    return ok;
};

Broker.prototype._onTimer = function () {
    //debug('#_onTimer()');

    // Update load
    var self = this;
    var now = Date.now();

    // Sync redis time
    if (now - this._lastTimeSync >= TIME_SYNC_INTERVAL) {
        this._syncRedisTime(now);
    }

    // Handle expired callbacks (RPC timeout)
    while (this._cbList.length > 0) {
        var item = this._cbList[0];
        if (item[0] + this.rpcTimeout > now) {
            break;
        }
        this._cbList.shift();
        var cb = this._cbs[item[1]];
        if (cb) {
            delete this._cbs[item[1]];
            debug('cb=', cb);
            /*eslint-disable callback-return */
            cb(new Error("RPC timeout. (elapsed=" + (now - item[0]) + ' [msec])'));
            /*eslint-enable callback-return */
        }
    }

    // Recalculate the total load.
    if (this._loadUpdated) {
        this._loadUpdated = false;
        this._load = 0;
        Object.keys(this._workers).forEach(function (id) {
            self._load += self._workers[id].load;
        });
    }

    if (this._option.healthCheckInterval > 0) {
        this._healthCheckIn--;
        if (this._healthCheckIn <= 0) {
            this._healthCheckIn = this._option.healthCheckInterval;
            this._pub.evalshaAsync(
                scripts.healthCheck.sha,
                5,
                self._keys.gh,
                self._keys.wh,
                self._keys.bh,
                self._keys.cz + ':' + self.clustername,
                self._keys.bz + ':' + self.clustername,
                self.id
            )
            .then(function (res) {
                if (res[0] == 0) {
                    debug('Broker (%s): healthcheck %d', self.id, res[0]);
                    return;
                }
                if (res[0] == 1) {
                    self.logger.debug('Broker (%s): healthcheck (%d: salvage issued)', self.id, res[0]);
                    return;
                }
                self.logger.warn('Broker (%s): healthcheck warning: %s', self.id, res[1]);
            }).done();
        }
    }

    this._timer = setTimeout(this._onTimer.bind(this), TIMER_INTERVAL);
};

Broker.State = State;

module.exports.Broker = Broker;
