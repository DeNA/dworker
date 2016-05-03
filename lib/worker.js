'use strict';

var Promise = require('bluebird');

/**
 * An enumeration of {@link Worker} states
 * @memberof Worker
 * @enum {number}
 * @constant
 */
var State = {
    /** Worker is inactive */
    INACTIVE: 0,
    /** Worker is being activated */
    ACTIVATING: 1,
    /** Worker is active */
    ACTIVE: 2,
    /** Worker is being destroyed */
    DESTROYING: 3,
    /** Worker has been destroyed */
    DESTROYED: 4
};

/**
 * Enumeration of {@link Worker} creation causes
 * @memberof Worker
 * @enum {number}
 * @constant
 */
var CreateCause = {
    /** Worker was created for the first time. */
    NEW: 0,
    /** Worker was created as a result of recovery process. */
    RECOVERY: 1
};

/**
 * Enumeration of {@link Worker} destroy causes
 * @memberof Worker
 * @enum {number}
 * @constant
 */
var DestroyCause = {
    /** Worker is destroyed by itself. */
    SELF: 0,
    /** Worker is destroyed by the system. (server shutdown, etc) */
    SYSTEM: 1
};

/**
 * Default {@link Worker} load
 * @memberof Worker
 * @enum {number}
 * @constant
 */
var DEFAULT_LOAD = 1;

/**
 * Worker class constructor.
 * Application must subclass this to implement abstract members.
 * Do not instantiate directly from application. Worker instances are created by {@link Broker}.
 * The subclass must pass all the arguments to this base Worker class. (See example)
 * @example
 * var util = require('util');
 * var Worker = require('dworker').Worker;
 *
 * function MyWorker() {
 *     Worker.apply(this, arguments);
 *     // Your code here
 * }
 *
 * util.inherits(MyWorker, Worker);
 *
 * // Override abstract method
 * MyWorker.prototype.onCreate = function (info) {
 *     // Your implementation
 * };
 * // Override abstract method
 * MyWorker.prototype.onDestroy = function (info) {
 *     // Your implementation
 * };
 * // Override abstract method
 * MyWorker.prototype.onAsk = function (method, data) {
 *     // Your implementation
 * };
 * // Override abstract method
 * MyWorker.prototype.onTell = function (method, data) {
 *     // Your implementation
 * };
 *
 * @constructor
 * @interface
 * @property {string} id Worker ID (read-only)
 * @property {number} state Current worker state (read-only)
 * @property {object} attributes Attributes given by creator of this worker. (read-only)
 * @property {number} load Current load imposed by this worker. (read/write)
 */
function Worker(id, broker, attributes) {
    this.__priv = {
        id: id,
        br: broker,
        attributes: attributes || {},
        state: State.INACTIVE, // modified by broker
        curLoad: 0
    };

    // Define getter 'id'
    this.__defineGetter__("id", function () {
        return this.__priv.id;
    });
    // Define getter 'state'
    this.__defineGetter__("state", function () {
        return this.__priv.state;
    });
    // Define getter 'attributes'
    this.__defineGetter__("attributes", function () {
        return this.__priv.attributes;
    });
    // Define getter 'load'
    this.__defineGetter__("load", function () {
        return this.__priv.curLoad;
    });
    // Define setter 'load'
    this.__defineSetter__("load", function (newLoad) {
        if (this.state !== State.DESTROYED && newLoad >= 0) {
            newLoad = Math.floor(newLoad);
            var prevLoad = this.__priv.curLoad;
            this.__priv.curLoad = newLoad;
            this.__priv.br._updateLoad(this, newLoad - prevLoad);
        }
    });
}

/**
 * Destroy this worker.
 * Call of this method will trigger {@link onDestroy} callback with
 * info.cause being {@link DestroyCause#SELF}.
 * @param {object} [option] Options.
 * @param {boolean} [option.noRecover] No recover option. This is set to true by default.
 * When this option is set to false, and this worker is created with recoverable option
 * set to true, this worker will be recreated right away somewhere in the same cluster.
 * This option is provide only for development purpose. (e.g. to test restoration of
 * context data on DB, etc.) Do not set this option to false in production for
 * obvious reasons.
 * @returns {void}
 */
Worker.prototype.destroy = function (option) {
    this.load = 0;
    this.__priv.br._destroyWorker(this, option);
};

/**
 * Callback function for Worker#onAsk.
 * @callback Worker~onAskCallback
 * @param {Error} err Error object.
 * @param {object} data Request data.
 */
/**
 * Called when ask operation is received.
 * Application must override this method to handle ask operation.
 * @abstract
 * @param {string} method Method name.
 * @param {object} data Data sent by requesting entity.
 * @param {Worker~onAskCallback} [cb] Callback from this method.
 * @returns {Promise} Returns a Promise if `cb` is not supplied. See {@link Worker~onAskCallback}
 */
Worker.prototype.onAsk = function (method, data, cb) {
    void(method, data);
    return Promise.reject(new Error("Not implemented")).nodeify(cb);
};

/**
 * Called when tell operation is received.
 * Application must override this method to handle tell operation.
 * @abstract
 * @param {string} method Method name.
 * @param {object} data Data sent by requesting entity.
 */
Worker.prototype.onTell = function (method, data) {
    void(method, data);
};

/**
 * Callback function for Worker#onCreate.
 * @callback Worker~onCreateCallback
 * @param {Error} err Error object.
 * @param {object} data Request data.
 */
/**
 * Called when this worker is created.
 * @abstract
 * @param {object} info Information of this callback.
 * @param {number} info.cause Cause of the creation.
 * @param {Worker~onCreateCallback} [cb] Callback from this method.
 * @see Worker.CreateCause
 * @returns {Promise} Returns a Promise if `cb` is not supplied. See {@link Worker~onCreateCallback}
 * Error return, or rejection of promise, will be ignored by
 * the caller.
 */
Worker.prototype.onCreate = function (info, cb) {
    void(info);
    this.load = DEFAULT_LOAD;
    return Promise.resolve().nodeify(cb);
};

/**
 * Callback function for Worker#onDestroy.
 * @callback Worker~onDestroyCallback
 * @param {Error} err Error object.
 * @param {object} data Request data.
 */
/**
 * Called when this worker is about to be destroyed.
 * @param {object} info Information of this callback.
 * @param {number} info.cause Cause of the destruction.
 * @param {Worker~onDestroyCallback} [cb] Callback from this method.
 * @see Worker.DestroyCause
 * @returns {Promise} Returns a Promise if `cb` is not supplied. See {@link Worker~onDestroyCallback}
 * Error return, or rejection of promise, will be ignored by
 * the caller.
 */
Worker.prototype.onDestroy = function (info, cb) {
    void(info);
    return Promise.resolve().nodeify(cb);
};

/**
 * Optional custom Agent class property. Set to undefined by default (uses base Agent class).
 * @memberof Worker
 * @type {Agent}
 * @name agent
 */

Worker.State = State;
Worker.CreateCause = CreateCause;
Worker.DestroyCause = DestroyCause;
Worker.DEFAULT_LOAD = DEFAULT_LOAD;

exports.Worker = Worker;
