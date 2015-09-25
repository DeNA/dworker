'use strict';

var Promise = require('bluebird');


/**
 * Agent class constructor.
 * Do not instantiate directly from application.
 * Agent instances are created by {@link Broker}.
 * You may define a customized Agent by subclassing. When
 * worker class is registered, set Worker#agent property to the custom agent
 * so that broker will use the custom Agent class when returning an instance
 * of the Agent as a result of Broker#createWorker() or Broker#findWorker().
 * @constructor
 * @property {string} id Corresponding worker ID (read-only)
 */
function Agent(id, broker) {
    if (typeof id !== 'string' || id.length === 0) {
        throw new Error('Invalid agent ID');
    }
    if (!broker) {
        throw new Error('Invalid broker');
    }
    this.__priv = {
        id: id,
        br: broker
    };

    // Define getter 'id'
    this.__defineGetter__("id", function () {
        return this.__priv.id;
    });
}

/**
 * Callback function for Agent#ask.
 * Unlike {@link #tell}, this operation makes sures that the data is received
 * by the remote worker. The transaction remains until the remote worker
 * responds with a data, much like HTTP transaction.
 * @callback Agent~askCallback
 * @param {Error} err Error object.
 * @param {object} data Response data.
 */
/**
 * Ask operation for corresponding Worker.
 * @param {string} method Method name.
 * @param {object} data Data to send to the remote worker.
 * @param {Agent~askCallback} [cb] Callback from this method.
 * @returns {Promise} Returns a Promise if `cb` is not supplied.
 * See {@link Agent~askCallback}
 */
Agent.prototype.ask = function (method, data, cb) {
    if (!this.__priv.br) {
        return Promise.reject(new Error('Unlinked'));
    }
    return this.__priv.br._askWorker(
        method,
        data,
        this.__priv.id
    )
    .nodeify(cb);
};

/**
 * Callback function for Agent#tell.
 * @callback Agent~tellCallback
 * @param {Error} err Error object.
 */
/**
 * Tell operation for corresponding Worker.
 * Unlike {@link #ask}, this operation does not guarantee that the data is received
 * by the remote worker. This operation only guarantees that the data was
 * written into the destination is message queue.
 * @param {string} method Method name.
 * @param {object} data Data to send to the remote worker.
 * @param {Agent~tellCallback} [cb] Callback from this method.
 * @returns {Promise} Returns a Promise if `cb` is not supplied.
 * See {@link Agent~tellCallback}
 */
Agent.prototype.tell = function (method, data, cb) {
    if (!this.__priv.br) {
        return Promise.reject(new Error('Unlinked'));
    }
    return this.__priv.br._tellWorker(
        method,
        data,
        this.__priv.id
    )
    .nodeify(cb);
};

exports.Agent = Agent;
