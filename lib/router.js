'use strict';

var util = require('util');
var events = require('events');
var net = require('net');
var _ = require('lodash');
var debug = require('debug')('dw:router');
var Promise = require('bluebird');
//var safeCyclicIncrement = require('./common').safeCyclicIncrement;
var SafeCyclicCounter = require('./common').SafeCyclicCounter;

var defaults = {
    socTimeout: 30000
};

/**
 * An enumeration of {@link Broker} states
 * @private
 * @memberof Router
 * @enum {number}
 * @constant
 */
var ConnectionState = {
    CLOSED: 0,
    OPENING: 1,
    OPEN: 2,
    CLOSING: 3
};

////////////////////////////////////////////////////////////////////////////////
// Public section

/**
 * Router class. Route messages between brokers over direct TCP connections.
 * @constructor
 * @param {string} id A unique broker ID.
 * @param {object} [option] Options.
 * @param {object} [option.socTimeout] Socket timeout in milliseconds.
 */
function Router(option) {
    this._option = _.defaults(option || {}, defaults);
    this._server = null;
    this._serverSockets = {};
    this._clientSockets = {};
    this._requesterIdCounter = new SafeCyclicCounter();
    this._nClientSocketsOpened = 0;
    this._nClientSocketsClosed = 0;
    this._nClientSocketsFailed = 0;
    this._nServerSocketsOpened = 0;
    this._nServerSocketsClosed = 0;
    this._nServerSocketsFailed = 0;
    this._peakBufferSize = -1;

    // Logger
    var self = this;
    this.logger = {};
    ['debug', 'warn', 'info', 'error'].forEach(function (level) {
        self.logger[level] = function () {
            self.emit('log', { level: level, message: util.format.apply(self, arguments) });
        };
    });
}

util.inherits(Router, events.EventEmitter);

/**
 * Start listening for incoming connections.
 * @param {string} [host] Local IP address. If not given, the router will listen
 * on '0.0.0.0'.
 * @returns {Promise} Returns a promise. Resolved value will be the local port
 * number assigned by underlying OS. Therefore, EADDRINUSE will not occur.
 */
Router.prototype.listen = function (host) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self._server = net.createServer(self._onNewConnection.bind(self));
        self._server.listen(0, host, function () {
            resolve(self._server.address());
        });
        self._server.on('error', function (err) {
            self._server = null;
            reject(err);
        });
    });
};

/**
 * Send a request to remote broker.
 * If no connection is available with the specified remote broker, it will
 * first establish a TCP connection, then send the data. If the connection
 * exists and the specified remote address/port match, it will reuse the
 * connection to send the data.
 * @param {object} address Destination address of the broker.
 * @param {string} address.host Remote IP address.
 * @param {number} address.port Remote port number.
 * @returns {Promise} Returns a Promise. The promise will be resolved
 * when the operation successfully writes the data into the underlying
 * socket.
 */
Router.prototype.request = function (address, data) {
    if (!address || (typeof address !== 'object')) {
        return Promise.reject(new Error('address must be a valid object'));
    }
    if (typeof address.host !== 'string') {
        return Promise.reject(new Error('address.host must be a string'));
    }
    if (typeof address.port !== 'number') {
        return Promise.reject(new Error('address.port must be a number'));
    }
    var self = this;
    var remoteId = address.host + ':' + address.port;
    return new Promise(function (resolve, reject) {
        var info = self._clientSockets[remoteId];
        if (info) {
            if (info.state === ConnectionState.CLOSING ||
                info.state === ConnectionState.CLOSED) {
                info.soc.destroy();
                delete self._clientSockets[remoteId];
                info = null;
            }
        }

        if (info) {
            if (info.state === ConnectionState.OPENING) {
                // Put the data in the pending queue
                info.pending.push({
                    resolve: resolve,
                    reject: reject,
                    data: data
                });
            } else {
                self.logger.debug('dw:router: send request to remote=%s', remoteId);
                self._send(info.soc, data);
                resolve();
            }

            return;
        }

        var since = Date.now();

        // Create a new connection.
        // Client socket lifetime is managed in this closure.
        info = {
            state: ConnectionState.OPENING,
            pending: [{
                resolve: resolve,
                reject: reject,
                data: data
            }]
        };
        info.soc = new net.createConnection(address.port, address.host);
        info.soc.setTimeout(self._option.socTimeout);
        info.soc.on('connect', function () {
            info.state = ConnectionState.OPEN;
            info.frax = require('frax').create();
            info.frax.on('data', function (frame) {
                var msg;
                try {
                    msg = JSON.parse(frame.toString());
                } catch (e) {
                    debug('JSON parse error:', e);
                    info.lastError = e;
                    info.state = ConnectionState.CLOSING;
                    info.soc.end();
                    return;
                }
                self.logger.debug('dw:router: received response from %s', remoteId);
                self.emit('response', msg);
            });
            // Send all pending data.
            info.pending.forEach(function (item) {
                self.logger.debug('dw:router: send request (in pending) to remote=%s', remoteId);
                self._send(info.soc, item.data);
                item.resolve();
            });
            info.pending = [];
            self._nClientSocketsOpened++;
            self.logger.info('dw:router: client socket connect: remote=%s elapsed=%d', remoteId, Date.now() - since);
        });
        info.soc.on('data', function (buf) {
            info.frax.input(buf);
        });
        info.soc.on('end', function () {
            self.logger.info('dw:router: client socket ended: remote=%s', remoteId);
            self.emit('disconnect', address);
            info.state = ConnectionState.CLOSING;
            info.soc.end();
        });
        info.soc.on('timeout', function () {
            self.logger.info('dw:router: client socket timeout: remote=%s', remoteId);
            info.state = ConnectionState.CLOSING;
            info.soc.end();
        });
        info.soc.on('error', function (err) {
            self.logger.info('dw:router: client socket error: remote=%s err=%s', remoteId, err.message);
            self.emit('disconnect', address);
            info.state = ConnectionState.CLOSING;
            info.lastError = err;
            self._nClientSocketsFailed++;
        });
        info.soc.on('close', function (had_error) {
            info.state = ConnectionState.CLOSED; // just in case
            // Reject promise if any in pending queue
            info.pending.forEach(function (item) {
                /* istanbul ignore else */
                if (had_error) {
                    item.reject(info.lastError);
                } else {
                    self.logger.warn(
                        'dw:router: unexpected pending item on close with no error: %s',
                        JSON.stringify(item)
                    );
                }
            });
            info.pending = [];
            delete self._clientSockets[remoteId];
            self._nClientSocketsClosed++;
            self.logger.info(
                'dw:router: client socket: close event: remote=%s' +
                ' had_error=%s opened=%d closed=%d failed=%d elapsed=%d',
                remoteId,
                had_error,
                self._nClientSocketsOpened,
                self._nClientSocketsClosed,
                self._nClientSocketsFailed,
                Date.now() - since
            );
        });
        self._clientSockets[remoteId] = info;
    });
};

Router.prototype.respond = function (requesterId, data) {
    var soc = this._serverSockets[requesterId];
    if (soc) {
        this.logger.debug('dw:router: respond on requesterId=%d', requesterId);
        this._send(soc, data);
    } else {
        this.logger.warn('dw:router: socket not found for requesterId=%d', requesterId);
    }
};

Router.prototype.close = function () {
    var self = this;

    // Close all client sockets.
    Object.keys(this._clientSockets).forEach(function (brokerId) {
        var info = self._clientSockets[brokerId];
        /* istanbul ignore else */
        if (info.soc && info.state !== ConnectionState.CLOSED) {
            info.soc.destroy();
        }
    });
    this._clientSockets = {};

    // Close all server sockets.
    Object.keys(this._serverSockets).forEach(function (requesterId) {
        self._serverSockets[requesterId].destroy();
    });
    this._serverSockets = {};

    // Close listening socket
    if (this._server) {
        this._server.close();
        this._server = null;
    }
};

////////////////////////////////////////////////////////////////////////////////
// Private section

Router.prototype._send = function (soc, data) {
    var pl = JSON.stringify(data);
    var plBytes = Buffer.byteLength(pl);
    var buf = new Buffer(2 + plBytes);
    buf.writeUInt16BE(plBytes, 0);
    buf.write(pl, 2);
    soc.write(buf);

    if (this._peakBufferSize < soc.bufferSize) {
        this._peakBufferSize = soc.bufferSize;
        this.logger.info('dw:router: updated peak buffer size: %d', this._peakBufferSize);
    }
};

Router.prototype._onNewConnection = function (soc) {
    var self = this;
    var since = Date.now();
    var requesterId = this._requesterIdCounter.get();
    var lastError;
    var frax = require('frax').create();
    frax.on('data', function (frame) {
        var msg;
        try {
            msg = JSON.parse(frame.toString());
        } catch (e) {
            debug('JSON parse error:', e);
            lastError = e;
            soc.end();
            return;
        }
        self.logger.debug('dw:router: received request on requesterId=%d', requesterId);
        self.emit('request', msg, requesterId);
    });

    self._nServerSocketsOpened++;
    self.logger.info('dw:router: server socket connect: requesterId=%d', requesterId);

    // Server socket has timeout twice as long as that of client socket
    // to avoid simultaneous close, or race condition.
    soc.setTimeout(this._option.socTimeout * 2);

    soc.on('data', function (buf) {
        frax.input(buf);
    });
    soc.on('end', function () {
        self.logger.info('dw:router: server socket ended: requesterId=%d', requesterId);
        soc.end();
    });
    soc.on('timeout', function () {
        self.logger.info('dw:router: server socket timeout: requesterId=%d', requesterId);
        soc.end();
    });
    soc.on('error', /* istanbul ignore next */ function (err) {
        self.logger.info('dw:router: server socket error: requesterId=%d err=%s', requesterId, err.message);
        lastError = err;
        self._nServerSocketsFailed++;
    });
    soc.on('close', function (had_error) {
        delete self._serverSockets[requesterId];
        self._nServerSocketsClosed++;
        self.logger.info(
            'dw:router: server socket: close event: requesterId=%d had_error=%s' +
            ' opened=%d closed=%d failed=%d elapsed=%d',
            requesterId,
            had_error,
            self._nServerSocketsOpened,
            self._nServerSocketsClosed,
            self._nServerSocketsFailed,
            Date.now() - since
        );
    });
    this._serverSockets[requesterId] = soc;
};

module.exports.Router = Router;

