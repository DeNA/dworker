'use strict';

var crypto = require('crypto');

exports.Const = {
    MAX_SAFE_INTEGER: Math.pow(2, 53) - 1
};

exports.SafeCyclicCounter =
function SafeCyclicCounter(initialVal) {
    this._counter = 0;
    if (typeof initialVal === 'number') {
        this._counter = initialVal;
    }

    this.get = function () {
        var num = this._counter;
        /* istanbul ignore else */
        if (this._counter < exports.Const.MAX_SAFE_INTEGER) {
            this._counter++;
        } else {
            this._counter = 0;
        }
        return num;
    };
};

var POW_2_32 = Math.pow(2, 32);

exports.generateHashKey =
function generateHashKey(s) {
    var md5 = crypto.createHash('md5');
    md5.update(s, 'utf8');
    var buf = md5.digest(); // buf is of type Buffer

    // Take the first 8 bytes (64 bits), then zero the first 11 bits.
    // The last 8 bytes are not used.
    buf[0] = 0;
    buf[1] &= 31; // 0x1f

    return buf.readUInt32BE(0) * POW_2_32 + buf.readUInt32BE(4);
};

exports.NOOP = function noop() {};

