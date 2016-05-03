'use strict';

var Promise = require('bluebird');
var redis = Promise.promisifyAll(require('redis'));

exports.whilst =
function whilst(test, fn) {
    var loop = function () {
        if (!test()) {
            return Promise.resolve();
        }
        return fn()
        .then(loop);
    };

    return loop();
};

exports.doWhilst =
function doWhilst(fn, test) {
    var loop = function () {
        return fn()
        .then(function () {
            if (!test()) {
                return;
            }
            return loop();
        });
    };

    return loop();
};

// Remove all keys used by the given broker.
// The broker must a valid instance.
exports.initRedis =
function initRedis() {
    var c = redis.createClient();
    var pattern = 'dw:*';
    return c.keysAsync(pattern)
    .each(function (key) {
        return c.delAsync(key);
    })
    .then(function () {
        c.quit();
    });
};
