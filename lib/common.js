'use strict';


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

exports.NOOP = function noop() {};

