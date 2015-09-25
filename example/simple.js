'use strict';

var dw = require('..'); // typically: require('dworker')
var Broker = dw.Broker;
var Worker = dw.Worker;
var Promise = require('bluebird');
var util = require('util');

// Create a subclass of Worker.
util.inherits(MyWorker, Worker);

// Define a simple Worker.
function MyWorker() {
    Worker.apply(this, arguments); // <-- Don't forget this!
}
// Implement all virtual methods.
MyWorker.prototype.onCreate = function (info) {
    console.log('MyWorker: onCreate() is called: info=' + JSON.stringify(info));
    return Promise.resolve();
};
MyWorker.prototype.onDestroy = function (info) {
    console.log('MyWorker: onDestroy() is called: info=' + JSON.stringify(info));
    return Promise.resolve();
};
MyWorker.prototype.onAsk = function (method, data) {
    console.log('MyWorker: onAsk() is called');
    if (method === 'echo') {
        // Simply returns the data back to the agent.
        return Promise.resolve(data);
    }
    return Promise.reject(new Error('Unknown ask method: ' + method));
};
MyWorker.prototype.onTell = function (method, data) {
    console.log('MyWorker: onTell() is called');
    if (method === 'log') {
        console.log('MyWorker: logging: ' + JSON.stringify(data));
        return;
    }
    console.log('Error: unknown tell method: ' + method);
};


var brokerId = 'my-broker-0001'; // This must be unique across entire worker system
var br = new Broker(brokerId);

// Register your worker
br.registerWorker(MyWorker);

br.start()
.then(function () {
    console.log('Test: broker started');
    return br.createWorker('MyWorker');
})
.then(function (agent) {
    console.log('Test: worker is created: ID=' + agent.id);
    return agent.ask('echo', { msg: 'Yahoo!' })
    .then(function (data) {
        console.log('Test: echo is received: ' + data.msg);
        return agent.tell('log', { debug: 'echo successful' })
        .then(function () {
            return Promise.delay(10); // give 10 msec for the worker to log.
        });
    });
})
.then(function () {
    // Now destroy the broker
    return br.destroy()
    .then(function () {
        console.log('Test: DONE!');
        // Kill the redis connections.
        br.quit();
    });
})
.done();
