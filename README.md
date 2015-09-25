# dworker

Distributed worker system backed by Redis.

## Installation (once published)
    $ npm install dworker

## Features
* A Node.js version of "Actor model", where the 'actor' is called Worker.
* Supports ask() and tell() methods for worker communication. (Built-in RPC)
* Score based load balancing across server cluster.
* Ability to defines more than one "clusters" to set up which cluster a worker should be running in.
* Automatic worker recovery feature. The system detects a death of processes, then salvage workers that were running on the dead process, then automatically reconstruct them on other processes in a collaborative manner.
* No polling. The dworker uses Redis pubsub to signal other brokers (a manger of workers) to wake them up.
* All distributed - no central entity (except redis-server).

## Requirements
* Requires Redis version 2.6.0 or later (dworker uses lua)

## API
Generate JSDoc API document by the following command:

```
$ npm run doc
```
You should the document at ./doc/index.html

### Example
Here's an simple example:

```js
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
    return Promise.reject(new Error('Unknown tell method: ' + method));
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
    })
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
```

## Limitation
* As redis-server being the centralized DB as well as message router, when redis goes down, whole system goes down.
* All worker communication traffic converges at Redis server (uses Lists). This could be a bottleneck in throughput.

## Consideration
* Worker instance sits in memory for a long time. Try to persist application data to DB and purge data from memory where possible. Needless to say, leave critical data in memory won't be recovered. (See Limitation)

### Note
* From v0.5.0, all the communication between brokers (or workers) use direct TCP connections. This greatly offloads CPU/netowrk bandwdith on Redis server. If v0.4.x or earlier is used, please beware the high load on Redis server.

## Technical Details
(TODO)

