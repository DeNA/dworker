# dworker

Distributed worker system backed by Redis.

## Installation
    $ npm install dworker [--save]

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
var dw = require('dworker')
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

## LICENSE

The MIT License (MIT)

Copyright (c) 2015 ngmoco, LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

