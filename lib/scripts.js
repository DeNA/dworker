'use strict';

var fs = require('fs');

module.exports = {
    addBroker: {
        script: fs.readFileSync(__dirname + '/lua/addBroker.lua', 'utf8')
    },
    getLeastLoadedBroker: {
        script: fs.readFileSync(__dirname + '/lua/getLeastLoadedBroker.lua', 'utf8')
    },
    findOrCreateWorker: {
        script: fs.readFileSync(__dirname + '/lua/findOrCreateWorker.lua', 'utf8')
    },
    findBroker: {
        script: fs.readFileSync(__dirname + '/lua/findBroker.lua', 'utf8')
    },
    salvageWorkers: {
        script: fs.readFileSync(__dirname + '/lua/salvageWorkers.lua', 'utf8')
    },
    fetchWorkersToRecover: {
        script: fs.readFileSync(__dirname + '/lua/fetchWorkersToRecover.lua', 'utf8')
    },
    destroyWorker: {
        script: fs.readFileSync(__dirname + '/lua/destroyWorker.lua', 'utf8')
    },
    healthCheck: {
        script: fs.readFileSync(__dirname + '/lua/healthCheck.lua', 'utf8')
    }
};

