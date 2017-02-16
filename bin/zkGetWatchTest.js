#!/usr/bin/env node
/**
 * @overview The Manatee client.
 * @copyright Copyright (c) 2014, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 *                   _.---.._
 *      _        _.-' \  \    ''-.
 *    .'  '-,_.-'   /  /  /       '''.
 *   (       _                     o  :
 *    '._ .-'  '-._         \  \-  ---]
 *                  '-.___.-')  )..-'
 *                           (_/
 */
var bunyan = require('bunyan');
var once = require('once');
var zkClient = require('joyent-zookeeper-client');

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'info'),
    name: 'zkConnTest',
    serializers: {
        err: bunyan.stdSerializers.err
    }
});

var path = '/foo';

var connStr = process.argv[2];
if (!connStr) {
    console.error('usage: ' + process.argv.join(' ') + ' <connection string>');
    process.exit(1);
}

console.log('Connecting to: ' + connStr);

var opts = {
    'sessionTimeout': 5000,
    'spinDelay': 1000,
    'retries': 2
};

var zk = zkClient.createClient(connStr, opts);

this.opts = opts;
this.zk = zk;

var setupDataOnce = once(function setup() {
    function onWatch(event) {
        console.log(event, 'watch fired');
    }
    watch(zk, path, onWatch, function () {
        console.log('zk: watch set');
        function getData() {
            zk.getData(path, function (err, d, s) {
                if (err) {
                    console.log(err.name, 'zk: error fetching data');
                } else {
                    console.log(d.toString('utf8'), s.version, 'zk: got data');
                }
                setTimeout(getData, 5000);
            });
        }
        getData();
    });
});

//Creator says this is "Java Style"
zk.on('state', function (s) {
    //Just log it.  The other events are called.
    console.log(s, 'zk: new state (' + zk.getState().getName() + ')');
});

//Client is connected and ready. This fires whenever the client is
// disconnected and reconnected (more than just the first time).
zk.on('connected', function () {
    console.log(zk.getSessionId(), 'zk: connected');
    setupDataOnce();
});

//Client is connected to a readonly server.
zk.on('connectedReadOnly', function () {
    console.log('zk: connected read only');
});

//The connection between client and server is dropped.
zk.on('disconnected', function () {
    console.log('zk: disconnected');
});

//The client session is expired.
zk.on('expired', function () {
    console.log('zk: session expired, reiniting.');
});

//Failed to authenticate with the server.
zk.on('authenticationFailed', function () {
    console.log('zk: auth failed');
});

//Not even sure if this is really an error that would be emitted...
zk.on('error', function (err) {
    console.log({err: err}, 'zk: unexpected error, reiniting');
});

zk.connect();

function watch(z, p, onWatch, cb) {
    cb = once(cb);
    function dataWatchFired(event) {
        onWatch(event);
        return (registerDataWatch());
    }
    function registerDataWatch() {
        z.exists(p, dataWatchFired, cb);
    }
    registerDataWatch();
}
