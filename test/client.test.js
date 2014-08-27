/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var once = require('once');
var vasync = require('vasync');
var ManateeClient = require('../manatee.js');

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'warn'),
    name: 'manatee-integ-tests',
    serializers: {
        err: bunyan.stdSerializers.err
    },
    src: true
});

var n1Opts = null;
var n2Opts = null;
var n3Opts = null;

var MANATEES = {};
var SHARD_PATH = '/manatee/ffb914c7-aba4-40a0-ba8a-df2f7c1b1c32/election';

var MANATEE_CLIENT = null;


/*
 * Tests
 */

exports.clientTest = function (t) {
    MANATEE_CLIENT = ManateeClient.createClient({
        path: SHARD_PATH,
        zk: {
            servers: [ {host: '127.0.0.1', port: 2181} ],
            timeout: 30000
        }
    });
    var emitReady = false;
    var done = once(t.done);
    var id = setTimeout(function () {
        t.fail('client test exceeded tiemout');
        MANATEE_CLIENT.removeAllListeners();
        done();
    }, 40000);

    MANATEE_CLIENT.on('topology', function (dbs) {
        var barrier = vasync.barrier();
        barrier.on('drain', function () {
            t.ok(emitReady, 'manatee client did not emit ready event');
            clearTimeout(id);
            MANATEE_CLIENT.removeAllListeners();
            //done();
        });
        Object.keys(MANATEES).forEach(function (k) {
            var m = MANATEES[k];
            barrier.start(m.getPgUrl());
            if (dbs.indexOf(m.getPgUrl()) === -1) {
                t.fail('client did not get url ' + m.getPgUrl());
            }
            barrier.done(m.getPgUrl());
        });
    });

    MANATEE_CLIENT.once('ready', function () {
        emitReady = true;
    });
};
