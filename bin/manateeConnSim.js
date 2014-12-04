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
var manatee = require('../manatee');

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
    console.error('usage: ' + process.argv.join(' ') +
                  ' <zk connection string>');
    process.exit(1);
}

console.log('Connecting to: ' + connStr);

var opts = {
    'log': LOG,
    'path': '/manatee/1.moray.coal.joyent.us',
    'zk': {
        'connStr': connStr,
        'opts': {
            'sessionTimeout': 60000,
            'spinDelay': 1000,
            'retries': 60
        }
    }
};

var client = manatee.createClient(opts);

client.on('error', function (err) {
    console.log(err, 'manatee: client error');
    process.exit(1);
});

client.on('ready', function () {
    console.log('manatee: ready ', client.topology);
});

client.on('topology', function (urls) {
    console.log('manatee: topology', urls);
});
