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
var manatee = require('../manatee');
var zkClient = require('node-zookeeper-client');

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'warn'),
    name: 'manatee-integ-tests',
    serializers: {
        err: bunyan.stdSerializers.err
    },
    src: true
});

//Note: You should never change this to something that could contain "real" data
var SHARD_PATH = '/node_manatee_test';
var ELECTION_PATH = SHARD_PATH + '/election';
var STATE_PATH = SHARD_PATH + '/state';
var ZK_CONN_STR = process.env.ZK_CONN_STR || '127.0.0.1';

/*
 * Helpers
 */

function id(ip) {
    return (ip + ':5432:12345-');
}

function u(ip) {
    return ('tcp://' + ip + ':5432');
}

function stateFrom(ips) {
    var ret = {
        'async': []
    };
    if (ips[0]) {
        ret.primary = {
            'pgUrl': u(ips[0])
        };
    }
    if (ips[1]) {
        ret.sync = {
            'pgUrl': u(ips[1])
        };
    }
    for (var i = 2; i < ips.length; ++i) {
        ret.async.push({
            'pgUrl': u(ips[i])
        });
    }
    return (new Buffer(JSON.stringify(ret, null, 0)));
}

function addAndVerify(opts, idPrefix, expected, cb) {
    var createCalled = false;
    if (expected) {
        function onTopology(topology) {
            if (!createCalled) {
                opts.t.fail('topology change before create called');
            }
            opts.t.deepEqual(expected, topology, 'topologies dont match');
            opts.manatee.removeListener('topology', onTopology);
            return (cb());
        }
        opts.manatee.on('topology', onTopology);
    }

    function onCreate(err, seqPath) {
        if (err) {
            return (cb(err));
        }
        opts.ids[idPrefix] = seqPath;
        createCalled = true;
        if (!expected) {
            return (cb());
        }
    }

    var path = ELECTION_PATH + '/' + idPrefix;
    opts.zk.create(path, new Buffer('foo'),
                   zkClient.CreateMode.EPHEMERAL_SEQUENTIAL,
                   onCreate);
}

function delAndVerify(opts, idPrefix, expected, cb) {
    if (!opts.ids[idPrefix]) {
        return (cb(new Error('no record of that id in zk: ' + idPrefix)));
    }
    if (expected) {
        var deleteCalled = false;
        function onTopology(topology) {
            if (!deleteCalled) {
                opts.t.fail('topology change before delete called');
            }
            opts.t.deepEqual(expected, topology, 'topologies dont match');
            opts.manatee.removeListener('topology', onTopology);
            return (cb());
        }
        opts.manatee.on('topology', onTopology);
    }

    opts.zk.remove(opts.ids[idPrefix], function (err, seqPath) {
        if (err) {
            return (cb(err));
        }
        delete opts.ids[idPrefix];
        deleteCalled = true;
        if (!expected) {
            return (cb());
        }
    });
}

function createStateAndVerify(opts, ips, expected, cb) {
    var createCalled = false;
    function onTopology(topology) {
        if (!createCalled) {
            opts.t.fail('topology change before create called');
        }
        opts.t.deepEqual(expected, topology, 'topologies dont match');
        opts.manatee.removeListener('topology', onTopology);
        return (cb());
    }
    opts.manatee.on('topology', onTopology);

    function onCreate(err) {
        if (err) {
            return (cb(err));
        }
        createCalled = true;
    }

    opts.zk.create(STATE_PATH, stateFrom(ips),
                   zkClient.CreateMode.PERSISTANT,
                   onCreate);
}

function setStateAndVerify(opts, ips, expected, cb) {
    var setCalled = false;
    function onTopology(topology) {
        if (!setCalled) {
            opts.t.fail('topology change before set called');
        }
        opts.t.deepEqual(expected, topology, 'topologies dont match');
        opts.manatee.removeListener('topology', onTopology);
        return (cb());
    }
    opts.manatee.on('topology', onTopology);

    function onSet(err) {
        if (err) {
            return (cb(err));
        }
        setCalled = true;
    }

    opts.zk.setData(STATE_PATH, stateFrom(ips), function (err) {
        if (err) {
            return (cb(err));
        }
        setCalled = true;
    });
}

function delStateAndVerify(opts, expected, cb) {
    var deleteCalled = false;
    function onTopology(topology) {
        if (!deleteCalled) {
            opts.t.fail('topology change before delete called');
        }
        opts.t.deepEqual(expected, topology, 'topologies dont match');
        opts.manatee.removeListener('topology', onTopology);
        return (cb());
    }
    opts.manatee.on('topology', onTopology);

    opts.zk.remove(STATE_PATH, function (err) {
        if (err) {
            return (cb(err));
        }
        deleteCalled = true;
    });
}

function cleanTestData(opts, subcb) {
    opts.zk.remove(STATE_PATH, function (err) {
        opts.zk.getChildren(ELECTION_PATH, function (err2, cs) {
            vasync.forEachParallel({
                'inputs': cs,
                'func': function d(c, scb) {
                    var p = ELECTION_PATH + '/' + c;
                    opts.zk.remove(p, scb);
                }
            }, subcb);
        });
    });
}

/**
 * This goes through the process of adding and removing nodes in /election
 * and making sure we get 'correct' topology events.
 */
function cycle(opts, cb) {
    var ip1 = '19.19.19.19';
    var ip2 = '20.20.20.20';
    var ip3 = '21.21.21.21';
    opts.ids = {};
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            cleanTestData,
            // No state, so we rely on ordering
            function createOneA(_, subcb) {
                addAndVerify(opts, id(ip1), [ u(ip1) ], subcb);
            },
            function createTwoA(_, subcb) {
                addAndVerify(opts, id(ip2), [ u(ip1), u(ip2) ], subcb);
            },
            function deleteOneA(_, subcb) {
                delAndVerify(opts, id(ip1), [ u(ip2) ], subcb);
            },
            function createThreeA(_, subcb) {
                addAndVerify(opts, id(ip3), [ u(ip2), u(ip3) ], subcb);
            },
            function deleteTwoA(_, subcb) {
                delAndVerify(opts, id(ip2), [ u(ip3) ], subcb);
            },
            function deleteThreeA(_, subcb) {
                delAndVerify(opts, id(ip3), [ ], subcb);
            },
            //Starting from scratch, roll in a state, update, then delete
            function createStateB(_, subcb) {
                createStateAndVerify(opts, [ ip1, ip2 ],
                                     [ u(ip1), u(ip2) ], subcb);
            },
            function updateStateB(_, subcb) {
                setStateAndVerify(opts, [ ip1, ip2, ip3 ],
                                  [ u(ip1), u(ip2), u(ip3) ], subcb);
            },
            function deleteStateB(_, subcb) {
                delStateAndVerify(opts, [], subcb);
            },
            //Starting from scratch, get ordered, add state, revert to ordered
            function createOneC(_, subcb) { // 1
                addAndVerify(opts, id(ip1), [ u(ip1) ], subcb);
            },
            function createTwoC(_, subcb) { // 1, 2
                addAndVerify(opts, id(ip2), [ u(ip1), u(ip2) ], subcb);
            },
            function createStateC(_, subcb) {
                createStateAndVerify(opts, [ ip3, ip1 ],
                                     [ u(ip3), u(ip1) ], subcb);
            },
            function createThreeC(_, subcb) { // 1, 2, 3
                addAndVerify(opts, id(ip3), null, subcb);
            },
            function deleteOneC(_, subcb) { // 2, 3
                delAndVerify(opts, id(ip1), null, subcb);
            },
            function updateStateC(_, subcb) {
                setStateAndVerify(opts, [ ip1, ip2, ip3 ],
                                  [ u(ip1), u(ip2), u(ip3) ], subcb);
            },
            function deleteStateC(_, subcb) { // Check that it reverted
                delStateAndVerify(opts, [ u(ip2), u(ip3) ], subcb);
            },
            function deleteTwoC(_, subcb) {
                delAndVerify(opts, id(ip2), [ u(ip3) ], subcb);
            },
            function deleteThreeC(_, subcb) {
                delAndVerify(opts, id(ip3), [ ], subcb);
            }
        ]
    }, function (err) {
        if (err) {
            return (cb(err));
        }
        if (Object.keys(opts.ids).length > 0) {
            opts.t.fail('ids are left over after cycle: ' + opts.ids);
            return (cb(new Error()));
        }
        delete opts.ids;
        return (cb());
    });
}

/*
 * Tests
 */

exports.clientTest = function (t) {
    var opts = {
        't': t
    };
    vasync.pipeline({
        'arg': opts,
        'funcs': [
            function createClient(_, cb) {
                cb = once(cb);

                var tid = null;
                _.manatee = manatee.createClient({
                    log: LOG,
                    path: SHARD_PATH,
                    zk: {
                        connStr: ZK_CONN_STR,
                        opts: {
                            'sessionTimeout': 5000,
                            'spinDelay': 1000,
                            'retries': 2
                        }
                    }
                });

                var calledReady = false;
                _.manatee.on('ready', function () {
                    calledReady = true;
                });

                _.manatee.on('topology', function (top) {
                    if (tid) {
                        clearTimeout(tid);
                    }
                    t.ok(calledReady, 'ready wasn\'t called first');
                    //Topology should be empty
                    t.ok(top, 'topology');
                    t.equals(0, top.length);
                    return (cb());
                });

                _.manatee.on('error', function (err) {
                    if (tid) {
                        clearTimeout(tid);
                    }
                    t.fail(err);
                    return (cb(err));
                });

                tid = setTimeout(function () {
                    var err = new Error('client didn\'t emit topology');
                    return (cb(err));
                }, 5000);
            },
            function createDirectory(_, cb) {
                cb = once(cb);
                _.manatee.removeAllListeners('topology');
                var topologyCalled = false;
                function onTopology(topology) {
                    console.log('TOPOLOGY ', topology);
                    topologyCalled = true;
                }
                _.manatee.on('topology', onTopology);
                //We're cheating here... but we'd rather not have to manage
                // yet another zk client.
                _.zk = _.manatee._zk;
                _.zk.mkdirp(SHARD_PATH + '/election', function (err) {
                    //Waiting to see if topology is called...
                    setTimeout(function () {
                        _.manatee.removeListener('topology', onTopology);
                        if (topologyCalled) {
                            t.fail('topology called when no changes were made');
                        }
                        return (cb(err));
                    }, 1000);
                });
            },
            function cycleOnce(_, cb) {
                cycle(_, cb);
            },
            function cycleTwice(_, cb) {
                cycle(_, cb);
            },
            cleanTestData,
            function deleteDirectories(_, cb) {
                _.zk.remove(ELECTION_PATH, function (err) {
                    _.zk.remove(SHARD_PATH, cb);
                });
            }
        ]
    }, function (err) {
        if (opts.manatee) {
            opts.manatee.close();
        }
        if (err) {
            t.fail(err.message);
        }
        t.done();
    });
};
