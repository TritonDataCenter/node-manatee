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
var assert = require('assert-plus');
var backoff = require('backoff');
var bunyan = require('bunyan');
var mod_crypto = require('crypto');
var mod_mooremachine = require('mooremachine');
var mod_net = require('net');
var mod_url = require('url');
var once = require('once');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');
var zkClient = require('joyent-zookeeper-client');

var EventEmitter = require('events').EventEmitter;

/**
 * Create a Manatee client.
 *
 * @constructor
 * @augments EventEmitter
 *
 * @param {object} options Manatee options.
 * @param {Bunyan} [options.log] Bunyan logger.
 * @param {string} options.path ZK path of the shard, for example:
 * /manatee/1.moray.coal.joyent.us
 * @param {object} options.zk ZK client options.
 * @param {Bunyan} [options.zk.log] Bunyan logger.
 * @param {object[]} options.zk.connStr ZK conn string.  For example:
 * 10.99.99.80:2181,10.99.99.81:2181,10.99.99.82:2181
 * @param {number} options.zk.opts opts sent directly to the
 * node-zookeeper-client
 *
 * @throws {Error} If the options object is malformed.
 *
 * @fires error If there is an error creating the ZKClient.
 * @fires topology When the topology has changed, in the form of an array of
 * Postgres URLS. e.g  ["tcp://postgres@127.0.0.1:30003/postgres",
 * "tcp://postgres@127.0.0.1:20003/postgres",
 * "tcp://postgres@127.0.0.1:10003/postgres"], where the first element is the
 * primary, the second element is the sync slave, and the third and additional
 * elements are the async slaves.
 * @fires ready When the client is ready and connected.
 *
 */
function Manatee(options) {
    assert.object(options, 'options');
    assert.optionalObject(options.log, 'options.log');
    assert.string(options.path, 'options.path');
    assert.object(options.zk, 'options.zk');

    var self = this;
    EventEmitter.call(this);

    /** @type {Bunyan} The bunyan log object */
    self._log = null;

    if (options.log) {
        self._log = options.log.child();
    } else {
        self._log = bunyan.createLogger({
            level: (process.env.LOG_LEVEL || 'info'),
            name: 'mantee-client',
            serializers: {
                err: bunyan.stdSerializers.err
            }
        });
    }

    /**
     * @type {string} Path under which shard metadata such as elections are
     * stored. e.g. /manatee/1.moray.coal.joyent.us
     */
    self._electionPath = options.path + '/election';
    self._clusterStatePath = options.path + '/state';
    /** @type {Object} The zk cfg */
    self._zkCfg = options.zk;
    /** @type {zkplus.client} The ZK client */
    self._zk = null;
    self._inited = false;
    self._closed = false;
    self._clusterState = null;
    self._actives = null;
    self._urls = [];

    self.__defineGetter__('topology', function topology() {
        return (self._urls);
    });

    process.nextTick(function init() {
        self._init();
    });
}
util.inherits(Manatee, EventEmitter);

module.exports = {
    createClient: function createClient(options) {
        return (new Manatee(options));
    },
    createPrimaryResolver: function createPrimaryResolver(options) {
        return (new ManateePrimaryResolver(options));
    }
};

/**
 * Close connection to zookeeper.
 */
Manatee.prototype.close = function close() {
    var self = this;
    var log = self._log;
    if (self._zk) {
        self._zk.removeAllListeners();
        self._zk.on('error', function (err) {
            log.error({err: err}, 'err after zk close');
        });
        self._zk.close();
    }
    if (!self._closed) {
      self._closed = true;
      self.emit('close');
    }
};


/**
 * #@+
 * @private
 * @memberOf Shard
 */

Manatee.prototype._handleTopologyChange = function handleTopologyChange(urls) {
    var self = this;
    urls = urls || [];

    //Debounce topology changes
    var equal = urls.length === self._urls.length;
    self._urls.forEach(function (u, i) {
        equal = equal && (u === urls[i]);
    });
    if (equal) {
        return;
    }

    self._urls = urls;
    if (self._inited) {
        self.emit('topology', self._urls);
    }
};

Manatee.prototype._handleClusterState = function handleClusterState(res) {
    var self = this;
    var log = self._log;

    if (!res || !res.data) {
        if (!self._inited) {
            return;
        }

        //If the cluster state was deleted, we now revert back to using the
        // election as the topology
        self._clusterState = null;
        if (self._actives) {
            self._handleTopologyChange(self._childrenToUrls(self._actives));
        }
        return;
    }

    res.data = res.data.toString('utf8');
    log.debug(res, 'manatee: handling cluster state update');
    try {
        self._clusterState = JSON.parse(res.data);
    } catch (err) {
        var msg = 'error JSON parsing zookeeper cluster state';
        log.fatal({
            err: err,
            data: res.data,
            zkpath: self._clusterStatePath
        }, msg);
        self.emit('error', new Error(msg));
        return;
    }

    self._handleTopologyChange(self._clusterStateToUrls(self._clusterState));
};

Manatee.prototype._handleActive = function handleActive(res) {
    var self = this;
    var log = self._log;

    if (!res || !res.children) {
        self._actives = null;
        log.debug('no actives, nothing to do');
        return;
    }

    //Always keep track of our current actives.
    self._actives = res.children;

    if (self._clusterState) {
        //We're relying on the cluster state, so we can just ignore the actives
        // notification.
        return;
    }

    self._handleTopologyChange(self._childrenToUrls(self._actives));
};

Manatee.prototype._setWatches = function setWatches(zk, cb) {
    var self = this;
    var log = self._log;

    log.debug('zk: setting up watches');

    //Watch the cluster state
    function watchClusterState(_, subcb) {
        function onWatching(err, res) {
            if (!err) {
                self._handleClusterState(res);
            }
            return (subcb(err));
        }
        self._watchNode(zk,
                        self._clusterStatePath,
                        self._handleClusterState.bind(self),
                        onWatching);
    }

    //Watch the ephemeral directory
    function watchEphemeralDirectory(_, subcb) {
        function onWatching(err, res) {
            if (!err) {
                self._handleActive(res);
            }
            return (subcb(err));
        }
        self._watchNode(zk,
                        self._electionPath,
                        self._handleActive.bind(self),
                        onWatching);
    }

    vasync.pipeline({
        'funcs': [
            watchClusterState,
            watchEphemeralDirectory
        ]
    }, function (err) {
        log.debug('zk: done setting up watches');
        return (cb(err));
    });
};

/**
 * Inits and manages the zookeeper client.
 */
Manatee.prototype._init = function _init() {
    var self = this;
    var log = self._log;

    log.debug('init: entered');

    var emitReady = once(function emitReadyFunc() {
        self._inited = true;
        process.nextTick(self.emit.bind(self, 'ready'));
        process.nextTick(self.emit.bind(self, 'topology', self._urls));
    });

    function setupZkClient() {
        var zk = zkClient.createClient(self._zkCfg.connStr, self._zkCfg.opts);
        self._zk = zk;

        var resetZkClientOnce = once(function resetZkClient() {
            if (zk) {
                zk.close();
            }
            process.nextTick(setupZkClient);
        });
        var setWatchesOnce = once(self._setWatches.bind(self));

        //Creator says this is "Java Style"
        zk.on('state', function (s) {
            //Just log it.  The other events are called.
            log.trace(s, 'zk: new state');
        });

        //Client is connected and ready. This fires whenever the client is
        // disconnected and reconnected (more than just the first time).
        zk.on('connected', function () {
            log.debug(zk.getSessionId(), 'zk: connected');
            setWatchesOnce(zk, function (err) {
                if (err) {
                    log.error(err, 'zk: err setting up data, reiniting');
                    return (resetZkClientOnce());
                } else {
                    emitReady();
                }
            });
        });

        //Client is connected to a readonly server.
        zk.on('connectedReadOnly', function () {
            //Don't do anything for this.
            log.debug('zk: connected read only');
        });

        //The connection between client and server is dropped.
        zk.on('disconnected', function () {
            //Don't do anything for this.
            log.debug('zk: disconnected');
        });

        //The client session is expired.
        zk.on('expired', function () {
            //This causes the client to "go away".  A new one should be
            // created after this.
            log.info('zk: session expired, reiniting.');
            resetZkClientOnce();
        });

        //Failed to authenticate with the server.
        zk.on('authenticationFailed', function () {
            //Don't do anything for this.
            log.fatal('zk: auth failed');
        });

        //Not even sure if this is really an error that would be emitted...
        zk.on('error', function (err) {
            //Create a new ZK.
            log.warn({err: err}, 'zk: unexpected error, reiniting');
            resetZkClientOnce();
        });

        zk.connect();
    }

    setupZkClient();
};


/**
 * Will call the way function on any change to the node or its children.
 * returns the following structure:
 *
 * {
 *    'data': ...         //can be null
 *    'children': [ ... ] //can be empty or null)
 *    'version': ...      //can be null
 * }
 *
 * When all elements are null it means that the node doesn't exist
 *
 * Zookeeper watches will only fire once.  That's right.  Once.  So anytime
 * a watch fires we need to re-register the watch.
 *
 * The wat function is called each time something changes.
 * The cb is only called once, after the watch is initally set with the initial
 * read.
 */
Manatee.prototype._watchNode = function _watchNode(zk, path, wat, cb) {
    var self = this;
    var currStat = null;
    var currChildren = null;
    var currData = null;

    cb = once(cb);

    function getRes() {
        return ({
            data: currData ? currData : null,
            version: currStat ? currStat.version : null,
            children: currChildren ? currChildren : null
        });
    }

    //The event this gives back isn't what it changed to, only that it
    // changed.
    function dataWatchFired(event) {
        if (self._closed) {
            return;
        }
        return (getData(true));
    }
    function childrenWatchFired(event) {
        if (self._closed) {
            return;
        }
        return (registerChildrenWatch());
    }
    function getData(regWatch) {
        if (self._closed) {
            return;
        }
        zk.getData(path, function (err, data, stat) {
            var prevStat = currStat;
            currStat = stat;
            currData = data;
            if (err && err.name === 'NO_NODE') {
                err = null;
                currChildren = null;
            } else if (err) {
                return (setTimeout(getData, 5000));
            }

            function done() {
                if (regWatch) {
                    registerDataWatch();
                }
            }

            // Init
            if (!cb.called) {
                // If we exist, we need to set up the children watch.
                if (currStat) {
                    registerChildrenWatch(done);
                } else {
                    cb(null, getRes());
                    done();
                }
                // After Init
            } else {
                // Ongoing, if we are newly created, we need to set up the
                // children watch again since the old disappears into the ether
                if (!prevStat && currStat && currStat.version === 0) {
                    registerChildrenWatch(done);
                } else {
                    wat(getRes());
                    done();
                }
            }
        });
    }
    function registerChildrenWatch(subcb) {
        if (self._closed) {
            return;
        }
        zk.getChildren(path, childrenWatchFired, function (err, children) {
            if (err && err.name === 'NO_NODE') {
                return;
            }
            if (err) {
                return (setTimeout(registerChildrenWatch, 5000));
            }
            currChildren = children;
            // Init
            if (!cb.called) {
                cb(null, getRes());
                // After Init
            } else {
                wat(getRes());
            }
            if (subcb) {
                subcb();
            }
        });
    }
    function registerDataWatch() {
        if (self._closed) {
            return;
        }
        zk.exists(path, dataWatchFired, function (err, stat) {
            //We might have missed a watch while we were processing "other
            // things" Just fetch the data.  We'll only register a watch when
            // the watch fires.
            if (stat && currStat && currStat.version !== stat.version) {
                getData(false);
            }
        });
    }
    getData(true);
};

/**
 * Election nodes are postfixed with -123456. so sort by the number after -,
 * and you'll have:
 * [primary, sync, async, ..., asyncn]
 *
 * @param {string[]} children The array of Postgres peers.
 * @return {string[]} The array of transformed PG URLs. e.g.
 * [tcp://postgres@10.0.0.0:5432]
 */
Manatee.prototype._childrenToUrls = function childrenToUrls(children) {
    function compare(a, b) {
        var seqA = parseInt(a.substring(a.lastIndexOf('-') + 1), 10);
        var seqB = parseInt(b.substring(b.lastIndexOf('-') + 1), 10);

        return (seqA - seqB);
    }

    /**
     * transform an zk election node name into a postgres url.
     * @param {string} zkNode The zknode, e.g.
     * 10.77.77.9:pgPort:backupPort:hbPort-0000000057, however previous
     * versions of manatee will only have 10.77.77.9-0000000057, so we have to
     * be able to disambiguate between the 2.
     *
     * @return {string} The transformed PG URL, e.g.
     * tcp://postgres@10.0.0.0:5432
     */
    function transformPgUrl(zkNode) {
        var encodedString = zkNode.split('-')[0];
        var data = encodedString.split(':');
        /*
         * if we're using the legacy format, there will not be ':', and as such
         * the split will return an array of length 1
         */
        if (data.length === 1) {
            return 'tcp://' + data[0];
        } else {
            return 'tcp://' + data[0] + ':' + data[1];
        }
    }

    var urls = (children || []).sort(compare).map(function (val) {
        return transformPgUrl(val);
    });

    return urls;
};

/**
 * Cluster state looks like this:
 *
 * {
 *   ...
 *   "primary": {
 *     "pgUrl": "tcp://postgres@10.77.77.52:5432/postgres",
 *     ...
 *   },
 *   "sync": <same as primary>,
 *   "async": [ <same as primary>, ... ]
 *   ...
 * }
 */
Manatee.prototype._clusterStateToUrls = function clusterStateToUrls(cs) {
    var urls = [];
    if (cs.primary) {
        urls.push(cs.primary.pgUrl);
    }
    if (cs.sync) {
        urls.push(cs.sync.pgUrl);
    }
    if (cs.async) {
        cs.async.forEach(function (a) {
            urls.push(a.pgUrl);
        });
    }
    return (urls);
};


/*
 *
 */
function ManateePrimaryResolver(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    this.mpr_opts = options;
    this.mpr_manatee = null;
    this.mpr_previous = null;
    this.mpr_primary = null;
    this.mpr_lastError = null;
    this.mpr_log = options.log.child({
        component: 'ManateePrimaryResolver'
    });

    mod_mooremachine.FSM.call(this, 'stopped');
}
util.inherits(ManateePrimaryResolver, mod_mooremachine.FSM);

ManateePrimaryResolver.prototype.state_stopped = function (S) {
    S.on(this, 'startAsserted', function () {
        S.gotoState('starting');
    });
};

ManateePrimaryResolver.prototype.state_starting = function (S) {
    var self = this;

    if (this.mpr_manatee === null) {
        this.mpr_manatee = new Manatee(this.mpr_opts);
    }


    S.on(this.mpr_manatee, 'ready', function () {
        S.gotoState('running');
    });

    S.on(this.mpr_manatee, 'error', function (err) {
        self.mpr_log.warn(err, 'manatee client emitted error');
        self.mpr_lastError = err;
        S.gotoState('failed');
    });
};

ManateePrimaryResolver.prototype.state_running = function (S) {
    var self = this;

    S.on(self.mpr_manatee, 'topology', function (urls) {
        self.mpr_log.trace({
            urls: urls
        }, 'manatee topology changed');

        var primary = mod_url.parse(urls[0]);

        assert.strictEqual(primary.protocol, 'tcp:');
        assert.ok(
            mod_net.isIPv4(primary.hostname) ||
            mod_net.isIPv6(primary.hostname));

        self.diffPrimaryAndEmit({
            name: 'primary',
            address: primary.hostname,
            port: parseInt(primary.port, 10)
        });
    });

    S.on(self.mpr_manatee, 'error', function (err) {
        self.mpr_log.warn(err, 'manatee client emitted error');
        self.mpr_lastError = err;
        S.gotoState('failed');
    });

    S.on(self, 'stopAsserted', function () {
        S.gotoState('stopping');
    });
};

ManateePrimaryResolver.prototype.state_stopping = function (S) {
    S.on(this.mpr_manatee, 'close', function () {
        S.gotoState('stopped');
    });

    this.mpr_manatee.close();
    this.mpr_manatee = null;
};

ManateePrimaryResolver.prototype.state_failed = function (S) {
    this.mpr_previous = this.mpr_primary;
    this.mpr_primary = null;

    S.timeout(1000, function () {
        S.gotoState('starting');
    });

    S.on(this, 'stopAsserted', function () {
        S.gotoState('stopped');
    });
};

ManateePrimaryResolver.prototype.start = function () {
    assert.ok(this.isInState('stopped'));
    this.emit('startAsserted');
};

ManateePrimaryResolver.prototype.stop = function () {
    assert.ok(this.isInState('running') || this.isInState('failed'));
    this.emit('stopAsserted');
};

ManateePrimaryResolver.prototype.count = function () {
    return (this.mpr_primary === null ? 0 : 1);
};

ManateePrimaryResolver.prototype.getLastError = function () {
    return (this.mpr_lastError);
};

ManateePrimaryResolver.prototype.list = function () {
    var backends = {};
    if (this.mpr_primary !== null) {
        backends[this.mpr_primary.key] = this.mpr_primary;
    }
    return backends;
};

ManateePrimaryResolver.prototype.diffPrimaryAndEmit = function (np) {
    var op = this.mpr_primary;

    if (op !== null &&
        op.name === np.name &&
        op.address === np.address &&
        op.port === np.port) {
        return;
    }

    np.key = mod_crypto.randomBytes(9).toString('base64');

    this.mpr_previous = op;
    this.mpr_primary = np;

    this.mpr_log.info({
        oldPrimary: this.mpr_previous,
        newPrimary: this.mpr_primary
    }, 'Manatee primary has changed');

    this.emit('added', np.key, np);

    if (op !== null) {
        this.emit('removed', op.key);
    }
};
