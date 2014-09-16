<!--
j   This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# node-manatee

                       _.---.._
          _        _.-' \  \    ''-.
        .'  '-,_.-'   /  /  /       '''.
       (       _                     o  :
        '._ .-'  '-._         \  \-  ---]
                      '-.___.-')  )..-'
                               (_/

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

# Overview
This is the client for [Manatee](http://www.seacow.io). Consumers can use this
client to determine the current Manatee shard topology. For background on
Manatee itself, look [here](http://www.seacow.io).

Note this client does not provide you with a handle to an actual PostgreSQL
connection. It simply returns the topology of the shard. You will need to
manage the PostgreSQL connections yourself.

# Example
```javascript
var manatee = require('node-manatee');

var client = manatee.createClient({
    // Path to the shard's Zookeeper path.
   "path": "/manatee/1",
    // node-zkplus config.
   "zk": {
       "connectTimeout": 2000,
       "servers": [{
           "host": "172.27.10.97",
           "port": 2181
       }, {
           "host": "172.27.10.90",
           "port": 2181
       }, {
           "host": "172.27.10.101",
           "port": 2181
       }],
       "timeout": 20000
   }
});
client.once('ready', function () {
    console.log('manatee client ready');
});

client.on('topology', function (urls) {
    console.log({urls: urls}, 'topology changed');
    // insert code here to manage the PG connections.
});

client.on('error', function (err) {
    console.error({err: err}, 'got client error');
});
```

# Configuration
The client config is a JSON object which takes the following parameters.

* `path`, which is the Zookeeper path of the specific shard. This should be
  identical to the `shardPath` parameter in the Manatee server's `sitter.json`
  config.
* `zk`, which is the [zkplus](http://mcavage.me/node-zkplus/)
  configuration [object](http://mcavage.me/node-zkplus/#zkpluscreateclientoptions).
* An optional `log` object, which is a
  [bunyan](https://github.com/trentm/node-bunyan) logger.

# API
The client emits `ready`, `topology`, and `error` events to consumers.

## ready
The `ready` event is emitted once, when the client has been successfully
initialized.

## topology
The `topology` event is emitted everytime there is a possible change in the
Shard topology. It emits an ordered array of urls like so:

```javascript
['tcp://postgres@10.0.0.0:5432', 'tcp://postgres@10.0.0.1:5432', 'tcp://postgres@10.0.0.2:5432']
```

Where the first element is the primary, the second element the sync, and the
third and any additional elements asyncs. Only the primary can be used for
writes. If you want strongly consistent data, then only use the primary for
reads as well. Otherwise, all 3 nodes can be used for reads, but data on the
sync[1] and async will be slightly out of date compared to the primary.

[1] Synchronous replication only ensures that the transaction logs(xlog) are
persisted to the standby from the primary when a write request is complete.
This doesn't mean the xlog has been processed by the standby. Thus the data
from the request will not be immediately available on the standby until it has
processed the xlogs from the request a short time later.

## error
`error` is emitted when there is an unrecoverable error with the client.
Consumers should reconnect on error events.
