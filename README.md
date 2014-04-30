
                       _.---.._
          _        _.-' \  \    ''-.
        .'  '-,_.-'   /  /  /       '''.
       (       _                     o  :
        '._ .-'  '-._         \  \-  ---]
                      '-.___.-')  )..-'
                               (_/

# Overview
This is the client for [Manatee](http://www.seacow.io). Consumers can use this
client to determine the current Manatee shard topology.

# Example
```javascript
var manatee = require('node-manatee');

var client = manatee.createClient({
   "path": "/manatee/1/election",
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
client.on('ready', function () {
    console.log('manatee client ready');
});

client.on('topology', function (urls) {
    console.log({urls: urls}, 'topology changed');
});

client.on('error', function (err) {
    console.log({err: err}, 'got client error');
});
```
# API
The client emits `ready`, `topology`, and `error` events.

## ready
The `ready` event is emitted once, when the client has been successfully
initialized.

## topology
The `topology` event is emitted everytime there is a possible change in the
Shard topology. It emits an ordered array of urls like so:

```javascript
['tcp://postgres@10.0.0.0:5432', 'tcp://postgres@10.0.0.1:5432', 'tcp://postgres@10.0.0.2:5432']
```

Where the first element will be the primary, the second element the sync, and
the third and additional element asyncs. Only the primary should be used for
writes. If you want strongly consistent data, then only the primary should be
used for reads as well. Otherwise, all 3 nodes can be used for reads, but data
on the sync[1] and async will be slightly out of date compared to the primary.

[1] Synchronous replication only ensures that the transaction logs(xlog) are
persisted to the sync standby when a write request is complete. This doesn't
mean the xlog has been processed by the sync. Thus the data form the request
will not be immediately available on the sync until it has processed the xlogs
from the request.

## error
`error` is emitted when there is an unrecoverable error with the client.
Consumers should reconnect on error events.
