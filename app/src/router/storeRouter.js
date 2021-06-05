const express = require('express');
const storeRouter = express.Router();
storeRouter.use(express.json());
const http = require('http');


let keyvalueStore = {};
var vectorClock = {};
const OFFSET = 2;

var HashRing = require('hashring');

//Every view that may be occupied by a replica.
var views = process.env.VIEW.split(',');  //10.0.0.2:8085, 10.0.0.3:8085, 10.0.0.4:8085
var numViews = views.length;

let ring = setHashRing(process.env.SHARD_COUNT);

// set the consistent hashing hashring based on shardCount
function setHashRing(shardCount) {
    let tempRing = [];
    for (var i=1; i<= shardCount; i++) {
        tempRing.push(i.toString());
    }
    let ring = new HashRing(tempRing, 'md5',
        {
            'replicas': 1
        });
    return ring;
}

// return the hash of a key
function hashKey(key) {
    return ring.get(key);
}


// add newKVS to current KVS, only for inter-view use
function setKVS(newKVS) {
    for (var key in newKVS) {
        let updateFlag = true;
        if (vectorClock.hasOwnProperty(key)) {
           for(var index = 0; index < newCM[key].length; index++) {
                if (newCM[key][index] < vectorClock[key][index]) {
                    updateFlag = false;
                }
            }     
        }
        if (updateFlag) {
            keyvalueStore[key] = newKVS[key];
        }   
    }
}

// replace the KVS with newKVS
function replaceKVS(newKVS) {
    keyvalueStore = newKVS;
}

// retrieve the KVS
function retrieveKVS() {
    return keyvalueStore;
}

// replace causal metadata for new
function replaceCM(newCM) {
    vectorClock = newCM;
}

// get length of KVS for shard-id-key-count
function getLength() {
    return (Object.keys(keyvalueStore)).length;
}

// add new causal metadata to current
function setCM(newCM) {
    vectorClock = pointwiseMaximum(vectorClock, newCM);
}

// get kvs from another replica and merge it with current kvs
// or replace, depending on replace param
function getKVS(views1, replace) {
    return new Promise(function(resolve, reject) {
        for (var view of views1) {
            let replicadownFlag = false;
            // const shards = shardRouter.getShards(); && shards[shardRouter.getThisShard()].includes(view)
            if (view != process.env.SOCKET_ADDRESS) {
                const params = view.split(':');
                const options = {
                    protocol: 'http:',
                    host: params[0],
                    port: params[1],
                    path: '/key-value-store/sync-kvs', // view only route
                    method: 'GET',
                    headers: {
                    }
                };
                const req = http.request(options, function(res) {
                    let body = '';
                    res.on('data', function (chunk) {
                        body += chunk;
                    });
                    res.on('end', function() {
                        // console.log(body);
                        if (!replace) {
                            setKVS(JSON.parse(body).kvs); // add kvs to current KVSs
                            setCM(JSON.parse(body).cm); // add cm to current cm  
                        } else {
                            replaceKVS(JSON.parse(body).kvs); // replace
                            replaceCM(JSON.parse(body).cm); // replace  
                        }
                        
                        resolve();
                    })
                });
                req.on('error', function(error) {
                    console.log("Error: Could not connect to replica at " + view);
                    replicadownFlag = true; // could not retrieve KVS
                });
                req.end();
                // if KVS successfully retrieved, done
                // else try with next view in 'views'
                if (!replicadownFlag) {
                    break;
                }
            }   
        }
        resolve();
    })
}


// need to export setKVS function for index.js use
// (storeRouter in index.js) -> storeRouter.router
module.exports = {
    router:storeRouter,
    setKVS:setKVS,
    setCM:setCM,
    getKVS:getKVS,
    replaceKVS:replaceKVS,
    replaceCM:replaceCM,
    getLength:getLength,
    retrieveKVS:retrieveKVS,
    setHashRing:setHashRing,
    hashKey:hashKey
};

// Require it here to avoid circular dependency
const shardRouter = require("./shardRouter.js")

storeRouter.route('/')
.all((req, res, next) => {
    res.status(405).json({
      status: 405,
      error: "A key is required",
    });
});

// view-only route for ease in updating KVS
storeRouter.route('/sync-kvs')
.get(async (req, res) => {

    res.status(200).json({"message": "Retrieved successfully", "kvs": keyvalueStore, "cm": vectorClock});
});

// shard store only route for resharding
storeRouter.route('/reshard')
.put(async (req, res) => {
    // intialize json object with empty kvs for each shard
    let keysToSend = {};
    let cmToSend = {};
    for (shard in shardRouter.getShards()) {
        keysToSend[shard] = {};
        cmToSend[shard] = {};
    }
    // loop through this kvs, hash each key
    // then place into temp kvs for correct shard
    for (key in keyvalueStore) {
        let newShard = hashKey(key);
        if (shardRouter.getThisShard() != newShard) {
            keysToSend[newShard][key] = keyvalueStore[key];
            delete keyvalueStore[key];
            if (vectorClock.hasOwnProperty(key)) {
                cmToSend[newShard][key] = vectorClock[key];
                delete vectorClock[key];
            }
        }
    }
    // for every shard, send temp kvs for update
    for (shard in keysToSend) {
        for (node of shardRouter.getShards()[shard]) {
            // send keysToSend[shard] to this node
            Req(node, 'PUT', '/key-value-store/put-keys', {"KVS":keysToSend[shard], "CM":cmToSend[shard]});
        }
    }
});

// update kvs en masse for resharding
storeRouter.route('/put-keys')
.put(async (req, res) => {
    console.log("kvs size sent" + Object.keys(req.body["KVS"]).length);
    setCM(req.body["CM"]);
    setKVS(req.body["KVS"]);
    console.log("total kvs size after sent" + Object.keys(keyvalueStore).length);
    res.status(200).send();
});

// update hashrin based on new shard count
storeRouter.route('/update-hash')
.put(async (req, res) => {
    ring = setHashRing(req.body["shard-count"]);
    res.status(200).send();
});

storeRouter.route('/:key')
.get(async (req, res) => {

    const { key } = req.params;

   // console.log('In storeRouter GET - key:',key)
    var hashedKey = ring.hash(key);
  //  console.log('In storeRouter GET - hashedKey:',hashedKey)
    var shardId = ring.get(hashedKey);
 //   console.log('In storeRouter GET - shardId:',shardId)
    var shards = shardRouter.getShards();
 //   console.log('In storeRouter GET - shards:',shards)
    var nodes = shards[shardId]
 //   console.log('In storeRouter GET - nodes:',nodes)
    if(nodes.includes(process.env.SOCKET_ADDRESS)) { 
        const val = keyvalueStore[key];
        if (!val){
            res.status(404).json({"error": "Key does not exist", "message": "Error in GET"});
        } else {
            //vectorClock[req.params.key][]+=1 
                res.status(200).json({"message": "Retrieved successfully", 
                                     "causal-metadata":vectorClock,
                                     "value": val});
            }
    }
    else{
            var node = nodes[0];

            const REPLICA_HOST = node.split(':')[0];
            const port = node.split(':')[1];

            // const data = JSON.stringify({
            //     "value": value,
            //     "causal-metadata": causalMetadata
            // });
            const options = {
                protocol: 'http:',
                host: REPLICA_HOST,
                port: port,
                //params: 
                path: `/key-value-store/${key}`,
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    // 'Content-Length': data.length
                    }
            };
            const req = http.request(options, function(resForward) {
                console.log(resForward.statusCode);
                var statusCode = resForward.statusCode;
                let body = '';
                resForward.on('data', function (chunk) {
                    body += chunk;
                });
                resForward.on('end', function() {
                    console.log(body);
                    jsonForm = JSON.parse(body)                    
                    res.status(statusCode).json({
                        "message": jsonForm.message,
                        "causal-metadata": jsonForm["causal-metadata"],
                        "value": jsonForm["value"]
                    })
                })
            });
            req.on('error', function(err) {
                console.log("Error: Request failed at " + view);
            });
            // req.write(data);
            req.end();

        }
    })
.put(async (req, res, next) => {

    checkViews();

    const REPLICA = process.env.SOCKET_ADDRESS;   // Get the REPLICA's address
    const CURRENT_REPLICA_HOST = REPLICA.split(':')[0];   // e.g. 10.0.0.2
    const VECTOR_CLOCK_INDEX = REPLICA.split('.')[3].split(':')[0] - OFFSET;  // Get the last byte of address to use as our vector clock index

    const { key } = req.params;
    const { value } = req.body;
    const causalMetadata = req.body['causal-metadata'];

    console.log('should enter', causalMetadata)


    if (!req.body["value"]) {
        res.status(400).json({
            "error": "Value is missing",
            "message": "Error in PUT"
        });
    } else if (req.params.key.length > 50) {
        res.status(400).json({
            "error": "Key is too long", 
            "message": "Error in PUT"
        });
    } else {

        var hashedKey = ring.hash(key)
        var shardId = ring.get(hashedKey);

        var shards = shardRouter.getShards();

        var nodes = shards[shardId]

        console.log('shardId: ', shardId)
        console.log('shards: ', shards)
        console.log('nodes: ', nodes)

        if(nodes.includes(process.env.SOCKET_ADDRESS)) {
            console.log('should enter',causalMetadata)
            if(causalMetadata.length == 0) {
                console.log('causal metadata is 0')
                keyvalueStore[key] = value;
                vectorClock[key] = [];
                for(var i = 0; i < numViews; i++) {
                    vectorClock[key].push(0);
                }
                vectorClock[key][VECTOR_CLOCK_INDEX] = 1;
                res.status(201).json({
                    "message": "Added successfully",
                    "causal-metadata": vectorClock,
                    "shard-id": shardId
                });
            } else if(await compareVectorClocks(causalMetadata)) {
                if(keyvalueStore.hasOwnProperty(key)) {
                    console.log("in put 1");
                    keyvalueStore[key] = value;
                    if(req.body['broadcast']) {
                        vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
                    } else {
                        vectorClock[key][VECTOR_CLOCK_INDEX] = vectorClock[key][VECTOR_CLOCK_INDEX]+1;
                    }
                    console.log('-------------------', CURRENT_REPLICA_HOST)
                    res.status(200).json({
                        "message": "Updated successfully",
                        "causal-metadata": vectorClock,
                        "shard-id": shardId
                    });
                } else {
                    keyvalueStore[key] = value;
                    vectorClock[key] = [];
                    for(var i = 0; i < numViews; i++) {
                        vectorClock[key].push(0);
                    }
                    if(req.body['broadcast']) {
                        vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
                    } else {
                        vectorClock[key][VECTOR_CLOCK_INDEX] = 1;
                    }
                    res.status(201).json({
                        "message": "Added successfully",
                        "causal-metadata": vectorClock,
                        "shard-id": shardId
                    });
                }
            } else {
                //wait
                vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
                while(!await compareVectorClocks(causalMetadata)) { // while causal metadata is out of date
                    await getKVS(process.env.VIEW.split(','));
                }
                console.log('in else');

                if(keyvalueStore.hasOwnProperty(key)) {
                    console.log("in put 2");
                    keyvalueStore[key] = value;
                    if(req.body['broadcast']) {
                        vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
                    } else {
                        vectorClock[key][VECTOR_CLOCK_INDEX] = vectorClock[key][VECTOR_CLOCK_INDEX]+1;
                    }
                    console.log('-----------------', CURRENT_REPLICA_HOST)
                    res.status(200).json({
                        "message": "Updated successfully",
                        "causal-metadata": vectorClock,
                        "shard-id": shardId
                    });
                } else {
                    keyvalueStore[key] = value;
                    vectorClock[key] = [];
                    for(var i = 0; i < numViews; i++) {
                        vectorClock[key].push(0);
                    }
                    if(req.body['broadcast']) {
                        vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
                    } else {
                        vectorClock[key][VECTOR_CLOCK_INDEX] = 1;
                    }
                    res.status(201).json({
                        "message": "Added successfully",
                        "causal-metadata": vectorClock,
                        "shard-id": shardId
                    });
                }
            }

            if(!req.body['broadcast']) {
                console.log('   i am broadcasting ---------------- ',CURRENT_REPLICA_HOST)
                causalBroadcast(CURRENT_REPLICA_HOST, key, value, vectorClock, nodes);
            }

        } else {

            console.log('in else')
            console.log('redirecting with data', causalMetadata)

            var node = nodes[0];

            const REPLICA_HOST = node.split(':')[0];
            const port = node.split(':')[1];

            const data = JSON.stringify({
                "value": value,
                "causal-metadata": causalMetadata
            });
            const options = {
                protocol: 'http:',
                host: REPLICA_HOST,
                port: port,
                //params: 
                path: `/key-value-store/${key}`,
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': data.length
                    }
            };
            const req = http.request(options, function(resForward) {
                console.log('The http request status code is: ', resForward.statusCode);
                var statusCode = resForward.statusCode
                let body = '';
                resForward.on('data', function (chunk) {
                    body += chunk;
                });
                resForward.on('end', function() {
                    console.log('Ending', body);
                    //Fix here
                    jsonForm = JSON.parse(body)                    
                    res.status(statusCode).json({
                        "message": jsonForm.message,
                        "causal-metadata": jsonForm["causal-metadata"],
                        "shard-id": jsonForm["shard-id"]
                    })
                })
            });
            req.on('error', function(err) {
                console.log("Error: Request failed at " + view);
            });
            req.write(data);
            req.end();
        }

    }
    
})
.delete(async (req,res) => {
    const REPLICA = process.env.SOCKET_ADDRESS;   // Get the REPLICA's address
    const VECTOR_CLOCK_INDEX = REPLICA.split('.')[3].split(':')[0] - OFFSET; 
    const CURRENT_REPLICA_HOST = REPLICA.split(':')[0];  
    const causalMetadata = req.body['causal-metadata']
    const key = req.params.key

    //Delete key value store
   //  var hashedKey = ring.hash(key)
        var shardId = ring.get(key);

        var shards = shardRouter.getShards();

        var nodes = shards[shardId]

    if(nodes.includes(process.env.SOCKET_ADDRESS)) {
        const val = keyvalueStore[key];
        if (!val){
            res.status(404).json({"error": "Key does not exist", "message": "Error in DELETE"});
        } else {

            if(await compareVectorClocks(causalMetadata)){
                delete keyvalueStore[key];
                //creates causal metadata and increment 1 for current replica since it's a write operation
                deleteCausalBroadcast(CURRENT_REPLICA_HOST, key,causalMetadata)
                vectorClock[key][VECTOR_CLOCK_INDEX] = vectorClock[key][VECTOR_CLOCK_INDEX]+1;
                //broadcast to all other replicas
                res.status(200).json({"message":"Deleted successfully","causal-metadata":vectorClock,"shard-id":shardId});

            }else{
                res.status(404).json({"error": "Inconsistent causality", "message": "All causally preceding operations must be complete first before applying DELETE"});
            }
        }
    }
    else{
        let body = await forward_delete(nodes);

    }
})
.all(async(req,res,next) => {
    res.status(405).send();
});

function forward_delete(nodes){
        var node = nodes[0];
        const REPLICA_HOST = node.split(':')[0];
        const port = node.split(':')[1];

        const data = JSON.stringify({
            "value": value,
            "causal-metadata": causalMetadata,
            "broadcast": true
        });
        const options = {
            protocol: 'http:',
            host: REPLICA_HOST,
            port: port,
            //params: 
            path: `/key-value-store/${key}`,
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': data.length
                }
        };
        const req = http.request(options, function(res) {
            console.log(res.statusCode);
            let body = '';
            res.on('data', function (chunk) {
                body += chunk;
            });
            res.on('end', function() {
                console.log(body);
            })
        });
        req.on('error', function(err) {
            console.log("Error: Request failed at " + view);
        });
        req.write(data);
        req.end();
}

async function compareVectorClocks(metadataVC) {

    for(var key in vectorClock) {
        for(var index = 0; index < vectorClock[key].length; index++) {
            if(!metadataVC.hasOwnProperty(key)){
                return false;
            } else if(metadataVC[key][index] > vectorClock[key][index]) {
                return false;
            }
        }
    }
    return true;
}


async function causalBroadcast(CURRENT_REPLICA_HOST, key, value, causalMetadata, nodes) {
    for(view of nodes) {
        const REPLICA_HOST = view.split(':')[0];
        if(REPLICA_HOST != CURRENT_REPLICA_HOST) {
            console.log('CURRENT_REPLICA_HOST', CURRENT_REPLICA_HOST)
            console.log('key', key)
            console.log('value', value)
            console.log('nodes', nodes)
            console.log('view', view)
            console.log('REPLICA_HOST', REPLICA_HOST)
            const port = view.split(':')[1];
            const data = JSON.stringify({
                "value": value,
                "causal-metadata": causalMetadata,
                "broadcast": true
            });
            const options = {
                protocol: 'http:',
                host: REPLICA_HOST,
                port: port,
                //params: 
                path: `/key-value-store/${key}`,
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': data.length
                  }
            };
            const req = http.request(options, function(res) {
                console.log(res.statusCode);
                let body = '';
                res.on('data', function (chunk) {
                    body += chunk;
                });
                res.on('end', function() {
                    console.log(body);
                })
            });
            req.on('error', function(err) {
                console.log("Error: Request failed at " + view);
            });
            req.write(data);
            req.end();

        }
    }
}

async function deleteCausalBroadcast(CURRENT_REPLICA_HOST, key, causalMetadata) {
    for(view of views) {
        const REPLICA_HOST = view.split(':')[0];
        if(REPLICA_HOST != CURRENT_REPLICA_HOST) {
            const port = view.split(':')[1];
            const data = JSON.stringify({
                "causal-metadata": causalMetadata,
                "broadcast": true
            });
            const options = {
                protocol: 'http:',
                host: REPLICA_HOST,
                port: port,
                //params: 
                path: `/key-value-store/${key}`,
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': data.length
                  }
            };
            const req = http.request(options, function(res) {
                console.log(res.statusCode);
                let body = '';
                res.on('data', function (chunk) {
                    body += chunk;
                });
                res.on('end', function() {
                    console.log(body);
                })
            });
            req.on('error', function(err) {
                console.log("Error: Request failed at " + view);
            });
            req.write(data);
            req.end();

        }
    }
}
function checkViews() {
    views = process.env.VIEW.split(',');
    numViews = views.length;
}
function pointwiseMaximum(localVectorClock, incomingVectorClock) {
    var newVectorClock = {};
    //TODO? Assuming incomingVectorClock always has more keys
    console.log('local', localVectorClock);
    console.log('incoming', incomingVectorClock);
    
    for(var key in incomingVectorClock) {
        if(!localVectorClock.hasOwnProperty(key)){
            newVectorClock[key] = incomingVectorClock[key];
        } else {
            newVectorClock[key] = [];
            for(var index = 0; index < incomingVectorClock[key].length; index++) {
                newVectorClock[key].push(Math.max(localVectorClock[key][index], incomingVectorClock[key][index]));
            }
        }
    }
    console.log(newVectorClock)
    return newVectorClock;
}

// http request shell fcn, same as in shardRouter
function Req(view, method, path, dat) {
    // return new Promise(function(resolve, reject) {
        if (view != process.env.SOCKET_ADDRESS) {
            const params = view.split(':');
            const data = JSON.stringify(dat);
            const options = {
                protocol: 'http:',
                host: params[0],
                port: params[1],
                path: path,
                method: method,
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': data.length
                }
            };
            const req = http.request(options, function(res) {
                console.log(res.statusCode);
                let body = '';
                res.on('data', function (chunk) {
                    body += chunk;
                });
                res.on('end', function() {
                    console.log(body);
                    // resolve();
                })
            });
            req.on('error', function(err) {
                console.log("Error: Could not connect to replica at " + view);
                // reject();
            });
            req.write(data);
            req.end();
        }   
    // })
}