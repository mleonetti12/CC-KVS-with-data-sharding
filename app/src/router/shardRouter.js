const express = require('express');
const shardRouter = express.Router();
shardRouter.use(express.json());
const viewRouter = require('./viewRouter');

const http = require('http');

const socketAddress = process.env.SOCKET_ADDRESS;
let shardCount = process.env.SHARD_COUNT;
let shards = {};
let shardKeyNums = {'testID':15};
let thisShard = '-1';

if (shardCount) {
   shardNodes(viewRouter.getView(), shardCount); 
}

function getShards() {
    return shards;
}

function getThisShard() {
    return thisShard;
}

module.exports = {
    router:shardRouter,
    getShards:getShards,
    getThisShard:getThisShard
};

const storeRouter = require('./storeRouter');

shardRouter.route('/shard-ids') // should be good
.get(async (req, res, next) => {
    let shardIDS = [];
    for (id in shards) {
        shardIDS.push(id);
    }
    res.status(200).json({"message":"Shard IDs retrieved successfully", "shard-ids": shardIDS});
});

shardRouter.route('/node-shard-id') // should be good
.get(async (req, res, next) => {
    res.status(200).json({"message":"Shard ID of the node retrieved successfully", "shard-id": thisShard});
});

shardRouter.route('/shard-id-members/:shardId') // should be good
.get(async (req, res, next) => {
    const id = req.params.shardId;
    let members = shards[id];
    if (!members) {
        members = [];
    }
    res.status(200).json({"message":"Members of shard ID retrieved successfully", "shard-id-members": members});
});

shardRouter.route('/shard-id-key-count/:shardId')
.get(async (req, res, next) => {
    const id = req.params.shardId;
    let keyNum = -1;
    if (id == thisShard) {
        keyNum = storeRouter.getLength();
        // keyNum = 10;
    } else {
        let body = await Get(shards[id], '/key-value-store/sync-kvs');
        // console.log(body);
        keyNum = Object.keys(JSON.parse(body).kvs).length;
    }
    res.status(200).json({"message":"Key count of shard ID retrieved successfully", "shard-id-key-count": keyNum});
});

shardRouter.route('/add-member/:shardId')
.put(async (req, res, next) => {
    const id = req.params.shardId;
    // let members = shards[id];
    // console.log(members);
    let node = req.body["socket-address"];
    // console.log(node);
    console.log('this is the id in add-member: ' + shards[id]);
    if (!shards[id]) {
        res.status(404).json({"error": "Shard ID does not exist", "message": "error in PUT"});
    } else {
        if (!node) {
            res.status(400).json({"error": "No node specified", "message": "error in PUT"});
        } else if (!shards[id].includes(node)) {
            shards[id].push(node);
            // send req to this node to update kvs, also set its thisShard
            Req(node, 'PUT', '/key-value-store-shard/update/' + id, {"ss": shards});
            // then broadcast this PUT to other nodes
            for (var view of viewRouter.getView()) {
                
                Req(view, 'PUT', '/key-value-store-shard/add-member-hidden/' + id, {"socket-address":node});
            }
            res.status(200).send();
        } else {
            res.status(400).json({"error": "Node already exists in shard", "message": "error in PUT"});
        } 
    }    
});

shardRouter.route('/add-member-hidden/:shardId')
.put(async (req, res, next) => {
    const id = req.params.shardId;
    // let members = shards[id];
    // console.log(members);
    let node = req.body["socket-address"];
    // console.log(node);
    if (!shards[id]) {
        res.status(404).json({"error": "Shard ID does not exist", "message": "error in PUT"});
    } else {
        if (!node) {
            res.status(400).json({"error": "No node specified", "message": "error in PUT"});
        } else if (!shards[id].includes(node)) {
            shards[id].push(node);
            res.status(200).send();
        } else {
            res.status(400).json({"error": "Node already exists in shard", "message": "error in PUT"});
        } 
    }    
});

// update the shard of this node
// for use with add-member
shardRouter.route('/update/:shardId')
.put(async (req, res, next) => {
    const id = req.params.shardId;
    thisShard = id;
    shards = req.body["ss"];
    // console.log(shards);
    console.log("id in update" + id);
    await storeRouter.getKVS(shards[id], true);
    console.log("shard[id] in update" + shards[id]);
    res.status(200).send(); 
    
});

// used to check if this node is holding the correct data
// if not retrieved the kvs from another node in its shard
// this is ran after kvs's have been resharded
shardRouter.route('/check-update')
.put(async (req, res, next) => {
    shardNodes(viewRouter.getView(), req.body["shard-count"]); // assign nodes to shards
    const KVS = storeRouter.retrieveKVS();
    for (key in KVS) {
        if (storeRouter.hashKey(key) != thisShard) {
            await storeRouter.getKVS(shards[thisShard], true);
            break;
        }
    }
    res.status(200).send();    
});

// 
// shardRouter.route('/sync-shard')
// .get(async (req, res) => {
//     res.status(200).json({"message": "Retrieved successfully", "ss": shards});
// });

// main reshard route called by admin
shardRouter.route('/reshard')
.put(async (req, res, next) => {
    let shardCount1 = req.body["shard-count"];
    // not enough nodes to reshard with this value
    if (viewRouter.getView().length < shardCount1 * 2) {
        res.status(400).json({"message": "Not enough nodes to provide fault-tolerance with the given shard count!"});
    } else {
        let prevShardCount = shardCount1;
        let promises = [];
        storeRouter.setHashRing(shardCount1); // update hashring
        for (view of viewRouter.getView()) {
            // wait for every node to update its hashring before continuing
            promises.push(promiseReq(view, 'PUT', '/key-value-store/update-hash', {"shard-count":shardCount1}));
        }
        Promise.allSettled(promises)
        .then(reshard(prevShardCount, shardCount1)) // then reshard kvs
        .then(shardNodes(viewRouter.getView(), shardCount1)) // then assign nodes to new shards
        .catch((error) =>{
            console.log(error);
        }); 

        // this node checks if it storing correct data
        const KVS = storeRouter.retrieveKVS();
        for (key in KVS) {
            if (storeRouter.hashKey(key) != thisShard) {
                await storeRouter.getKVS(shards[thisShard], true);
                break;
            }
        }
        // all nodes check if they are storing correct data
        for (view of viewRouter.getView()) {
            Req(view, 'PUT', '/key-value-store-shard/check-update', {"shard-count":shardCount1});
        }
        res.status(200).json({"message": "Resharding done successfully"});
    }
});


// backend reshard function
function reshard(prevShardCount, shardCount1) {
    // calculate shards that need to be reviewed
    const diff = shardCount1 - prevShardCount;
    // if no change in #, do nothing
    if (diff == 0) {
        return;
    } else if (diff > 0) { // if shards added
        // run addition protocol
        // loop through all nodes of shards that need reviewing
        for (var i = prevShardCount; i < shardCount1; i++) {
            for (node of shards[i.toString()]) {
                // reshard the kvs of these nodes
                Req(view, 'PUT', '/key-value-store/reshard', {"shard-count":shardCount1});
            }
        }
    } else {
        // run deletion protocol
        for (var i = prevShardCount; i > shardCount1; i--) {
            for (node of shards[i.toString()]) {
                Req(view, 'PUT', '/key-value-store/reshard', {"shard-count":shardCount1});
            }
        }
    }
    
}

// only sets which nodes correspond to which shard, does not allocate data
// split nodes into even sized paritions of length floor(nodes.length / shardCount)
// first parition = (shardID = 1), ..., etc -> excess added to shard 1
function shardNodes(nodes, shardCount1) {
    nodes.sort();
    let prevInd = 0;
    let newShards = {};
    // partition nodes into shardCount chunks of size floor(nodes.length / shardCount)
    // first parition = (shardID = 1), ...
    for (let i = 1; i <= shardCount1; i++) {
        let nodeArr = [];
        for (let j = prevInd; j < prevInd + Math.floor(nodes.length / shardCount1); j++) {
            nodeArr.push(nodes[j]);
            if (nodes[j] == socketAddress) {
                thisShard = i.toString();
            }
        }
        newShards[i] = nodeArr;
        prevInd = prevInd + Math.floor(nodes.length / shardCount1);
    }
    // add remaining nodes to the first shard
    for (let i = prevInd; i < nodes.length; i++) {
        newShards[1].push(nodes[i]);
    }
    shards = newShards;
    // call function to reshard this nodes data somewhere in here
    // need to send excess data to other nodes
    // maybe put reshard here
}

// basic http request shell with no promise
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
                    // console.log(body);
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

// same function as above but with promise
// for use in waiting for req to finish
function promiseReq(view, method, path, dat) {
    return new Promise(function(resolve, reject) {
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
                    // console.log(body);
                    resolve();
                })
            });
            req.on('error', function(err) {
                console.log("Error: Could not connect to replica at " + view);
                reject();
            });
            req.write(data);
            req.end();
        }   
    })
}

// basic get shell with promise
function Get(views, path) {
    return new Promise(function(resolve, reject) {
        for (var view of views) {
            let replicadownFlag = false;
            if (view != process.env.SOCKET_ADDRESS) {
                const params = view.split(':');
                const options = {
                    protocol: 'http:',
                    host: params[0],
                    port: params[1],
                    path: path,
                    method: 'GET',
                    headers: {
                    }
                };
                const req = http.request(options, function(res) {
                    console.log(res.statusCode);
                    let body = '';
                    res.on('data', function (chunk) {
                        body += chunk;
                    });
                    res.on('end', function() {
                        resolve(body);
                        // return body;
                    })
                });
                req.on('error', function(err) {
                    console.log("Error: Could not connect to replica at " + view);
                    replicadownFlag = true;
                });
                req.end();
                if (!replicadownFlag) {
                    break;
                }
            }  
        }
    })
}

