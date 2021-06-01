const express = require('express');
const shardRouter = express.Router();
shardRouter.use(express.json());
const viewRouter = require('./viewRouter');
const storeRouter = require('./storeRouter');

const socketAddress = process.env.SOCKET_ADDRESS;
let shardCount = process.env.SHARD_COUNT;
let shards = {};
let shardKeyNums = {'testID':15};
let thisShard = '-1';

shardNodes(viewRouter.getView(), shardCount);

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
    // temporary until implement kvs
    let keyNum = 0;
    // have a getter function/global var that gets/holds the num keys in each shard
    if (!keyNum) {
        keyNum = 0;
    }
    res.status(200).json({"message":"Key count of shard ID retrieved successfully", "shard-id-key-count": keyNum});
});

shardRouter.route('/add-member/:shardId')
.put(async (req, res, next) => {
    const id = req.params.shardId;
    let members = shards[id];
    let node = req.body["socket-address"];
    if (!members) {
        res.status(404).json({"error": "Shard ID does not exist", "message": "error in PUT"});
    } else {
        if (!node) {
            res.status(400).json({"error": "No node specified", "message": "error in PUT"});
        } else if (!members.includes(node)) {
            members.push(node);
            // send req to this node to update kvs, also set its thisShard
            Req(node, 'PUT', '/key-value-store-shard/update/' + id);
            // then broadcast this PUT to other nodes
            for (var view of viewRouter.getView()) {
                Req(view, 'PUT', '/key-value-store-shard/add-member/' + id, {"socket-address":node});
            }
            res.status(200).send();
        }   
    }    
});

shardRouter.route('/update/:shardId')
.put(async (req, res, next) => {
    const id = req.params.shardId;
    thisShard = id.toString();
    let members = shards[id];
    // let body = await Get(members[0], 'ROUTE TO GET ENTIRE KVS')
    // parse body here and set this KVS to the one parsed from body
    res.status(200).send();
});



shardRouter.route('/reshard')
.put(async (req, res, next) => {
    let shardCount = req.body["shard-count"];
    if (viewRouter.getView() < shardCount * 2) {
        res.status(400).json({"message": "Not enough nodes to provide fault-tolerance with the given shard count!"});
    } else {
        shardNodes(viewRouter.getView(), shardCount);
        res.status(200).json({"message": "Resharding done successfully"});
    }
});

// only sets which nodes correspond to which shard, does not allocate data
// split nodes into even sized paritions of length floor(nodes.length / shardCount)
// first parition = (shardID = 1), ..., etc -> excess added to shard 1
function shardNodes(nodes, shardCount) {
    nodes.sort();
    let prevInd = 0;
    let newShards = {};
    // partition nodes into shardCount chunks of size floor(nodes.length / shardCount)
    // first parition = (shardID = 1), ...
    for (let i = 1; i <= shardCount; i++) {
        let nodeArr = [];
        for (let j = prevInd; j < prevInd + Math.floor(nodes.length / shardCount); j++) {
            nodeArr.push(nodes[j]);
            if (nodes[j] == socketAddress) {
                thisShard = i.toString();
            }
        }
        newShards[i] = nodeArr;
        prevInd = prevInd + Math.floor(nodes.length / shardCount);
    }
    // add remaining nodes to the first shard
    for (let i = prevInd; i < nodes.length; i++) {
        newShards[1].push(nodes[i]);
    }
    shards = newShards;
    // call function to reshard this nodes data somewhere in here
    // need to send excess data to other nodes
}

function getShards() {
    return shards;
}

function Req(view, method, path, data) {
    return new Promise(function(resolve, reject) {
        if (view != process.env.SOCKET_ADDRESS) {
            const params = view.split(':');
            const data = JSON.stringify(data);
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

function Get(view, path) {
    return new Promise(function(resolve, reject) {
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
                })
            });
            req.on('error', function(err) {
                console.log("Error: Could not connect to replica at " + view);
                reject();
            });
            req.end();
        }   
    })
}

module.exports = {
    router:shardRouter,
    getShards:getShards
};