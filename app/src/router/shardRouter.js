const express = require('express');
const shardRouter = express.Router();
shardRouter.use(express.json());
const viewRouter = require('./viewRouter');

const socketAddress = process.env.SOCKET_ADDRESS;
let shardCount = process.env.SHARD_COUNT;
let shards = {};
let shardKeyNums = {'testID':15};
let thisShard = -1;

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
        } else {
            members.push(node);
            // send req to this node to update kvs, also set its thisShard
            // then broadcast this PUT to other nodes
            res.status(200).send();
        }   
    }    
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
                thisShard = i;
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
    // call function to reshard data somewhere in here
}

module.exports = {
    router:shardRouter
};