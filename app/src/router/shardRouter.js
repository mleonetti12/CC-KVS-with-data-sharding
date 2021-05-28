const express = require('express');
const shardRouter = express.Router();
shardRouter.use(express.json());
const viewRouter = require('./router/viewRouter');

const socketAddress = process.env.SOCKET_ADDRESS;
let shardCount = process.env.SHARD_COUNT;
let shardIDS = [1, 2, 3];
let shards = {'testID':[socketAddress]};
let shardKeyNums = {'testID':15};
let thisShard = 1;

shardRouter.route('/shard-ids')
.get(async (req, res, next) => {
    res.status(200).json({"message":"Shard IDs retrieved successfully", "shard-ids": shardIDS});
});

shardRouter.route('/node-shard-id')
.get(async (req, res, next) => {
    res.status(200).json({"message":"Shard ID of the node retrieved successfully", "shard-id": thisShard});
});

shardRouter.route('/shard-id-members/:shardId')
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
    let keyNum = shardKeyNums[id];
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
        // reshard here
        res.status(200).json({"message": "Resharding done successfully"});
    }
});

module.exports = {
    router:shardRouter
};