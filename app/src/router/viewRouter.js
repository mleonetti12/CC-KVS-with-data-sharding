const express = require('express');
const viewRouter = express.Router();
viewRouter.use(express.json());

let viewStore = process.env.VIEW.split(',');
const socketAddress = process.env.SOCKET_ADDRESS;

viewRouter.route('/')
.get(async (req, res, next) => {
    res.status(200).json({"message":"View retrieved successfully", "view": viewStore});
})
.delete(async (req, res, next) => {
    let delInd = viewStore.indexOf(req.body['socket-address']);
    if (delInd > -1) {
        viewStore.splice(delInd, 1);
        res.status(200).json({"message":"Replica deleted successfully from the view"});
    } else {
        res.status(404).json({"error":"Socket address does not exist in the view", "message":"Error in DELETE"});
    }
})
.put(async (req, res, next) => {
    const sockAddress = req.body["socket-address"];
    const putAdd = viewStore.find(x => x == sockAddress);
    if (putAdd) {
        res.status(404).json({"error":"Socket address already exists in the view", "message":"Error in PUT"});
    } else {
        viewStore.push(sockAddress);
        res.status(201).json({"message":"Replica added successfully to the view"});
    }
});

// retrieve viewStore for use in gossiping
function getView() {
    return viewStore;
}

// delete view from viewStore
function deleteView(view) {
    // console.log("I found it");
    const delInd = viewStore.indexOf(view);
    if (delInd > -1) {
        viewStore.splice(delInd, 1);
        return true;
    } else {
        return false;
    }
}

module.exports = {
    router:viewRouter,
    getView:getView,
    deleteView:deleteView
};