const express = require('express');
const storeRouter = express.Router();
storeRouter.use(express.json());
const http = require('http');

const keyvalueStore = {};
var vectorClock = {};
const OFFSET = 2;

//Every view that may be occupied by a replica.
var views = process.env.VIEW.split(',');  //10.0.0.2:8085, 10.0.0.3:8085, 10.0.0.4:8085
var numViews = views.length;

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
})

storeRouter.route('/:key')
.get(async (req, res) => {
    const val = keyvalueStore[req.params.key];
    if (!val){
        res.status(404).json({"error": "Key does not exist", "message": "Error in GET"});
    } else {
        //vectorClock[req.params.key][]+=1 
        res.status(200).json({"message": "Retrieved successfully", 
                             "causal-metadata":vectorClock,
                             "value": val});
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

        if(causalMetadata.length == 0) {
            keyvalueStore[key] = value;
            //vectorClock[key] = [0, 0, 0];
            vectorClock[key] = [];
            for(var i = 0; i < numViews; i++) {
                vectorClock[key].push(0);
            }
            vectorClock[key][VECTOR_CLOCK_INDEX] = 1;
            res.status(201).json({
                "message": "Added successfully",
                "causal-metadata": vectorClock
            });
        } else if(await compareVectorClocks(causalMetadata)) {
            if(keyvalueStore.hasOwnProperty(key)) {
                keyvalueStore[key] = value;
                if(req.body['broadcast']) {
                    vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
                } else {
                    vectorClock[key][VECTOR_CLOCK_INDEX] = vectorClock[key][VECTOR_CLOCK_INDEX]+1;
                }
                res.status(200).json({
                    "message": "Updated successfully",
                    "causal-metadata": vectorClock
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
                    "causal-metadata": vectorClock
                });
            }
        } else {
            //wait
            vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
            while(!await compareVectorClocks(causalMetadata)) {

            }
            console.log('in else')

            if(keyvalueStore.hasOwnProperty(key)) {
                keyvalueStore[key] = value;
                if(req.body['broadcast']) {
                    vectorClock = pointwiseMaximum(vectorClock, causalMetadata);
                } else {
                    vectorClock[key][VECTOR_CLOCK_INDEX] = vectorClock[key][VECTOR_CLOCK_INDEX]+1;
                }
                res.status(200).json({
                    "message": "Updated successfully",
                    "causal-metadata": vectorClock
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
                    "causal-metadata": vectorClock
                });
            }
        }
        if(!req.body['broadcast']) {
            causalBroadcast(CURRENT_REPLICA_HOST, key, value, vectorClock);
        }
    }
    
})
.delete(async (req,res) => {
    const REPLICA = process.env.SOCKET_ADDRESS;   // Get the REPLICA's address
    const VECTOR_CLOCK_INDEX = REPLICA.split('.')[3].split(':')[0] - OFFSET; 
    const CURRENT_REPLICA_HOST = REPLICA.split(':')[0];  
    const causalMetadata = req.body['causal-metadata']
    const key = req.params.key
    const val = keyvalueStore[key];

    //Delete key value store
    if (!val){
        res.status(404).json({"error": "Key does not exist", "message": "Error in DELETE"});
    } else {
        if(await compareVectorClocks(causalMetadata)){
            delete keyvalueStore[key];
            //creates causal metadata and increment 1 for current replica since it's a write operation
            deleteCausalBroadcast(CURRENT_REPLICA_HOST, key,causalMetadata)
            vectorClock[key][VECTOR_CLOCK_INDEX] = vectorClock[key][VECTOR_CLOCK_INDEX]+1;
            //broadcast to all other replicas
            res.status(200).json({"message":"Deleted successfully","causal-metadata":vectorClock});

        }else{
            res.status(404).json({"error": "Inconsistent causality", "message": "All causally preceding operations must be complete first before applying DELETE"});
        }
    }
})
.all(async(req,res,next) => {
    res.status(405).send();
});
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


async function causalBroadcast(CURRENT_REPLICA_HOST, key, value, causalMetadata) {
    for(view of views) {
        const REPLICA_HOST = view.split(':')[0];
        if(REPLICA_HOST != CURRENT_REPLICA_HOST) {
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
    console.log(localVectorClock);
    console.log(incomingVectorClock);
    
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

// // add newKVS to current KVS, only for inter-view use
// function setKVS(newKVS) {
//     for (var key in newKVS) {
//         keyvalueStore[key] = newKVS[key];
//     }
// }

// // add new causal metadata to current
// function setCM(newCM) {
//     for (var key in newCM) {
//         vectorClock[key] = newCM[key];
//     }
// }

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

// add new causal metadata to current
function setCM(newCM) {
    for (var key in newCM) {
        let updateFlag = true;
        if (vectorClock.hasOwnProperty(key)) {
           for(var index = 0; index < newCM[key].length; index++) {
                if (newCM[key][index] < vectorClock[key][index]) {
                    updateFlag = false;
                }
            }     
        }
        if (updateFlag) {
            vectorClock[key] = newCM[key];
        }   
    }
}

// need to export setKVS function for index.js use
// (storeRouter in index.js) -> storeRouter.router
module.exports = {
    router:storeRouter,
    setKVS:setKVS,
    setCM:setCM
};