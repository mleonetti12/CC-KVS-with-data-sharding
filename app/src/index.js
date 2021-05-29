const express = require('express');
const app = express();

app.use(express.json());
const http = require('http');

const storeRouter = require('./router/storeRouter');
const viewRouter = require('./router/viewRouter');
const shardRouter = require('./router/shardRouter');
app.use('/key-value-store', storeRouter.router);
app.use('/key-value-store-view', viewRouter.router);
app.use('/key-value-store-shard', shardRouter.router);

const splitAdd = process.env.SOCKET_ADDRESS.split(':'); // get seperated socket and ip
const views = process.env.VIEW.split(',');				// get views as array


// broadcast PUT to all views, wait for completion
// then get the KVS from one view and update current KVS
// then have server start listening for requests
// then start gossip (checking in on random views for downtime)	
let promises = [];
for (view of views) {
	promises.push(broadcastReq(view, process.env.SOCKET_ADDRESS, 'PUT'));
}
Promise.allSettled(promises)
	.then(storeRouter.getKVS(views))
	.then(app.listen(8085, splitAdd[0], function(){
		startGossip();
	}))
	.then(setTimeout(startGossip, 0))
	.catch((error) =>{
		console.log(error);
	});	

// previous version of app.listen(), kept for testing

// app.listen(8085, splitAdd[0], function() {
	// broadcast PUT to all views, wait for completion
	// then get the KVS from one view and update current KVS
	// then have server start listening for requests
	// then start gossip (checking in on random views for downtime)	
// 	let promises = [];
// 	for (view of views) {
// 		promises.push(broadcastReq(view, process.env.SOCKET_ADDRESS, 'PUT'));
// 	}
// 	Promise.allSettled(promises)
// 		.then(getKVS(views))
// 		.then(setTimeout(startGossip, 0))
// 		.catch((error) =>{
// 			console.log(error);
// 		});	
// });

// start listening for down views
async function startGossip() {
	let interval = 1000; // send req every second
	const gossipEngine = setInterval(function(){
		// get list of views omitting itself
		let currentView = viewRouter.getView();
		let tempView = [...currentView]; // create shallow copy to avoid overwriting
		let thisIndex = tempView.indexOf(process.env.SOCKET_ADDRESS);
		if (thisIndex > -1) {
			tempView.splice(thisIndex, 1);
		}
		// get random view to check in on
		let randomView = tempView[Math.floor(Math.random() * tempView.length)];
		// send GET req to check in
		if (randomView) {
			const params = randomView.split(':');
			const options = {
				protocol: 'http:',
				host: params[0],
				port: params[1],
				path: '/key-value-store-view',
				timeout: 2000, // timeout set to 2s
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
					console.log(randomView + " " + body);
				})
			});
			// on timeout assume down
			req.on('timeout', () => {
				console.log("deleting: " + randomView);
				viewRouter.deleteView(randomView); //delete down view from viewStore
				deleteBroadcast(tempView, randomView); //broadcast DELETE
				clearInterval(gossipEngine); // stop gossiping temporarily
    			req.abort();
			});
			req.on('error', function(err) {
			});
			req.end();

		}	
	}, interval);
}

// shell method for broadcasting DELETES
function deleteBroadcast(viewSet, targetView) {
	// wait for all views to delete downed view
	let delPromises = [];
	for (view of viewSet) {
		if (view != targetView){
			delPromises.push(broadcastReq(view, targetView, 'DELETE'));
		}	
	}
	Promise.allSettled(delPromises)
		.then(startGossip) // start gossiping again
		.catch((error) =>{
			console.log(error);
		});	
}

// function for broadcasting HTTP reqs to all views in 'view'
function broadcastReq(view, add, method) {
	return new Promise(function(resolve, reject) {
		if (view != process.env.SOCKET_ADDRESS) {
			// console.log(view);
			const params = view.split(':');
			const data = JSON.stringify({'socket-address' : add});
			const options = {
				protocol: 'http:',
				host: params[0],
				port: params[1],
				path: '/key-value-store-view',
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

// // get kvs from another replica and merge it with current kvs
// function getKVS(views) {
// 	return new Promise(function(resolve, reject) {
// 		for (var view of views) {
// 			let replicadownFlag = false;
// 			if (view != process.env.SOCKET_ADDRESS) {
// 				const params = view.split(':');
// 				const options = {
// 					protocol: 'http:',
// 					host: params[0],
// 					port: params[1],
// 					path: '/key-value-store/sync-kvs', // view only route
// 					method: 'GET',
// 	    			headers: {
// 	  				}
// 				};
// 				const req = http.request(options, function(res) {
// 					let body = '';
// 					res.on('data', function (chunk) {
// 						body += chunk;
// 					});
// 					res.on('end', function() {
// 						console.log(body);
// 						storeRouter.setKVS(JSON.parse(body).kvs); // add kvs to current vks
// 						storeRouter.setCM(JSON.parse(body).cm); // add cm to current cm
// 						resolve();
// 					})
// 				});
// 				req.on('error', function(err) {
// 					console.log("Error: Could not connect to replica at " + view);
// 					replicadownFlag = true; // could not retrieve KVS
// 				});
// 				req.end();
// 				// if KVS successfully retrieved, done
// 				// else try with next view in 'views'
// 				if (!replicadownFlag) {
// 					break;
// 				}
// 			}	
// 		}
// 		resolve();
// 	})
// }