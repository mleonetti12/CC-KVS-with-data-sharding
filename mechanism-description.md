# Causal Dependency Tracking

The Causal-metadata was in the form of a collection of vector clocks for each key in the store.

This was comprised of a JavaScript object, where the key was the attribute name, and its value was an array to represent the vector clock.

Each replica has its own local vector clock to track the events that were done (e.g. a PUT) and would update/increment the index in the array based on the replica's tokenized ip address, subtracted by an offset for the index.

For example, if a PUT(x, 1) request was made to replica 2, then the causal-metadata that would be sent back would be of the form { "x" : [0, 1, 0] } indicating that an event has occured in the 2nd replica.

This sort of "rule" for the causal-metadata, that is sent to a replica on some request, will allow the replicas to compare this causal-metadata with its local vector clock to see if it is outdated/causally dependent on some previous request. In which case, it will wait for the broadcast.

To compare the local vector clock (indicating what operations have been done on the store) there is a function that will check each key's vector clock to see that if its outdated, then the operation wasn't done, hence we need to wait the current request that is causally dependent on some request prior, to do this.

The Causal Dependency Mechanism is implemented by using the Vector Clock Algorithm, specifically having each Key it's own Vector Clock. The reason for doing that is because each key is causally independent from each other, so each key must have its own Vector Clock to maintain this independence. This mechanism is implemented in the file storeRouter.js 

The Vector Clocks are all stored in vectorClock = {} java's object data structure with each key as key, and the key's value as an array with length of the number of replicas. Therefore if we have two keys and 3 replicas the vectorClock could look like:
vectorClock = {'x':[0,0,0],'y':[0,0,0]} 

For GET requests, there isn't any causal metadata being passed into the GET request. If the key that the request is asking for isn't present, then it will simply return "key doesn't exist". No update to the replica's vector clock and no broadcasting is necessary since it's not a 'write' action, so there's no change in causal consistency. 

Both DELETE and PUT functions require helper functions such as causalBroadcast(), deleteCausalBroadcast(), pointWiseMaximum() and compareVectorClocks()
to maintain their causal consistency/broadcasting.


# Detecting when a node is down

For the replica failure detection mechanism, we utilized a gossip type approach. 

This was implemented in the index.js file, and takes the form of the function startGossip().

This function runs the function gossipEngine() at the interval of once every second.

gossipEngine() first retrieves a shallow copy of its replicas view store, and trims its own socket address from the array.

It then takes a random view from the updated view store, and sends a GET request to the replica's key-value-store endpoint at that view.

If the replica responds within the timeout interval of 2 seconds, it continues looping and sending random GETs.

If the replica does not respond withing the 2 second interval, it is assumed to be down and:

 - deletes that replica from its own view store
 - broadcasts a DELETE request to all other known views to delete the replica from their views
 - waits for the DELETE requests to be cleared then continues looping

This function is ran after the server begins listening for requests via a promise chain after the app.listen() function

We chose to use a gossip based method as it seems easy to wrap our heads around and implement.

The interval of once per second was chosen as we wanted to respond to downed replicas quickly, while also not overwhelming our replicas with requests and possibly interfering with other requests being sent to the replica simultaneously.

Timeouts were chosen as the detection mechanism over error responses, as error responses are handled late by nodejs and would take too long to detect downed views.

The timeout of 2 seconds was chosen as we didn't want to delay the response to the request for too long, and it worked as well as longer timeouts lengths during testing.

A clear possible false positive case is where a replica is up, but fails to respond within the timeout interval for whatever reason (flooded with reqs, backend issues, etc), which would lead to it being assumed to be down and getting deleted from the view.


# Sharding Keys across Nodes
