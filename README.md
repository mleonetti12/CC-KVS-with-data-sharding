# CSE138_Assignment4


# Acknowledgements:


# Citations:

https://www.npmjs.com/package/hashring

Learned: How to parse key values for a consistent hashing ring

Applied: Used to parse key values and determine positioning of nodes around the ring

https://www.toptal.com/big-data/consistent-hashing

Learned: How adding/removing nodes from a consistent hashing ring affects overall key hashing

Applied: Used to implement main resharding algorithm

# Team Contributions:

Maxwell Leonetti
- shardRouter.js
  - Entirety
  - All shard operations
- storeRouter.js
  - Resharding operation
  - setHashRing, /sync-kvs, /reshard, /put-keys, /update-hash

Ming Jeng
- storeRouter.js
- the GET Operation
- the DELETE Operation

Ronald Chhua
- storeRouter.js
 - PUT & GET request
 - Request Forwarding to Shard
 - Hashring, causal broadcast