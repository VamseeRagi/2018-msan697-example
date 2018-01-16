//Create Directory sharding_sets and under it, create sh_1, sh_2, sh_3, config_1

//3 Shards and 1 Config
//on Tab 1
mongod --dbpath ~/Class/2017_MSAN697/Spark_Example/Day6/mongodb/sharding_sets/sh_1 --port 20006 --shardsvr
//on Tab 2
mongod --dbpath ~/Class/2017_MSAN697/Spark_Example/Day6/mongodb/sharding_sets/sh_2 --port 20007 --shardsvr
//on Tab 3
mongod --dbpath ~/Class/2017_MSAN697/Spark_Example/Day6/mongodb/sharding_sets/sh_3 --port 20008 --shardsvr
//On Tab 4
mongod --dbpath ~/Class/2017_MSAN697/Spark_Example/Day6/mongodb/sharding_sets/config_1 --port 20009 --configsvr

//On Tab 5 : Start mongo shell(should be the config server address)
mongos --configdb localhost:20009 --chunkSize 1

mongo

use admin
//.addShard : Adds a database instance or replica set to a sharded cluster. 
sh.addShard("localhost:20006")
sh.addShard("localhost:20007")
sh.addShard("localhost:20008")
sh.status()

sh.enableSharding("mydb")
sh.shardCollection("mydb.friend",{"name":"hashed"})

use mydb
diane = {"name":"Diane MK Woodbridge", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}
yannet = {"name":"Yannet Interian", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}
david_guy =  {"name":"David Guy Brizan", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}
david  =  {"name":"David Uminsky", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}
db.friend.insert([diane, yannet, david_guy, david])

sh.status()

db.friend.getShardDistribution()


//Example 1
//All are same as above.

sh.enableSharding("msan697")
sh.shardCollection("msan697.business",{"name":"hashed"})

//on the different terminal,
// mongoimport --collection business --db msan697 --file=business.json
db.printShardingStatus(true); // This is same as sh.status() but shows all when sh.status cannot print because there are too many chunks to print.

