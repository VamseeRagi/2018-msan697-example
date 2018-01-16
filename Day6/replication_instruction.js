//start all 3 mongod processes with --dbpath, --port and --replSet
mongod --port 20003 --dbpath rs_1 --replSet test
mongod --port 20004 --dbpath rs_2 --replSet test
mongod --port 20005 --dbpath rs_3 --replSet test

//Start the mongo shell with one of the node.
mongo --host localhost --port 20003

//configuration
config_test={_id: "test" ,  members:[{_id:0,host:"localhost:20003"},{_id:1,host:"localhost:20004"},{_id:2,host:"localhost:20005"}]}
//initiate replica set (rs : a global variable for a replication helper function.
rs.initiate(config_test) 

//check replication status
rs.status()

//Insert into primary.
conn1 = new Mongo("localhost:20003")
primaryDB = conn1.getDB("test")
primaryDB.friend.insert({"name":"Diane MK Woodbridge"})

//Secondary DB
conn2 = new Mongo("localhost:20004")
secondaryDB = conn2.getDB("test")
secondaryDB.friend.find().pretty() //output --> "errmsg" : "not master and slaveOk=false"
conn2.setSlaveOk()
secondaryDB.friend.find().pretty() //output --> shows data inserted into the primaryDB
secondaryDB.friend.insert({"name":"Yannet Interian"}) //output -->  "errmsg" : "not master"

//Automatic failover
primaryDB.adminCommand({"shutdown":1}) //.adminCommand() : Provides a helper to run specified database commands against the admin database.
rs.status() //You would see a secondary becomes a primary
