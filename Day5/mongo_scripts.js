///////////////////////////////////////////////
//To run this script mongo < mongo_scripts.js//
///////////////////////////////////////////////

//Choose db
use mydb
db

diane = {"name":"Diane MK Woodbridge", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}
yannet = {"name":"Yannet Interian", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}
david_guy =  {"name":"David Guy Brizan", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}
david  =  {"name":"David Uminsky", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}

//Insert
db.friend.insert(diane)
db.friend.insert(yannet)

//Bulk Insert
db.friend.insert([david_guy, david])

//Import raw data - do it on the terminal (not on Mongo shell
//mongoimport --db msan697 --collection business --file ../Data/business.json

//Find
db.friend.find()
db.friend.findOne()
db.friend.find({"name":"Diane MK Woodbridge"})

//Initialize to simplify the example.
db.friend.drop()
db.friend.insert(diane)
db.friend.find()

//Update
diane.noKids = 2 //Update the document
diane

//replace the document
db.friend.update({"name":"Diane MK Woodbridge"}, diane)
db.friend.find()

//set the new field using $set
db.friend.update({"name":"Diane MK Woodbridge"},{$set:{"noCats":1}})
db.friend.find()

//unset the field
db.friend.update({"name":"Diane MK Woodbridge"},{$unset:{"noCats":""}})
db.friend.find()

//increase a value of the field.
db.friend.update({"name":"Diane MK Woodbridge"},{$inc : {"noCats":2}})
db.friend.find()

//rename a field.
db.friend.update({"name":"Diane MK Woodbridge"},{$rename : {"noCats":"numCats"}})
db.friend.find()

//$min : updates the value of the field to a specified value if the specified value is less than the current value of the field. 
db.friend.update({"name":"Diane MK Woodbridge"},{$min : {"numCats":1}})
db.friend.find()

//Update an array
//Add to an array
db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {"name":"Abigail", "content" : "Please cook mac and cheese"}}})
db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {"name":"Jackson", "content" : "Please cook mac and cheese"}}})
db.friend.find()

//Remove the last item
db.friend.update({"name":"Diane MK Woodbridge"},{$pop : {"comments": 1}})
db.friend.findOne()

db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {"name":"Jackson", "content" : "Please cook mac and cheese"}}})
db.friend.findOne()

//Remove the first item
db.friend.update({"name":"Diane MK Woodbridge"},{$pop : {"comments": -1}})
db.friend.findOne()


db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {"name":"Jackson", "content" : "Please cook mac and cheese"}}})
db.friend.findOne()

//Removes from an existing array all instances of a value or values that match a specified condition.
db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name":"Jackson"}}})
db.friend.findOne()

db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {"name":"Abigail", "content" : "Please cook mac and cheese"}}})
db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {"name":"Jackson", "content" : "Please cook mac and cheese"}}})
db.friend.findOne()

//Use positional operator for updates
//$ : update the first match
db.friend.update({"comments.content":"Please cook mac and cheese"},{$set : {"comments.$.content": "Please cook mac and cheeeeese"}})
db.friend.findOne()


//$each : Modify $push to append multiple items for array updates.
comment_1 = {"name":"Bora", "content" : "I need more water"}
comment_2 = {"name":"Jonathan", "content" : "Come home early"}
//db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": [comment_1, comment_2]}}) // Result in comments{comment, comment, [comment_1, commeent_2]}
db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {$each:[comment_1, comment_2]}}})  // Result in comments{comment, comment, comment_1, commeent_2}
db.friend.findOne()

db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Jonathan"}}})
db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Bora"}}})
db.friend.findOne()

//$position : provide the array index for an update
db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {$each:[comment_1, comment_2], $position : 1}}})
db.friend.findOne()

db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Jonathan"}}})
db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Bora"}}})

//$slice : limits the number of array elements 
db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {$each:[comment_1, comment_2], $slice : 2}}})
db.friend.findOne()

db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Jonathan"}}})
db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Bora"}}})

db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {$each:[comment_1, comment_2],  $slice : 3}}})
db.friend.findOne()

db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Jonathan"}}})
db.friend.update({"name":"Diane MK Woodbridge"},{$pull : {"comments": {"name": "Bora"}}})

//$sort : orders the elements of an array during a $push operation. (1 - ascending, -1 - descending)
db.friend.update({"name":"Diane MK Woodbridge"},{$push : {"comments": {$each:[comment_1, comment_2], $position : 1, $slice : 4, $sort: {"name":1}}}})
db.friend.findOne()

diane = {"name":"Diane MK Woodbridge", "address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}, "noKids":2, "numCats":1}
db.friend.insert(diane) // Now there are two "Diane MK Woodbridge"
db.friend.find()

//Update multiple
db.friend.update({"name":"Diane MK Woodbridge"},{$set:{"numCats":2}},{multi:true})
db.friend.find()


//Upsert
db.friend.update({"name":"Yannet Interian"},{$set: {"address" : {"street" : "101 Howard", "city" : "San Francisco", "state" : "CA"}}}, {upsert:true})
db.friend.find()

//Remove
db.friend.remove({"name":"Yannet Interian"})
db.friend.find()

//Drop the entire collection.
db.friend.drop()
db.friend.count()
