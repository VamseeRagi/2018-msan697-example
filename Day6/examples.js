//Example 1.
//Load data to world_bank_project in msan697.
//mongoimport --db msan697 --collection world_bank_project --file=world_bank_project_small.json

db.world_bank_project.find({},{"borrower":1,"_id":0}).pretty() // To format the result, you can add the .pretty() to the operation.
db.world_bank_project.find({},{"borrower":true,"_id":false}).pretty() // This is same as the above.

//Example 2.
db.world_bank_project.find({"sector1.Percent":{$gte:60}}).count()
db.world_bank_project.find({"sector1.Percent":{$gte:60}},{"borrower":1}).pretty()

//Example 3.
db.world_bank_project.find({$or:[{"theme1.Name":"Water resource management"}, {"themecode" : "65"}]},{"url":1, "_id":0})

//Example 4.
db.world_bank_project.find({"impagency":{$in:["MINISTRY OF EDUCATION","MINISTRY OF FINANCE"]}},{"borrower":1}).pretty()

//Example 5.
db.world_bank_project.find({"project_name":{$regex:"Project$"}},{"project_name":1})

//Example 6.
db.world_bank_project.find({"project_name":{$regex:"Project$"}},{"project_name":1}).pretty()
db.world_bank_project.find({"project_name":{$regex:"Project$"}},{"theme_namecode":{$slice:-1},"project_name":1}).pretty()

//Example 7.
db.world_bank_project.find({},{"mjtheme":1}).pretty()
db.world_bank_project.find({mjtheme: "Environment and natural resources management"}, {mjtheme:1}).pretty()

//Example 8.
db.world_bank_project.find({projectdocs:{$elemMatch:{DocType:"PID", DocDate:"12-AUG-2013"}}},{projectdocs:1}).pretty()

//Example 9.
db.world_bank_project.findOne({"themecode" : "65", "totalamt" : 75000000, "totalcommamt" : 75000000, "url" : "http://www.worldbank.org/projects/P122700/angola-learning-all-project?lang=en"})

//Example 10.
db.world_bank_project.aggregate({$match:{"totalcommamt" : 75000000}}).pretty()
db.world_bank_project.aggregate({$match:{"totalcommamt" : 75000000}},{$project:{"projectdocs":1}}).pretty()
db.world_bank_project.aggregate({$match:{"totalcommamt" : 75000000}},{$project:{"projectdocs":1}},{$unwind:"$projectdocs"}).pretty()
db.world_bank_project.aggregate({$match:{"totalcommamt" : 75000000}},{$project:{"projectdocs":1}},{$unwind:"$projectdocs"},{$group:{_id:"$projectdocs.DocType"}}).pretty()
