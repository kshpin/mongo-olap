const {MongoClient, Timestamp} = require("mongodb");
const OLAP = require("../src/OLAP");

async function connectDb(url, dbName) {
	let mongoClient = await MongoClient.connect(url);
	let db = mongoClient.db(dbName);
	return {mongoClient, db};
}

(async function() {
	let url = "mongodb://localhost:27017/";
	let dbName = "db1";

	let {mongoClient, db} = await connectDb(url, dbName);

	let response = await db.collection("c_test").find({
		_id: "throughNode"/*,
		zeroOne: new Timestamp(0, 1),
		oneZero: new Timestamp(1, 0)*/
	}).next();

	console.log(response);
})();

// Timestamp(low, high) // high bits determine time, low are tie-breakers
