const MongoClient = require("mongodb").MongoClient;
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

	let olap = new OLAP(mongoClient, db, "olap_config"); // each OLAP is tied to a particular DB
	await olap.startOplogListening(null);

	/*await olap.loadCube({
		source: "db1.c2",
		dimensions: [
			{
				path: "type",
				id: "type"
			}
		],
		measures: [
			{
				path: "number",
				id: "number"
			}
		]
	});

	await olap.loadCube({
		source: "db1.c_test",
		dimensions: [
			{
				path: "c",
				id: "cField"
			},
			{
				path: "a",
				id: "aField"
			},
		],
		measures: [
			{
				path: "b",
				id: "bField"
			}
		]
	});*/

	await olap.loadCubes();

	/*await olap.createCube({
		source: "db1.c1",
		dimensions: [
			{
				path: "state",
				id: "stateID"
			},
			{
				path: "city",
				id: "cityID"
			},
		],
		measures: [
			{
				path: "pop",
				id: "population"
			}
		]
	});*/

	console.log(await olap.aggregate(
		"c1",
		[
			"population"
		],
		[
			{dimension: "stateID"}
		],
		{
			cityID: "SAN JOSE"
		}
	));
})();
