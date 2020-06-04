const MongoClient = require("mongodb").MongoClient;
const OLAP = require("./OLAP");

const NATS = require("nats");
const nc = NATS.connect({
	url: "nats://localhost:4222",
//	user: "foo",
//	pass: "bar"
});

async function connectDb(url, dbName) {
	let mongoClient = await MongoClient.connect(url);
	let db = mongoClient.db(dbName);
	return {mongoClient, db};
}

function errToString(err) {
	return JSON.stringify({message: err.message, stackTrace: err.stack.toString()});
}

async function startService() {
	// let url = process.env.DB_URL;
	// let dbName = process.env.DB_NAME;
	let url = "mongodb://localhost:27017/";
	let dbName = "db1";

	let {mongoClient, db} = await connectDb(url, dbName);

	let olap = new OLAP(mongoClient, db, "olap_config");

	nc.subscribe("olap_createCube", async (msg, reply) => {
		msg = JSON.parse(msg);

		let failed = false;
		try {
			await olap.createCube(msg);
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}
		if (!failed && reply) nc.publish(reply, JSON.stringify({status: 0}));
	});

	nc.subscribe("olap_loadCubes", async (msg, reply) => {
		let failed = false;
		try {
			await olap.loadCubes();
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}
		if (!failed && reply) nc.publish(reply, JSON.stringify({status: 0}));
	});

	nc.subscribe("olap_listCubes", async (msg, reply) => {
		let failed = false;
		let result;
		try {
			result = await olap.listCubes();
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}
		if (!failed && reply) nc.publish(reply, JSON.stringify(result));
	});

	nc.subscribe("olap_deleteCube", async (msg, reply) => {
		msg = JSON.parse(msg);

		let failed = false;
		try {
			await olap.deleteCube(msg);
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}
		if (!failed && reply) nc.publish(reply, JSON.stringify({status: 0}));
	});

	nc.subscribe("olap_startAutoUpdate", async (msg, reply) => {
		msg = JSON.parse(msg);

		let failed = false;
		try {
			await olap.startAutoUpdate(msg);
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}
		if (!failed && reply) nc.publish(reply, JSON.stringify({status: 0}));
	});

	nc.subscribe("olap_stopAutoUpdate", async (msg, reply) => {
		let failed = false;
		try {
			await olap.stopAutoUpdate();
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}
		if (!failed && reply) nc.publish(reply, JSON.stringify({status: 0}));
	});

	nc.subscribe("olap_updateAggregates", async (msg, reply) => {
		let failed = false;
		try {
			await olap.updateAggregates();
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}

		if (!failed && reply) nc.publish(reply, JSON.stringify({status: 0}));
	});

	nc.subscribe("olap_aggregate", async (msg, reply) => {
		msg = JSON.parse(msg);

		let failed = false;
		let result;
		try {
			result = await olap.aggregate(msg);
		} catch (err) {
			if (reply) nc.publish(reply, errToString(err));
			failed = true;
		}

		if (!failed && reply) {
			if (msg.pretty) nc.publish(reply, JSON.stringify(result, null, 4));
			else nc.publish(reply, JSON.stringify({status: 0, aggregates: result}));
		}
	});
}

module.exports = {startService};

