const {MongoClient} = require("mongodb");
const NATS = require("nats");
const OLAP = require("./OLAP");
const logger = require("./logs/logger").child({
	module: "OLAPService"
});

let config = require("./config");

async function startService() {
	let log = logger.child({
		func: "startService"
	});

	let mongoClient;
	let db;
	let olap;

	let nc;

	let subscribeCodes = [];

	// process signal handle -------------------------------------------------------------------------------------------

	let processingRequests = 0;
	let shuttingDown = false;

	let sigResponse = async (signal) => {
		let log = logger.child({
			func: "sigResponse"
		});

		log.info({
			event: `received signal [${signal}]`,
		});

		shuttingDown = true;

		if (olap) olap.stopAutoUpdate({exiting: true});

		if (processingRequests !== 0 || (olap && olap.currentlyUpdating)) log.debug({stage: "waiting for jobs to complete"});
		while (processingRequests !== 0 || (olap && olap.currentlyUpdating)) {
			await new Promise(res => olap.finishEmitter.once("done", res));

			let trace = {
				event: "job completed",
				requestsLeft: processingRequests
			};
			if (olap) trace.currentlyUpdatingAggregates = olap.currentlyUpdating;
			log.trace(trace);
		}

		log.info({stage: "shutting down"});

		try {
			if (olap) {
				await olap.stopOplogBuffering({exiting: true});
				log.debug({event: "oplog buffering stopped"});
			}
			if (mongoClient) {
				await mongoClient.close();
				log.debug({event: "mongo client closed"});
			}

			for (code of subscribeCodes) {
				nc.unsubscribe(code);
			}

			log.debug({stage: "shutting down", event: "done"});
		} catch (err) {
			log.warn({error: err.message});
			process.exit(1);
		}

		process.exit(0);
	};

	process.on("SIGTERM", () => sigResponse("SIGTERM"));
	process.on("SIGINT", () => sigResponse("SIGINT"));

	// -----------------------------------------------------------------------------------------------------------------

	let dbRetryInterval = process.env.DB_RETRY_INTERVAL || 1000;
	log.debug({stage: `Connecting to MongoDB at [${config.mongo.url}]`});
	while (true) {
		try {
			[mongoClient, db] = await connectDb(config.mongo.url, config.mongo.dbName);

			break;
		} catch (err) {
			log.trace({message: "MongoDB retrying", error: err.message});
			await new Promise(res => setTimeout(res, dbRetryInterval));
		}
	}

	log.debug({stage: "Connected to MongoDB"});

	try {
		olap = new OLAP(mongoClient, db, "olap_state", "olap_cubes");
	} catch (err) {
		log.fatal({error: err.message});
		sigResponse("SELF KILL");
	}

	let connected = false;

	log.debug({stage: `Connecting to NATS at [${config.nats.url}]`});

	nc = NATS.connect({ // TODO add other authorization methods
		url: process.env.NATS_URL || "nats://localhost:4222/",
		//user: "foo",
		//pass: "bar",
		json: true,
		maxPingOut: 1,
		maxReconnectAttempts: -1,
		pingInterval: config.nats.pingInterval,
		waitOnFirstConnect: true
	});

	nc.on("connect", async () => {
		log.debug({event: "Connected to NATS"});
		connected = true;

		log.debug({stage: "loading state"});
		await olap.loadState();

		subscribeCodes = [
			nc.subscribe("olap_createCube",			(msg, reply) => natsResponse(msg, reply, olap.createCube)),
			nc.subscribe("olap_loadCubes",			(msg, reply) => natsResponse(msg, reply, olap.loadCubes)),
			nc.subscribe("olap_listCubes",			(msg, reply) => natsResponse(msg, reply, olap.listCubes)),
			nc.subscribe("olap_deleteCube",			(msg, reply) => natsResponse(msg, reply, olap.deleteCube)),
			nc.subscribe("olap_startAutoUpdate",		(msg, reply) => natsResponse(msg, reply, olap.startAutoUpdate)),
			nc.subscribe("olap_stopAutoUpdate",		(msg, reply) => natsResponse(msg, reply, olap.stopAutoUpdate)),
			nc.subscribe("olap_startOplogBuffering",	(msg, reply) => natsResponse(msg, reply, olap.startOplogBuffering)),
			nc.subscribe("olap_stopOplogBuffering",	(msg, reply) => natsResponse(msg, reply, olap.stopOplogBuffering)),
			nc.subscribe("olap_updateAggregates",	(msg, reply) => natsResponse(msg, reply, olap.updateAggregates)),
			nc.subscribe("olap_aggregate",			(msg, reply) => natsResponse(msg, reply, olap.aggregate))
		];
	});
	nc.on("error", err => {
		log.error({error: err.message});
	});
	nc.on("disconnect", () => {
		log.fatal({error: "NATS disconnected"});
	});
	nc.on("reconnecting", () => {
		if (connected) sigResponse("SELF KILL");
		else log.trace("NATS retrying");
	});

	// nats request handle ---------------------------------------------------------------------------------------------

	let natsResponse = async (args, reply, func) => {
		let log = logger.child({
			func: "natsResponse"
		});

		log.debug({
			event: "request received",
			args
		});

		if (shuttingDown && reply) {
			nc.publish(reply, JSON.stringify({
				response: "shutting down: request ignored",
				success: false
			}));
			log.debug("shutting down: request ignored");
			return;
		}

		log.debug({event: "starting job", job: func.name});

		processingRequests++;
		let result = await runFunction(func, args);
		processingRequests--;

		if (reply) nc.publish(reply, result);

		log.debug({event: "job complete", job: func.name});
		olap.finishEmitter.emit("done");
	};

	let runFunction = async (func, params) => {
		try {
			return {response: await func.apply(olap, [params]), success: true};
		} catch (err) {
			return {response: err.message, success: false};
		}
	};
}

async function connectDb(url, dbName) {
	let mongoClient = await MongoClient.connect(url);
	let db = mongoClient.db(dbName);
	return [mongoClient, db];
}

function errToSendable(err) {
	return {message: err.message, stackTrace: err.stack.toString()};
}

module.exports = {startService};
