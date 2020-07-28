const {MongoClient} = require("mongodb");
const NATS = require("nats");
const OLAP = require("./OLAP");
const logger = require("./logs/logger").child({
	module: "OLAPService"
});

let config = require("./config");
let {InvalidRequestError} = require("./Validation");

const API = [
	"createCube",
	"loadCubes",
	"listCubes",
	"deleteCube",
	"startAutoUpdate",
	"stopAutoUpdate",
	"startOplogBuffering",
	"stopOplogBuffering",
	"updateAggregates",
	"aggregate"
];

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

	log.debug({stage: `Connecting to MongoDB at [${config.mongo.url}]`});
	while (true) {
		try {
			[mongoClient, db] = await connectDb(config.mongo.url, config.mongo.dbName);
			break;
		} catch (err) {
			log.trace({message: "MongoDB retrying", error: err.message});
			await new Promise(res => setTimeout(res, config.mongo.retryInterval));
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

	log.debug({stage: `Connecting to NATS at [${config.nats.connection.url}]`});

	nc = NATS.connect({
		json: true,
		maxPingOut: 1,
		maxReconnectAttempts: -1,
		waitOnFirstConnect: true,
		...config.nats.connection
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
	nc.on("connect", async () => {
		log.debug({event: "Connected to NATS"});
		connected = true;

		log.debug({stage: "loading state"});
		await olap.loadState();
		await olap.loadCubes();

		subscribeCodes = API.map(func => nc.subscribe(`${config.nats.prefix}.${func}`, (msg, reply) => natsResponse(msg, reply, olap[func])));
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
			return {
				data: await func.apply(olap, [params]),
				status: 0
			};
		} catch (err) {
			let response = {
				errorText: err.message
			};

			if (err instanceof InvalidRequestError) {
				response = {
					...response,
					errors: err.errors,
					status: 1
				};
			} else {
				response = {
					...response,
					status: -1
				};
			}

			return response;
		}
	};
}

async function connectDb(url, dbName) { // TODO add more authorization methods
	let mongoClient = await MongoClient.connect(url);
	let db = mongoClient.db(dbName);
	return [mongoClient, db];
}

module.exports = {startService};
