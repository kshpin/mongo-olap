const {Timestamp} = require("mongodb");
const EventEmitter = require("events");
const logger = require("./logs/logger").child({
	module: "OLAP"
});

const Cube = require("./Cube");

class OLAP {

	static DEFAULT_BUFFERING= true;
	static DEFAULT_AUTO_UPDATING = true;
	static DEFAULT_UPDATE_INTERVAL = 30000;

	client;
	db;
	cubes;

	oplogStream;
	oplogBuffer;
	buffering; // state

	updateTimeout;
	updateInterval; // state
	autoUpdating; // state

	currentlyUpdating;
	finishEmitter;

	stateColName;
	cubeMetaInfoColName;

	oldestOplogTs;

	constructor(client, db, stateColName, cubeInfoColName) {
		this.client = client;
		this.db = db;
		this.cubes = {};

		this.oplogBuffer = [];

		this.currentlyUpdating = false;

		this.finishEmitter = new EventEmitter();

		this.stateColName = stateColName;
		this.cubeMetaInfoColName = cubeInfoColName;

		this.oldestOplogTs = Timestamp.fromNumber(Date.now()/1000);

		this.onData = function(doc) {
			this.oplogBuffer.push({
				ns: doc.ns,
				ts: doc.ts,
				o: doc.o,
				o2: doc.o2
			});
		}.bind(this);
		this.onError = function() {
			this.startOplogBuffering();
		}.bind(this);
	}

	async loadState() {
		let log = logger.child({
			func: "loadState"
		});

		let state = await this.db.collection(this.stateColName).find({_id: "state"}).next();
		if (state) {
			if (typeof state.buffering !== "boolean") {
				log.warn({message: "invalid [buffering] state, using default", state});
				state.buffering = OLAP.DEFAULT_BUFFERING;
				await this.db.collection(this.stateColName).updateOne({_id: "state"}, {$set: {buffering: state.buffering}});
			}
			if (typeof state.autoUpdating !== "boolean") {
				log.warn({message: "invalid [autoUpdating] state, using default", state});
				state.autoUpdating = OLAP.DEFAULT_AUTO_UPDATING;
				await this.db.collection(this.stateColName).updateOne({_id: "state"}, {$set: {autoUpdating: state.autoUpdating}});
			}
			if (typeof state.updateInterval !== "number") {
				log.warn({message: "invalid [updateInterval] state, using default", state});
				state.updateInterval = OLAP.DEFAULT_UPDATE_INTERVAL;
				await this.db.collection(this.stateColName).updateOne({_id: "state"}, {$set: {updateInterval: state.updateInterval}});
			}
		} else {
			log.info({message: "no state, adding"});
			state = {buffering: true, autoUpdating: true, updateInterval: OLAP.DEFAULT_UPDATE_INTERVAL};
			await this.db.collection(this.stateColName).insertOne({_id: "state", ...state});
		}

		if (state.buffering) await this.startOplogBuffering();
		if (state.autoUpdating) await this.startAutoUpdate(state.updateInterval);
	}

	async createCube({name, model, principalEntity}) {
		let colName = model.source.slice(model.source.indexOf(".")+1);

		let cube = new Cube(this.client, this.db, this.cubeMetaInfoColName, colName, name, model, principalEntity);
		await cube.initNewCube();

		if (Object.keys(this.cubes).length === 0) this.oldestOplogTs = cube.lastProcessed;

		if (this.cubes[colName]) this.cubes[colName].push(cube);
		else this.cubes[colName] = [cube];

		if (this.buffering) await this.startOplogBuffering();
	}

	async loadCubes() {
		let log = logger.child({
			func: "loadCubes"
		});

		log.debug({stage: "getting cube information from the database"});

		let existingCubes = await this.db.collection(this.cubeMetaInfoColName).find({}).toArray();

		log.debug({stage: "loading cubes"});

		let errors = [];
		for (const extCube of existingCubes) {
			if (!extCube.valid) {
				log.trace({stage: "loading cubes", cube: extCube._id, message: "cube invalid, skipping"});
				continue;
			}

			let result = await this._loadCube(extCube._id, extCube.model, extCube.principalEntity, extCube.lastProcessed);

			if (result === 0) log.trace({stage: "loading cubes", cube: extCube._id, message: "loaded successfully"});
			else if (result === 1) log.trace({stage: "loading cubes", cube: extCube._id, message: "already loaded"});
			else {
				log.warn({stage: "loading cubes", cube: extCube._id, error: result});
				errors.push(result);
			}
		}

		if (this.buffering) {
			log.debug({stage: "rebuffering oplogs"});
			await this.startOplogBuffering();
		}

		return {errors};
	}

	async _loadCube(name, model, principalEntity, lastProcessed) {
		let colName = model.source.slice(model.source.indexOf(".")+1);
		if (this.cubes[colName] && this.cubes[colName].some(c => c.name === name)) return 1;

		let cube = new Cube(this.client, this.db, this.cubeMetaInfoColName, colName, name, model, principalEntity);

		try {
			await cube.validate();
		} catch (err) {
			this.db.collection(this.cubeMetaInfoColName).updateOne({_id: name}, {$set: {valid: false}});

			return "OLAP::_loadCube: could not load cube [" + name + "] for collection [" + colName + "]";
		}

		if (this.cubes[colName]) this.cubes[colName].push(cube);
		else this.cubes[colName] = [cube];

		if (this.oldestOplogTs > lastProcessed) this.oldestOplogTs = lastProcessed;

		return 0;
	}

	listCubes() {
		return Object.entries(this.cubes).map(([col, cubes]) => ({
			collection: col,
			cubes: cubes.map(cube => ({
				name: cube.name,
				principalEntity: cube.principalEntity
			}))
		}));
	}

	async deleteCube({colName, cubeName}) {
		if (!this.cubes.hasOwnProperty(colName)) throw new Error("OLAP::deleteCube: no cubes for collection [" + colName + "]");

		let cubeIdx = this.cubes[colName].findIndex(cube => cube.name === cubeName);
		if (cubeIdx === -1) throw new Error("OLAP::deleteCube: no cube [" + cubeName + "] for collection [" + colName + "]");
		let cube = this.cubes[colName][cubeIdx];

		await this.db.collection(cube.cubeColName).drop();
		await this.db.collection(cube.shadowColName).drop();
		await this.db.collection(this.cubeMetaInfoColName).deleteOne({_id: cube.name});

		this.cubes[colName].splice(cubeIdx, 1);
	}

	_getNameSpaces() {
		return Object.values(this.cubes).reduce((accumulator, cubes) => [...accumulator, ...cubes.map(cube => cube.model.source)], []);
	}

	_queueUpdate() {
		clearTimeout(this.updateTimeout);
		this.updateTimeout = setTimeout(this.updateAggregates.bind(this), this.updateInterval);
	}

	async startAutoUpdate({interval=OLAP.DEFAULT_UPDATE_INTERVAL}) {
		this.autoUpdating = true;
		this.updateInterval = interval;

		await this.db.collection(this.stateColName).updateOne({_id: "state"}, {$set: {autoUpdating: true, updateInterval: interval}});

		this._queueUpdate();
	}

	stopAutoUpdate({exiting=false}) {
		clearTimeout(this.updateTimeout);
		this.autoUpdating = false;

		if (!exiting) this.db.collection(this.stateColName).updateOne({_id: "state"}, {$set: {autoUpdating: false}});
	}

	async startOplogBuffering() {
		await this.stopOplogBuffering({});

		this.oplogStream = this.client.db("local").collection("oplog.rs").find({
			ns: {$in: this._getNameSpaces()},
			ts: {$gt: this.oldestOplogTs},
			op: {$in: ["i", "u", "d"]},
			$or: [{o: {$exists: 1}}, {o2: {$exists: 1}}]
		}, {
			tailable: true,
			awaitData: true,
			oplogReplay: true, // skip initial scan, only works when restricting "ts"
			numberOfRetries: Number.MAX_VALUE
		}).project({
			ns: 1,
			ts: 1,
			o: 1,
			o2: 1
		}).stream();

		this.oplogStream.on("data", this.onData);
		this.oplogStream.on("error", this.onError);

		this.buffering = true;

		await this.db.collection(this.stateColName).updateOne({_id: "state"}, {$set: {buffering: true}});
	}

	async stopOplogBuffering({exiting=false}) {
		if (!this.oplogStream) return;

		this.oplogStream.off("data", this.onData);
		this.oplogStream.off("error", this.onError);

		await this.oplogStream.close();
		this.oplogStream = null;

		this.buffering = false;

		if (!exiting) this.db.collection(this.stateColName).updateOne({_id: "state"}, {$set: {buffering: false}});
	}

	async updateAggregates() {
		let log = logger.child({
			func: "updateAggregates"
		});

		this.currentlyUpdating = true;

		log.debug({stage: "getting oplogs"});

		let oplogs;
		if (this.buffering) {
			oplogs = this.oplogBuffer;
			this.oplogBuffer = [];
		} else oplogs = await this.client.db("local").collection("oplog.rs").find({
			ns: {$in: this._getNameSpaces()},
			ts: {$gt: this.oldestOplogTs},
			op: {$in: ["i", "u", "d"]},
			$or: [{o: {$exists: 1}}, {o2: {$exists: 1}}]
		}, {
			tailable: false,
			awaitData: false,
			oplogReplay: false,
			numberOfRetries: 0
		}).project({
			ns: 1,
			ts: 1,
			o: 1,
			o2: 1
		}).toArray();

		log.debug({stage: "filtering oplogs"});

		let oplogsByCol = {};
		let lastOplogTs = Timestamp(0, 0);
		Object.keys(this.cubes).forEach(colName => {
			let ns = this.db.databaseName + "." + colName;
			oplogsByCol[colName] = oplogs.filter(oplog => {
				if (lastOplogTs.compare(oplog.ts) < 0) lastOplogTs = oplog.ts;
				return oplog.ns === ns;
			}).map(oplog => {
				if (oplog.o2) return {ts: oplog.ts, _id: oplog.o2._id};
				return {ts: oplog.ts, _id: oplog.o._id};
			});
		});

		let affectedCols = Object.keys(oplogsByCol);

		log.debug({stage: "updating aggregates"});

		for (let col of affectedCols) {
			for (let curCube of this.cubes[col]) {
				log.trace({stage: "updating aggregates", cube: curCube.name});
				await curCube.processOplogs(oplogsByCol[col], lastOplogTs);
			}
		}

		if (lastOplogTs) this.oldestOplogTs = lastOplogTs;

		if (this.autoUpdating) this._queueUpdate(this.updateInterval);

		this.currentlyUpdating = false;
		this.finishEmitter.emit("done");
	}

	async aggregate({colName, cubeName, measures, dimensions, filters, dateReturnFormat="ms"}) {
		let log = logger.child({
			func: "aggregate"
		});

		log.debug({stage: "entered function"});

		if (!this.cubes.hasOwnProperty(colName)) throw new Error("OLAP::getAggregates: no cubes for collection [" + colName + "]");

		await this.updateAggregates();

		let cube = this.cubes[colName].find(cube => cube.name === cubeName);
		if (!cube) throw new Error("OLAP::getAggregates: no cube [" + cubeName + "] for collection [" + colName + "]");

		log.debug({
			message: "arguments valid so far"
		});

		return await cube.getAggregates(measures, dimensions, filters, dateReturnFormat);
	}
}

module.exports = OLAP;
