const Cube = require("./Cube");

class OLAP {
	DEFAULT_UPDATE_INTERVAL = 30000; // ms

	client;
	db;
	cubes;

	oplogsCache;
	caching;

	updateInterval;
	updateTimeout;
	autoUpdating;

	configColName;

	oldestOplogTs;

	constructor(client, db, configColName) {
		this.client = client;
		this.db = db;
		this.cubes = {};

		this.oplogsCache = [];
		this.caching = false;

		this.updateInterval = this.DEFAULT_UPDATE_INTERVAL;
		this.autoUpdating = false;
		this.updateAggregates = this.updateAggregates.bind(this);

		this.configColName = configColName;
	}

	async createCube({name, model, principalEntity}) {
		let colName = model.source.slice(model.source.indexOf(".")+1);

		let cube = new Cube(this.client, this.db, this.configColName, colName, name, model, principalEntity);
		await cube.initNewCube();

		if (Object.keys(this.cubes).length === 0) this.oldestOplogTs = cube.lastProcessed;

		if (this.cubes[colName]) this.cubes[colName].push(cube);
		else this.cubes[colName] = [cube];
	}

	async loadCubes() {
		let existingCubes = await this.db.collection(this.configColName).find({}).toArray();

		for (const extCube of existingCubes) {
			await this._loadCube(extCube._id, extCube.model);
			if (this.oldestOplogTs > extCube.lastProcessed) this.oldestOplogTs = extCube.lastProcessed;
		}
	}

	async _loadCube(name, model) {
		let colName = model.source.slice(model.source.indexOf(".")+1);
		if (this.cubes[colName] && this.cubes[colName].some(c => c.name === name)) return;

		let cube = new Cube(this.client, this.db, this.configColName, colName, name, model);

		let failed = false;
		try {
			await cube.validate();
		} catch (err) {
			console.log(err);
			console.log("OLAP::_loadCube: could not load cube [" + name + "] for collection [" + colName + "]");
			failed = true;
		}
		if (!failed) {
			if (this.cubes[colName]) this.cubes[colName].push(cube);
			else this.cubes[colName] = [cube];
		}
	}

	listCubes() {
		let cols = [];

		Object.entries(this.cubes).forEach(([col, cubes]) => {
			cols.push({collection: col, cubes: cubes.map(cube => cube.name)});
		});

		return cols;
	}

	async deleteCube({colName, cubeName}) {
		if (!this.cubes.hasOwnProperty(colName)) throw new Error("OLAP::deleteCube: no cubes for collection [" + colName + "]");

		let cubeIdx = this.cubes[colName].findIndex(cube => cube.name === cubeName);
		if (cubeIdx === -1) throw new Error("OLAP::deleteCube: no cube [" + cubeName + "] for collection [" + colName + "]");
		let cube = this.cubes[colName][cubeIdx];

		await this.db.collection(cube.cubeColName).drop();
		await this.db.collection(cube.shadowColName).drop();
		await this.db.collection(this.configColName).deleteOne({_id: cube.name});

		this.cubes[colName].splice(cubeIdx, 1);
	}

	startAutoUpdate({interval=30000}) {
		this.updateInterval = interval;
		this.updateTimeout = setTimeout(this.updateAggregates, interval);
		this.autoUpdating = true;
	}

	stopAutoUpdate() {
		clearTimeout(this.updateTimeout);
		this.autoUpdating = false;
	}

	startOplogCaching() {
		if (this.caching) throw new Error("OLAP::startOplogCaching: already caching");

		this.oplogStream = this.client.db("local").collection("oplog.rs").find({
			wall: {$gt: this.oldestOplogTs},
			op: {$in: ["i", "u", "d"]},
			$or: [{o: {$exists: 1}}, {o2: {$exists: 1}}]
		}, {
			tailable: true,
			awaitData: true,
			oplogReplay: true,
			numberOfRetries: Number.MAX_VALUE
		}).project({
			ns: 1,
			wall: 1,
			o: 1,
			o2: 1
		}).stream();

		this.oplogStream.on("error", () => {
			throw new Error("OLAP::startOplogCaching: oplog stream crashed");
		});
		this.oplogStream.on("data", doc => {
			this.oplogsCache.push(doc);
		});

		this.caching = true;
	}

	async stopOplogCaching() {
		await this.oplogStream.close();

		this.caching = false;
	}

	async updateAggregates() {
		let oplogs;
		if (this.caching) oplogs = this.oplogsCache;
		else oplogs = await this.client.db("local").collection("oplog.rs").find({
			wall: {$gt: this.oldestOplogTs},
			op: {$in: ["i", "u", "d"]},
			$or: [{o: {$exists: 1}}, {o2: {$exists: 1}}]
		}, {
			tailable: false,
			awaitData: false,
			oplogReplay: false,
			numberOfRetries: 0
		}).project({
			ns: 1,
			wall: 1,
			o: 1,
			o2: 1
		}).toArray();

		let oplogsByCol = {};
		let lastOplogTs = 0;

		Object.keys(this.cubes).forEach(colName => {
			let ns = this.db.databaseName + "." + colName;
			oplogsByCol[colName] = oplogs.filter(oplog => {
				if (lastOplogTs < oplog.wall) lastOplogTs = oplog.wall;
				return oplog.ns === ns;
			}).map(oplog => {
				if (oplog.o2) return {ts: oplog.wall, _id: oplog.o2._id};
				return {ts: oplog.wall, _id: oplog.o._id};
			});

			if (oplogsByCol[colName].length === 0) delete oplogsByCol[colName];
		});

		let affectedCols = Object.keys(oplogsByCol);

		for (let col of affectedCols) {
			for (let curCube of this.cubes[col]) {
				await curCube.processOplogs(oplogsByCol[col], lastOplogTs);
			}
		}

		if (lastOplogTs) this.oldestOplogTs = lastOplogTs;

		if (this.autoUpdating) this.startAutoUpdate(this.updateInterval);
	}

	async aggregate({colName, cubeName, measures, dimensions, filters, dateReturnFormat="ms"}) {
		if (!this.cubes.hasOwnProperty(colName)) throw new Error("OLAP::getAggregates: no cubes for collection [" + colName + "]");

		await this.updateAggregates();

		let cube = this.cubes[colName].find(cube => cube.name === cubeName);
		if (!cube) throw new Error("OLAP::getAggregates: no cube [" + cubeName + "] for collection [" + colName + "]");

		return await cube.getAggregates(measures, dimensions, filters, dateReturnFormat);
	}
}

module.exports = OLAP;
