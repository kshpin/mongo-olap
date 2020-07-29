const {Timestamp} = require("mongodb");
const logger = require("./logs/logger").child({
	module: "Cube"
});

class Cube {

	static GRANULARITIES = ["year", "month", "day", "hour"]; // descending order necessary

	client;
	db;
	model;

	name;
	principalEntity;
	lastProcessed;

	toShadowProjection;
	toCubeInsertAggregation;
	toCubeDeleteAggregation;

	cubeMetaInfoColName;

	dataColName;
	cubeColName;
	shadowColName;

	constructor(client, db, configColName, name, model, principalEntity) {
		this.client = client;
		this.db = db;
		this.model = model;

		this.name = name;
		this.principalEntity = principalEntity;

		this.cubeMetaInfoColName = configColName;

		this.dataColName = model.source;
		this.cubeColName = `olap_${model.source}_${name}_cube`;
		this.shadowColName = `olap_${model.source}_${name}_shadow`;
	}

	async initNew(skipPreaggregation) {
		let log = logger.child({
			func: "initNew"
		});

		let toShadowProjection = {};

		let groupId = {};
		let toCubeProjectionQuery = {_id: false, count: "$_count"};

		let unwindArrays = [];
		let convertDates = [];

		let indexKeys = {};

		log.debug({stage: "preparing group query"});

		this.model.dimensions.forEach(dim => {
			let endPath = "d."+dim.id;
			let origPath = dim.path.replace(/\[]/g, "");
			let numUnwinds = (dim.path.match(/\[]/g) || []).length;

			// unwind the shadow element as many times as there are arrays in original path
			unwindArrays.push(...Array(numUnwinds).fill({$unwind: "$"+endPath}));

			toShadowProjection[endPath] = "$"+origPath;

			if (dim.type === "time") {
				convertDates.push({$addFields: {[endPath]: {$toDate: {$convert: {input: "$"+endPath, to: "long"}}}}}); // to date
				let toDiscreteTime = {$addFields: {}};
				let parts = {};

				let matched = false;
				Cube.GRANULARITIES.forEach(gran => {
					if (!matched) {
						if (gran === "day") toDiscreteTime.$addFields[endPath+".day"] = {["$dayOfMonth"]: "$"+endPath};
						else toDiscreteTime.$addFields[endPath+"."+gran] = {["$"+gran]: "$"+endPath};

						parts[gran] = "$"+endPath+"."+gran;
					}

					if (gran === dim.granularity) matched = true;
				});
				if (!matched) throw new Error("Cube::initNewCube: unsupported granularity ["+dim.granularity+"]");

				convertDates.push(toDiscreteTime); // to year/month/day/hour

				convertDates.push({ // to truncated date
					$addFields: {
						[endPath]: {
							$dateFromParts: parts
						}
					}
				});
			}

			groupId[dim.id] = {$ifNull: ["$"+endPath, "__null__"]};
			toCubeProjectionQuery[endPath] = "$_id."+dim.id;

			indexKeys[endPath] = 1;
		});

		let groupQuery = {
			_id: groupId,
			_count: {$sum: 1}
		};

		log.debug({stage: "preparing aggregation operation query"});

		this.model.measures.forEach(measure => {
			let endPath = "m."+measure.id;
			let origPath = measure.path.replace(/\[]/g, "");
			let numUnwinds = (measure.path.match(/\[]/g) || []).length;

			// unwind the shadow element as many times as there are arrays in original path
			unwindArrays.push(...Array(numUnwinds).fill({$unwind: "$"+endPath}));

			toShadowProjection[endPath] = "$"+origPath;

			groupQuery[measure.id] = {$sum: "$"+endPath};
			toCubeProjectionQuery[endPath] = "$"+measure.id;
		});

		let toShadowAggregationQuery = [
			{$project: toShadowProjection},
			{$out: this.shadowColName}
		];

		let toCubeAggregationQuery = [
			...unwindArrays,
			...convertDates,
			{$group: groupQuery},
			{$project: toCubeProjectionQuery},
			{$out: this.cubeColName}
		];

		log.debug({stage: "getting last processed time"});

		this.lastProcessed = Timestamp.fromNumber(Math.floor((await this.client.db("admin").collection("system.version").aggregate([{$addFields: {_ts: "$$NOW"}}]).next())._ts.getTime()/1000));

		log.debug({stage: "applying queries"});

		const session = this.client.startSession();
		session.startTransaction({});

		try {
			if (await this.db.listCollections({name: this.shadowColName}).hasNext()) await this.db.collection(this.shadowColName).drop();
			if (await this.db.listCollections({name: this.cubeColName}).hasNext()) await this.db.collection(this.cubeColName).drop();

			if (skipPreaggregation) {
				log.debug({message: "skipping shadow/cube collection aggregation"});
				await this.db.createCollection(this.shadowColName);
				await this.db.createCollection(this.cubeColName);
			} else {
				log.debug({message: "creating shadow collection"});
				await this.db.collection(this.dataColName).aggregate(toShadowAggregationQuery, {allowDiskUse: true}).next();
				log.debug({message: "creating cube collection"});
				await this.db.collection(this.shadowColName).aggregate(toCubeAggregationQuery, {allowDiskUse: true}).next();
			}

			log.debug({message: "updating meta information"});

			await this.db.createCollection(this.cubeMetaInfoColName);
			await this.db.collection(this.cubeMetaInfoColName).deleteMany({model: this.model}, {session});
			await this.db.collection(this.cubeMetaInfoColName).insertOne({_id: this.name, model: this.model, lastProcessed: this.lastProcessed, valid: true}, {session});

			log.debug({message: "creating indexes"});

			await this.db.collection(this.cubeColName).createIndex(indexKeys, {unique: true, sparse: true});
			let keys = Object.keys(indexKeys);
			if (keys.length > 1) {
				for (let key of keys) {
					await this.db.collection(this.cubeColName).createIndex({[key]: 1});
				}
			}

			await session.commitTransaction();
		} catch (err) {
			await session.abortTransaction();
			throw err;
		} finally {
			session.endSession();
		}

		this._buildQueries();
	}

	async load(lastProcessed) {
		try {
			await new Promise((res, rej) => this.db.collection(this.shadowColName, {strict: true}, err => {
				if (err) rej(err);
				else res();
			}));
			await new Promise((res, rej) => this.db.collection(this.cubeColName, {strict: true}, err => {
					if (err) rej(err);
					else res();
				}));

			this._buildQueries();
		} catch (err) {
			this.db.collection(this.cubeMetaInfoColName).updateOne({_id: this.name}, {$set: {valid: false}});
			return false;
		}

		this.lastProcessed = lastProcessed;
		return true;
	}

	_buildQueries() {
		let toShadowProjection = {};

		let groupId = {};
		let toCubeProjectionQuery = {_id: false, count: "$_count"};

		let mergeKeys = [];

		let unwindArrays = [];
		let convertDates = [];

		this.model.dimensions.forEach(dim => {
			let endPath = "d."+dim.id;
			let origPath = dim.path.replace(/\[]/g, "");
			let numUnwinds = (dim.path.match(/\[]/g) || []).length;

			// unwind the shadow element as many times as there are arrays in original path
			unwindArrays.push(...Array(numUnwinds).fill({$unwind: "$"+endPath}));

			toShadowProjection[endPath] = "$"+origPath;

			if (dim.type === "time") {
				convertDates.push({$addFields: {[endPath]: {$toDate: {$convert: {input: "$"+endPath, to: "long"}}}}}); // to date
				let toDiscreteTime = {$addFields: {}};
				let parts = {};

				let matched = false;
				Cube.GRANULARITIES.forEach(gran => {
					if (!matched) {
						if (gran === "day") toDiscreteTime.$addFields[endPath+".day"] = {["$dayOfMonth"]: "$"+endPath};
						else toDiscreteTime.$addFields[endPath+"."+gran] = {["$"+gran]: "$"+endPath};

						parts[gran] = "$"+endPath+"."+gran;
					}

					if (gran === dim.granularity) matched = true;
				});
				if (!matched) throw new Error("Cube::initNewCube: unsupported granularity ["+dim.granularity+"]");

				convertDates.push(toDiscreteTime); // to year/month/day/hour

				convertDates.push({ // to truncated date
					$addFields: {
						[endPath]: {
							$dateFromParts: parts
						}
					}
				});
			}

			groupId[dim.id] = {$ifNull: ["$"+endPath, "__null__"]};
			toCubeProjectionQuery[endPath] = "$_id."+dim.id;

			mergeKeys.push(endPath);
		});

		let insertGroupQuery = {
			_id: groupId,
			_count: {$sum: 1}
		};
		let deleteGroupQuery = {
			_id: groupId,
			_count: {$sum: -1}
		};

		let onMatchProjection = {
			count: {$add: ["$count", "$$new.count"]},
			d: "$d"
		};

		this.model.measures.forEach(measure => {
			let endPath = "m."+measure.id;
			let origPath = measure.path.replace(/\[]/g, "");
			let numUnwinds = (measure.path.match(/\[]/g) || []).length;

			// unwind the shadow element as many times as there are arrays in original path
			unwindArrays.push(...Array(numUnwinds).fill({$unwind: "$"+endPath}));

			toShadowProjection[endPath] = "$"+origPath;

			insertGroupQuery[measure.id] = {$sum: "$"+endPath};
			deleteGroupQuery[measure.id] = {$sum: {$multiply: ["$"+endPath, -1]}};

			toCubeProjectionQuery[endPath] = "$"+measure.id;

			onMatchProjection[endPath] = {
				$add: ["$"+endPath, "$$new."+endPath]
			};
		});

		this.toShadowProjection = toShadowProjection;
		this.toCubeInsertAggregation = [
			...unwindArrays,
			...convertDates,
			{$group: insertGroupQuery},
			{$project: toCubeProjectionQuery},
			{$merge: {
				into: this.cubeColName,
				on: mergeKeys,
				whenMatched: [
					{$project: onMatchProjection}
				]
			}}
		];
		this.toCubeDeleteAggregation = [
			...unwindArrays,
			...convertDates,
			{$group: deleteGroupQuery},
			{$project: toCubeProjectionQuery},
			{$merge: {
				into: this.cubeColName,
				on: mergeKeys,
				whenMatched: [
					{$project: onMatchProjection}
				]
			}}
		];
	}

	async _markLastProcessedTime(lastProcessed, valid) {
		await this.db.collection(this.cubeMetaInfoColName).updateOne({_id: this.name}, {$set: {lastProcessed, valid}});
		this.lastProcessed = lastProcessed;
	}

	async processOplogs(oplogs, lastProcessed) {
		let log = logger.child({
			func: "processOplogs"
		});

		if (!(await this.db.collection(this.cubeMetaInfoColName).find({_id: this.name}).next()).valid) {
			log.debug({message: `cube [${this.name}] not valid, skipping`});
			await this._markLastProcessedTime(lastProcessed, false);
			return;
		}

		let ids = oplogs.filter(oplog => oplog.ts.compare(this.lastProcessed) >= 0).map(oplog => oplog._id);
		if (ids.length === 0) {
			log.debug({message: `cube [${this.name}] has no oplogs, skipping`});
			await this._markLastProcessedTime(lastProcessed, true);
			return;
		}
		let shadowQuery = {_id: {$in: ids}};

		await this.db.collection(this.cubeMetaInfoColName).updateOne({_id: this.name}, {$set: {valid: false}});

		// subtract old shadow docs from aggregates
		await this.db.collection(this.shadowColName).aggregate([
			{$match: shadowQuery},
			...this.toCubeDeleteAggregation
		], {allowDiskUse: true}).hasNext();

		// delete old shadow docs from shadow
		await this.db.collection(this.shadowColName).deleteMany(shadowQuery);

		// add new docs to shadow
		await this.db.collection(this.dataColName).aggregate([
			{$match: shadowQuery},
			{$project: this.toShadowProjection},
			{$merge: this.shadowColName}
		], {allowDiskUse: true}).hasNext();

		// add new shadow docs to aggregates
		await this.db.collection(this.shadowColName).aggregate([
			{$match: shadowQuery},
			...this.toCubeInsertAggregation
		], {allowDiskUse: true}).hasNext();

		// purge zeros
		await this.db.collection(this.cubeColName).deleteMany({count: 0});

		await this._markLastProcessedTime(lastProcessed, true);
	}

	async getAggregates(dimensions=[], measures=[], filters, dateReturnFormat="ms") {
		let log = logger.child({
			func: "getAggregates"
		});

		log.debug({
			message: "entered Cube::getAggregates",
			measures,
			dimensions,
			filters,
			dateReturnFormat
		});

		let matchQuery = {};
		if (filters) {
			Object.entries(filters).forEach(([key, value]) => {
				let dim = this.model.dimensions.find(dim => dim.id === key);
				matchQuery["d."+key] = {};

				if (dim.type === "time") { // TODO make this look prettier
					if (value.$range) {
						if (value.$range.from) matchQuery["d."+key].$gte = new Date(value.$range.from);
						if (value.$range.to) matchQuery["d."+key].$lt = new Date(value.$range.to);
					} else matchQuery["d." + key] = new Date(value);
				} else {
					if (value.$range) {
						if (value.$range.from) matchQuery["d." + key].$gte = value.$range.from;
						if (value.$range.to) matchQuery["d." + key].$lt = value.$range.to;
					} else matchQuery["d." + key] = value;
				}
			});
		}

		let groupId = {};
		let convertDates = [];
		let finalProjection = {_id: 0, d: {}, m: {}, _count: 1};

		dimensions.forEach(dim => {
			let endPath = "d."+dim.id;

			groupId[dim.id] = "$"+endPath;
			finalProjection[endPath] = "$_id."+dim.id;

			let modelDim = this.model.dimensions.find(d => d.id === dim.id);
			if (modelDim.type === "time") {
				if (dateReturnFormat === "ms") finalProjection[endPath] = {$toDouble: "$_id."+dim.id};

				let modelIndex = Cube.GRANULARITIES.indexOf(modelDim.granularity);
				let reqIndex = Cube.GRANULARITIES.indexOf(dim.granularity);

				if (reqIndex === -1) throw new Error("Cube::getAggregates: requested granularity unsupported");

				if (reqIndex > modelIndex) throw new Error("Cube::getAggregates: requested granularity is smaller than stored");
				if (reqIndex < modelIndex) {
					convertDates.push({ // to date parts
						$addFields: {
							[endPath]: {
								$dateToParts: {
									date: {
										$convert: {
											input: {
												$convert: {
													input: "$" + endPath,
													to: "long",
													onError: {
														$convert: {
															input: 0,
															to: "long"
														}
													}
												}
											},
											to: "date"
										}
									}
								}
							}
						}
					});

					let parts = {};

					let matched = false;
					Cube.GRANULARITIES.forEach(gran => {
						if (!matched) {
							parts[gran] = "$"+endPath+"."+gran;
						}

						if (gran === dim.granularity) matched = true;
					});

					convertDates.push({ // to truncated date
						$addFields: {
							[endPath]: {
								$dateFromParts: parts
							}
						}
					});
				}
			}
		});

		if (Object.keys(finalProjection.d).length === 0) delete finalProjection.d;

		let groupQuery = {
			_id: groupId,
			_count: {$sum: "$count"}
		};

		measures.forEach(measure => {
			groupQuery[measure] = {$sum: "$m."+measure};
			finalProjection.m[measure] = "$"+measure;
		});

		if (Object.keys(finalProjection.m).length === 0) delete finalProjection.m;

		let aggregateQuery = [
			{$match: matchQuery},
			...convertDates,
			{$group: groupQuery},
			{$project: finalProjection}
		];

		return await this.db.collection(this.cubeColName).aggregate(aggregateQuery).toArray();
	}
}

module.exports = Cube;

/*
model: {
	source: "sourceCollection",
	dimensions: [
		{
			path: "accountId",
			id: "accID"
		},
		{
			path: "projectId",
			id: "projID"
		},
		{
			path: "abilities[].skills[]", // means abilities and skills are arrays, the values of which all contribute to the dimension
			id: "skills"
		},
		{
			path: "status",
			id: "status"
		},
		{
			path: "times.lastStatusAt",
			id: "lastStatusAt",
			type: "time",
			timeFormat: "ms" // TODO ["s", "ms", "ISODate", "ISODateString", "Timestamp"]
			granularity: "hour"
		}
	],
	measures: [ // count is built into each cell by default
		{
			path: "execTime",
			id: "execTime"
		}
	]
}
*/
