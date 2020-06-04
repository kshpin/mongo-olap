class Cube {

	static GRANULARITIES = ["year", "month", "day", "hour"];

	client;
	db;
	model;

	name;
	principalEntity;
	lastProcessed;

	toShadowProjection;
	toCubeInsertAggregation;
	toCubeDeleteAggregation;

	configColName;

	dataColName;
	cubeColName;
	shadowColName;

	constructor(client, db, configColName, colName, name, model, principalEntity) {
		this.client = client;
		this.db = db;
		this.model = model;

		this.name = name;
		this.principalEntity = principalEntity;

		this.configColName = configColName;

		this.dataColName = colName;
		this.cubeColName = colName + "_" + name + "_cube";
		this.shadowColName = colName + "_" + name + "_shadow";
	}

	async initNewCube() {
		let toShadowProjection = {};

		let groupId = {};
		let toCubeProjectionQuery = {_id: false, count: "$_count"};

		let unwindArrays = [];
		let convertDates = [];

		let indexKeys = {};

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

		this.lastProcessed = (await this.client.db("local").collection("oplog.rs").find({}, {wall: 1}).sort([["wall", -1]]).limit(1).next()).wall;

		const session = this.client.startSession();
		session.startTransaction({});

		try {
			if (await this.db.listCollections({name: this.shadowColName}).hasNext()) await this.db.collection(this.shadowColName).drop();
			if (await this.db.listCollections({name: this.cubeColName}).hasNext()) await this.db.collection(this.cubeColName).drop();

			await this.db.collection(this.dataColName).aggregate(toShadowAggregationQuery, {allowDiskUse: true}).next();
			await this.db.collection(this.shadowColName).aggregate(toCubeAggregationQuery, {allowDiskUse: true}).next();

			await this.db.createCollection(this.configColName);
			await this.db.collection(this.configColName).deleteMany({model: this.model}, {session});
			await this.db.collection(this.configColName).insertOne({_id: this.name, model: this.model, lastProcessed: this.lastProcessed, valid: true}, {session});

			await this.db.collection(this.cubeColName).createIndex(indexKeys, {unique: true});
			let keys = Object.keys(indexKeys);
			if (keys.length > 1) {
				for (let key of keys) {
					await this.db.collection(this.cubeColName).createIndex({[key]: 1});
				}
			}

			await session.commitTransaction();
			session.endSession();
		} catch (err) {
			await session.abortTransaction();
			session.endSession();

			throw err;
		}

		this._buildQueries();
	}

	async validate() {
		try {
			await new Promise((res, rej) => this.db.collection(this.shadowColName, {strict: true}, err => {
				if (err) rej(err);
				else res();
			}));
		} catch (err) {
			throw new Error("Cube::validate: shadow collection missing");
		}
		try {
			await new Promise((res, rej) => this.db.collection(this.cubeColName, {strict: true}, err => {
				if (err) rej(err);
				else res();
			}));
		} catch (err) {
			throw new Error("Cube::validate: cube collection missing");
		}

		if (!(await this.db.collection(this.configColName).find({_id: this.name}).next()).valid) throw new Error("Cube::validate: cube not valid");

		this._buildQueries();
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

	async processOplogs(oplogs, lastProcessed) {
		if (!(await this.db.collection(this.configColName).find({_id: this.name}).next()).valid) throw new Error("Cube::processOplogs: cube not valid");

		let ids = oplogs.filter(oplog => oplog.ts.getTime() > this.lastProcessed).map(oplog => oplog._id);
		if (ids.length === 0) return;
		let shadowQuery = {_id: {$in: ids}};

		await this.db.collection(this.configColName).updateOne({_id: this.name}, {$set: {valid: false}});

		// subtract new shadow docs from aggregates
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

		// mark last processed time
		await this.db.collection(this.configColName).updateOne({_id: this.name}, {$set: {lastProcessed, valid: true}});
		this.lastProcessed = lastProcessed;
	}

	async getAggregates(measures=[], dimensions=[], filters, dateReturnFormat) {
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

					convertDates.push(toDiscreteTime); // to year/month/day/hour

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
	source: "sourceDatabase.sourceCollection",
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
			timeFormat: "ms" // ["s", "ms", "ISODate", "ISODateString", "Timestamp"]
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
