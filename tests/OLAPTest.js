const {MongoClient} = require("mongodb");
const OLAP = require("../src/OLAP");
const assert = require("assert");
const {dockerCommand} = require('docker-cli-js');

async function connectDb(url, dbName) {
	let mongoClient = await MongoClient.connect(url);
	let db = mongoClient.db(dbName);
	return {mongoClient, db};
}

describe("OLAP", function() {
	let url = "mongodb://localhost:54321/";
	let dbName = "db1";

	let client, db;
	let olap;

	before(async function() {
		this.timeout(10000);

		try {
			await dockerCommand(`kill testMongod_143`);
		} catch (e) {}

		await dockerCommand(`run \
			-d \
			--rm \
			--name testMongod_143 \
			-p 54321:27017 \
			-v ${__dirname}/config/mongod.conf:/etc/mongo/mongod.conf \
			mongo --config /etc/mongo/mongod.conf`);

		// -v ${__dirname}/config/mongod.conf:/etc/mongo/mongod.conf \
		// --config /etc/mongo/mongod.conf

		await new Promise(res => setTimeout(res, 1000));

		await dockerCommand('exec testMongod_143 mongo --eval "var a = rs.initiate();" --quiet');

		let {mongoClient:clientR, db:dbR} = await connectDb(url, dbName);
		client = clientR;
		db = dbR;
	});

	after(async function() {
		//await olap.stopOplogBuffering();

		client.close();

		await dockerCommand("kill testMongod_143");
	});

	beforeEach(async function() {
		await db.dropDatabase();
		await db.collection("c1").insertMany([
			{
				c: "City 0",
				m: 1,
				f: 2
			},
			{
				c: "City 1",
				m: 10,
				f: 20
			},
			{
				c: "City 2",
				m: 100,
				f: 200
			},
			{
				c: "City 3",
				m: 1000,
				f: 2000
			}
		]);

		assert.equal(await db.collection("c1").find({}).count(), 4);

		olap = new OLAP(client, db, "olap_state", "olap_cubes");
	});

	describe("MongoDB mechanics", function() {
		it("should have the oplog collection", async function() {
			let oplogs = await client.db("local").collection("oplog.rs").find({}).toArray();
			assert.equal(oplogs.length > 0, true);
		});

		it("should be able to tail oplogs", async function() {
			let doc = await client.db("local").collection("oplog.rs").find({}, {ts: 1}).sort([["ts", -1]]).limit(1).next();

			let q = {
				ts: {$gt: doc.ts},
				op: {$in: ["i","u","d"]},
				ns: "db1.c1"
			};

			let stream = await (await client.db("local").collection("oplog.rs").find(q, {
				tailable: true,
				awaitData: true,
				oplogReplay: true,
				noCursorTimeout: true,
				numberOfRetries: Number.MAX_VALUE
			})).stream();

			let c = 0;
			stream.on("data", () => c++);

			let a = await client.db("local").collection("oplog.rs").find(q).count();

			await db.collection("c1").insertMany([
				{a: 1},
				{b: 2},
				{c: 3}
			]);

			await new Promise(res => setTimeout(res, 100));

			let b = await client.db("local").collection("oplog.rs").find(q).count();

			assert.equal(b, a+3);
			assert.equal(c, 3);
		});
	});

	describe("#createCube()", function() {
		it("should create a new cube", async function() {
			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			assert.equal(await db.collection("olap_c1_main_cube").count(), 4);
		});

		it("should accept non-existent collections", async function() {
			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1",
						id: "d1"
					}
				],
				measures: [
					{
						path: "m1",
						id: "m1"
					}
				]
			}});
		});

		it("should accept non-matching data", async function() {
			await db.collection("c1").insertOne({
				notC: "State 0",
				notM: 4,
				notF: 8
			});

			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			assert.equal(await db.collection("olap_c1_main_cube").count(), 5);
		});

		it("should accept void measure", async function() {
			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: []
			}});

			assert.equal(await db.collection("olap_c1_main_cube").count(), 4);
		});

		it("should accept array elements in dimension path", async function() {
			await db.collection("c2").insertMany([
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "2"}
						]
					},
					m1: 1
				},
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "2"}
						]
					},
					m1: 10
				}
			]);

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1.d2[].d3",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1",
						id: "mea"
					}
				]
			}});

			let cubeEntry = await db.collection("olap_c2_main_cube").find({}).next();
			assert.equal(cubeEntry.m.mea, 11);
		});

		it("should accept array elements in measure path", async function() {
			await db.collection("c2").insertMany([
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 1},
							{m3: 2}
						]
					}
				},
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 10},
							{m3: 20}
						]
					}
				},
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1.m2[].m3",
						id: "mea"
					}
				]
			}});

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 4,
				d: {dim: "1"},
				m: {mea: 33}
			});
		});

		it("should accept array dimension with missing field", async function() {
			await db.collection("c2").insertMany([
				{
					tags: ["1", "1"],
					val: 1
				},
				{
					tags: ["1", "1"],
					val: 10
				}
			]);

			await db.collection("c2").insertOne({
				val: 100
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "tags[]",
						id: "tag"
					}
				],
				measures: [
					{
						path: "val",
						id: "value"
					}
				]
			}});

			assert.equal(await db.collection("olap_c2_main_cube").find({}).count(), 1);

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 4,
				d: {tag: "1"},
				m: {value: 22}
			});
		});

		it("should accept time dimensions with [hour] granularity", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589290200000, //  05/12/2020, tuesday, 13:30
					processLength: 20
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "hour"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			let {_id: _id0, d: d0, ...rest0} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589011200000)}).next();
			assert.deepEqual(rest0, {
				count: 1,
				m: {procLength: 1}
			});
			let {_id: _id1, d: d1, ...rest1} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589202000000)}).next();
			assert.deepEqual(rest1, {
				count: 1,
				m: {procLength: 2}
			});
			let {_id: _id2, d: d2, ...rest2} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589288400000)}).next();
			assert.deepEqual(rest2, {
				count: 2,
				m: {procLength: 30}
			});
			let {_id: _id3, d: d3, ...rest3} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589806800000)}).next();
			assert.deepEqual(rest3, {
				count: 1,
				m: {procLength: 100}
			});
		});

		it("should accept time dimensions with [day] granularity", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589290200000, //  05/12/2020, tuesday, 13:30
					processLength: 20
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "day"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			let {_id: _id0, d: d0, ...rest0} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1588982400000)}).next();
			assert.deepEqual(rest0, {
				count: 1,
				m: {procLength: 1}
			});
			let {_id: _id1, d: d1, ...rest1} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589155200000)}).next();
			assert.deepEqual(rest1, {
				count: 1,
				m: {procLength: 2}
			});
			let {_id: _id2, d: d2, ...rest2} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589241600000)}).next();
			assert.deepEqual(rest2, {
				count: 2,
				m: {procLength: 30}
			});
			let {_id: _id3, d: d3, ...rest3} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589760000000)}).next();
			assert.deepEqual(rest3, {
				count: 1,
				m: {procLength: 100}
			});
		});

		it("should accept time dimensions with [month] granularity", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589290200000, //  05/12/2020, tuesday, 13:30
					processLength: 20
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "month"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			let {_id: _id0, d: d0, ...rest0} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1588291200000)}).next();
			assert.deepEqual(rest0, {
				count: 5,
				m: {procLength: 133}
			});
		});

		it("should accept time dimensions with [year] granularity", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589290200000, //  05/12/2020, tuesday, 13:30
					processLength: 20
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "year"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			let {_id: _id0, d: d0, ...rest0} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1577836800000)}).next();
			assert.deepEqual(rest0, {
				count: 5,
				m: {procLength: 133}
			});
		});

		it("should accept missing fields gracefully", async function() {
			await db.collection("c2").insertOne({
				meta: {
					br: {
						tags: ["tag1"]
					}
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					}
				],
				measures: []
			}});

			await olap.createCube({name: "cd", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.cd.tags[]",
						id: "tag"
					}
				],
				measures: []
			}});

			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_cube").find({}).next();
			assert.deepEqual(rest0, {
				count: 1,
				d: {tag: "tag1"}
			});

			assert.equal(await db.collection("olap_c2_cd_cube").find({}).count(), 0);
			assert.equal(await db.collection("olap_c2_cd_shadow").find({}).count(), 1);

			let {_id: _id1, ...rest1} = await db.collection("olap_c2_cd_shadow").find({}).next();
			assert.deepEqual(rest1, {
				d: {}
			});
		});
	});

	describe("#loadCubes()", function() {
		it("should load nothing if config empty", async function() {
			await olap.loadCubes();
			assert.equal(Object.keys(olap.cubes).length, 0);
		});

		it("should load a cube from the config", async function() {
			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			let secondOlap = new OLAP(client, db, "olap_state", "olap_cubes");
			await secondOlap.loadCubes();
			assert.equal(Object.keys(olap.cubes).length, 1);
		});
	});

	describe("#updateAggregates()", function() {
		it("should handle absences of oplogs with no cubes", async function() {
			await olap.updateAggregates();
		});

		it("should handle absences of oplogs with a cube", async function() {
			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await olap.updateAggregates();

			let {_id: _id0, ...rest0} = await db.collection("olap_c1_main_cube").find({"d.city": "City 0"}).next();
			assert.deepEqual(rest0, {
				count: 1,
				d: {city: "City 0"},
				m: {males: 1, females: 2}
			});
			let {_id: _id1, ...rest1} = await db.collection("olap_c1_main_cube").find({"d.city": "City 1"}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {city: "City 1"},
				m: {males: 10, females: 20}
			});
			let {_id: _id2, ...rest2} = await db.collection("olap_c1_main_cube").find({"d.city": "City 2"}).next();
			assert.deepEqual(rest2, {
				count: 1,
				d: {city: "City 2"},
				m: {males: 100, females: 200}
			});
			let {_id: _id3, ...rest3} = await db.collection("olap_c1_main_cube").find({"d.city": "City 3"}).next();
			assert.deepEqual(rest3, {
				count: 1,
				d: {city: "City 3"},
				m: {males: 1000, females: 2000}
			});
		});

		it("should process one insert oplog", async function() {
			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await db.collection("c1").insertOne({
				c: "City 4",
				m: 10000,
				f: 20000
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, ...rest0} = await db.collection("olap_c1_main_cube").find({"d.city": "City 0"}).next();
			assert.deepEqual(rest0, {
				count: 1,
				d: {city: "City 0"},
				m: {males: 1, females: 2}
			});
			let {_id: _id1, ...rest1} = await db.collection("olap_c1_main_cube").find({"d.city": "City 1"}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {city: "City 1"},
				m: {males: 10, females: 20}
			});
			let {_id: _id2, ...rest2} = await db.collection("olap_c1_main_cube").find({"d.city": "City 2"}).next();
			assert.deepEqual(rest2, {
				count: 1,
				d: {city: "City 2"},
				m: {males: 100, females: 200}
			});
			let {_id: _id3, ...rest3} = await db.collection("olap_c1_main_cube").find({"d.city": "City 3"}).next();
			assert.deepEqual(rest3, {
				count: 1,
				d: {city: "City 3"},
				m: {males: 1000, females: 2000}
			});
			let {_id: _id4, ...rest4} = await db.collection("olap_c1_main_cube").find({"d.city": "City 4"}).next();
			assert.deepEqual(rest4, {
				count: 1,
				d: {city: "City 4"},
				m: {males: 10000, females: 20000}
			});
		});

		it("should process one update oplog", async function() {
			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await db.collection("c1").updateOne({c: "City 3"}, {
				$set: {
					m: 10000,
					f: 20000
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, ...rest0} = await db.collection("olap_c1_main_cube").find({"d.city": "City 0"}).next();
			assert.deepEqual(rest0, {
				count: 1,
				d: {city: "City 0"},
				m: {males: 1, females: 2}
			});
			let {_id: _id1, ...rest1} = await db.collection("olap_c1_main_cube").find({"d.city": "City 1"}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {city: "City 1"},
				m: {males: 10, females: 20}
			});
			let {_id: _id2, ...rest2} = await db.collection("olap_c1_main_cube").find({"d.city": "City 2"}).next();
			assert.deepEqual(rest2, {
				count: 1,
				d: {city: "City 2"},
				m: {males: 100, females: 200}
			});
			let {_id: _id3, ...rest3} = await db.collection("olap_c1_main_cube").find({"d.city": "City 3"}).next();
			assert.deepEqual(rest3, {
				count: 1,
				d: {city: "City 3"},
				m: {males: 10000, females: 20000}
			});
		});

		it("should process one delete oplog", async function() {
			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await db.collection("c1").deleteOne({c: "City 3"});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, ...rest0} = await db.collection("olap_c1_main_cube").find({"d.city": "City 0"}).next();
			assert.deepEqual(rest0, {
				count: 1,
				d: {city: "City 0"},
				m: {males: 1, females: 2}
			});
			let {_id: _id1, ...rest1} = await db.collection("olap_c1_main_cube").find({"d.city": "City 1"}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {city: "City 1"},
				m: {males: 10, females: 20}
			});
			let {_id: _id2, ...rest2} = await db.collection("olap_c1_main_cube").find({"d.city": "City 2"}).next();
			assert.deepEqual(rest2, {
				count: 1,
				d: {city: "City 2"},
				m: {males: 100, females: 200}
			});
			assert.equal(await db.collection("olap_c1_main_cube").find({"d.city": "City 3"}).count(), 0);
		});

		it("should process all types of oplogs at once", async function() {
			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await db.collection("c1").insertOne({
				c: "City 4",
				m: 10000,
				f: 20000
			});
			await db.collection("c1").updateOne({c: "City 4"}, {
				$set: {
					m: 100000,
					f: 200000
				}
			});
			await db.collection("c1").deleteOne({c: "City 4"});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, ...rest0} = await db.collection("olap_c1_main_cube").find({"d.city": "City 0"}).next();
			assert.deepEqual(rest0, {
				count: 1,
				d: {city: "City 0"},
				m: {males: 1, females: 2}
			});
			let {_id: _id1, ...rest1} = await db.collection("olap_c1_main_cube").find({"d.city": "City 1"}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {city: "City 1"},
				m: {males: 10, females: 20}
			});
			let {_id: _id2, ...rest2} = await db.collection("olap_c1_main_cube").find({"d.city": "City 2"}).next();
			assert.deepEqual(rest2, {
				count: 1,
				d: {city: "City 2"},
				m: {males: 100, females: 200}
			});
			let {_id: _id3, ...rest3} = await db.collection("olap_c1_main_cube").find({"d.city": "City 3"}).next();
			assert.deepEqual(rest3, {
				count: 1,
				d: {city: "City 3"},
				m: {males: 1000, females: 2000}
			});
		});

		it("should process array dimension inserts", async function() {
			await db.collection("c2").insertMany([
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "1"}
						]
					},
					m1: 1
				},
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "1"}
						]
					},
					m1: 10
				}
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1.d2[].d3",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1",
						id: "mea"
					}
				]
			}});

			await db.collection("c2").insertOne({
				name: "test",
				d1: {
					d2: [
						{d3: "1"},
						{d3: "1"}
					]
				},
				m1: 100
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 6,
				d: {dim: "1"},
				m: {mea: 222}
			});
		});

		it("should process array dimension updates", async function() {
			await db.collection("c2").insertMany([
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "1"}
						]
					},
					m1: 1
				},
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "1"}
						]
					},
					m1: 10
				}
			]);

			await db.collection("c2").insertOne({
				name: "test",
				d1: {
					d2: [
						{d3: "1"},
						{d3: "1"}
					]
				},
				m1: 100
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1.d2[].d3",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1",
						id: "mea"
					}
				]
			}});

			await db.collection("c2").updateOne({name: "test"}, {$push: {"d1.d2": {d3: "1"}}});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 7,
				d: {dim: "1"},
				m: {mea: 322}
			});
		});

		it("should process array dimension deletes", async function() { // TODO sometimes random error - no operations specified?
			await db.collection("c2").insertMany([
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "1"}
						]
					},
					m1: 1
				},
				{
					d1: {
						d2: [
							{d3: "1"},
							{d3: "1"}
						]
					},
					m1: 10
				}
			]);

			await db.collection("c2").insertOne({
				name: "test",
				d1: {
					d2: [
						{d3: "1"},
						{d3: "1"}
					]
				},
				m1: 100
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1.d2[].d3",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1",
						id: "mea"
					}
				]
			}});

			await db.collection("c2").deleteOne({name: "test"});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 4,
				d: {dim: "1"},
				m: {mea: 22}
			});
		});

		it("should process array measure inserts", async function() {
			await db.collection("c2").insertMany([
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 1},
							{m3: 2}
						]
					}
				},
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 10},
							{m3: 20}
						]
					}
				},
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1.m2[].m3",
						id: "mea"
					}
				]
			}});

			await db.collection("c2").insertOne({
				name: "test",
				d1: "1",
				m1: {
					m2: [
						{m3: 100},
						{m3: 200}
					]
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 6,
				d: {dim: "1"},
				m: {mea: 333}
			});
		});

		it("should process array measure updates", async function() {
			await db.collection("c2").insertMany([
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 1},
							{m3: 2}
						]
					}
				},
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 10},
							{m3: 20}
						]
					}
				},
			]);

			await db.collection("c2").insertOne({
				name: "test",
				d1: "1",
				m1: {
					m2: [
						{m3: 100},
						{m3: 200}
					]
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1.m2[].m3",
						id: "mea"
					}
				]
			}});

			await db.collection("c2").updateOne({name: "test"}, {$push: {"m1.m2": {m3: 400}}});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 7,
				d: {dim: "1"},
				m: {mea: 733}
			});
		});

		it("should process array measure deletes", async function() {
			await db.collection("c2").insertMany([
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 1},
							{m3: 2}
						]
					}
				},
				{
					d1: "1",
					m1: {
						m2: [
							{m3: 10},
							{m3: 20}
						]
					}
				},
			]);

			await db.collection("c2").insertOne({
				name: "test",
				d1: "1",
				m1: {
					m2: [
						{m3: 100},
						{m3: 200}
					]
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "d1",
						id: "dim"
					}
				],
				measures: [
					{
						path: "m1.m2[].m3",
						id: "mea"
					}
				]
			}});

			await db.collection("c2").deleteOne({name: "test"});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 4,
				d: {dim: "1"},
				m: {mea: 33}
			});
		});

		it("should process time dimension inserts", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "hour"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			await db.collection("c3").insertOne({
				timestamp: 1589290200000, // 05/12/2020, tuesday, 13:30
				processLength: 20
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, d: d0, ...rest0} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589011200000)}).next();
			assert.deepEqual(rest0, {
				count: 1,
				m: {procLength: 1}
			});
			let {_id: _id1, d: d1, ...rest1} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589202000000)}).next();
			assert.deepEqual(rest1, {
				count: 1,
				m: {procLength: 2}
			});
			let {_id: _id2, d: d2, ...rest2} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589288400000)}).next();
			assert.deepEqual(rest2, {
				count: 2,
				m: {procLength: 30}
			});
			let {_id: _id3, d: d3, ...rest3} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589806800000)}).next();
			assert.deepEqual(rest3, {
				count: 1,
				m: {procLength: 100}
			});
		});

		it("should process time dimension updates", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await db.collection("c3").insertOne({
				timestamp: 1589290200000, // 05/12/2020, tuesday, 13:30
				processLength: 20
			});

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "hour"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			await db.collection("c3").updateOne({timestamp: 1589290200000}, { // 05/12/2020, tuesday, 13:30
				$set: {timestamp: 1589293800000}, // 05/12/2020, tuesday, 14:30
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, d: d0, ...rest0} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589011200000)}).next();
			assert.deepEqual(rest0, {
				count: 1,
				m: {procLength: 1}
			});
			let {_id: _id1, d: d1, ...rest1} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589202000000)}).next();
			assert.deepEqual(rest1, {
				count: 1,
				m: {procLength: 2}
			});
			let {_id: _id2, d: d2, ...rest2} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589288400000)}).next();
			assert.deepEqual(rest2, {
				count: 1,
				m: {procLength: 10}
			});
			let {_id: _id3, d: d3, ...rest3} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589806800000)}).next();
			assert.deepEqual(rest3, {
				count: 1,
				m: {procLength: 100}
			});
			let {_id: _id4, d: d4, ...rest4} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589292000000)}).next();
			assert.deepEqual(rest4, {
				count: 1,
				m: {procLength: 20}
			});
		});

		it("should process time dimension deletes", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await db.collection("c3").insertOne({
				timestamp: 1589290200000, // 05/12/2020, tuesday, 13:30
				processLength: 20
			});

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "hour"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			await db.collection("c3").deleteOne({
				timestamp: 1589290200000 // 05/12/2020, tuesday, 13:30
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, d: d0, ...rest0} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589011200000)}).next();
			assert.deepEqual(rest0, {
				count: 1,
				m: {procLength: 1}
			});
			let {_id: _id1, d: d1, ...rest1} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589202000000)}).next();
			assert.deepEqual(rest1, {
				count: 1,
				m: {procLength: 2}
			});
			let {_id: _id2, d: d2, ...rest2} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589288400000)}).next();
			assert.deepEqual(rest2, {
				count: 1,
				m: {procLength: 10}
			});
			let {_id: _id3, d: d3, ...rest3} = await db.collection("olap_c3_main_cube").find({"d.ts": new Date(1589806800000)}).next();
			assert.deepEqual(rest3, {
				count: 1,
				m: {procLength: 100}
			});
		});
	});

	describe("#updateAggregates() missing fields", async function() {
		it("array dimension inserts", async function() {
			await db.collection("c2").insertMany([
				{
					tags: ["1", "1"],
					val: 1
				},
				{
					tags: ["1", "1"],
					val: 10
				}
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c2",
				dimensions: [
					{
						path: "tags[]",
						id: "tag"
					}
				],
				measures: [
					{
						path: "val",
						id: "value"
					}
				]
			}});

			await db.collection("c2").insertOne({
				val: 100
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_main_cube").find({}).count(), 1);

			let {_id, ...rest} = await db.collection("olap_c2_main_cube").find({}).next();
			assert.deepEqual(rest, {
				count: 4,
				d: {tag: "1"},
				m: {value: 22}
			});
		});

		it("array dimension updates", async function() {
			await db.collection("c2").insertOne({
				meta: {
					br: {
						tags: ["tag1"]
					}
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					}
				],
				measures: []
			}});

			await olap.createCube({name: "cd", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.cd.tags[]",
						id: "tag"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.cd": {
						tags: ["tag1"]
					}
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_cube").find({}).next();
			assert.deepEqual(rest0, {
				count: 1,
				d: {tag: "tag1"}
			});

			let {_id: _id1, ...rest1} = await db.collection("olap_c2_cd_cube").find({}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {tag: "tag1"}
			});

			assert.equal(await db.collection("olap_c2_cd_shadow").find({}).count(), 1);
			let {_id: _id2, ...rest2} = await db.collection("olap_c2_cd_shadow").find({}).next();
			assert.deepEqual(rest2, {
				d: {tag: ["tag1"]}
			});
		});

		it("array dimension none+some", async function() {
			await db.collection("c2").insertOne({
				meta: {
					field1: 20,
					field2: {a: "b"}
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					},
					{
						path: "meta.br.completed",
						id: "done"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.tags": ["tag1"]
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_br_shadow").find({}).count(), 1);
			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_shadow").find({}).next();
			assert.deepEqual(rest0, {
				d: {tag: ["tag1"]}
			});

			let {_id: _id1, ...rest1} = await db.collection("olap_c2_br_cube").find({}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {tag: "tag1", done: "__null__"}
			});
		});

		it("array dimension none+all", async function() {
			await db.collection("c2").insertOne({
				meta: {
					field1: 20,
					field2: {a: "b"}
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					},
					{
						path: "meta.br.completed",
						id: "done"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.tags": ["tag1"],
					"meta.br.completed": true
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_br_shadow").find({}).count(), 1);
			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_shadow").find({}).next();
			assert.deepEqual(rest0, {
				d: {tag: ["tag1"], done: true}
			});

			let {_id: _id1, ...rest1} = await db.collection("olap_c2_br_cube").find({}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {tag: "tag1", done: true}
			});
		});

		it("array dimension some+some -> all", async function() {
			await db.collection("c2").insertOne({
				meta: {
					field1: 20,
					field2: {a: "b"}
				}
			});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.tags": ["tag1"],
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					},
					{
						path: "meta.br.completed",
						id: "done"
					},
					{
						path: "meta.br.name",
						id: "name"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.completed": true,
					"meta.br.name": "jerry"
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_br_shadow").find({}).count(), 1);
			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_shadow").find({}).next();
			assert.deepEqual(rest0, {
				d: {tag: ["tag1"], done: true, name: "jerry"}
			});

			let {_id: _id1, ...rest1} = await db.collection("olap_c2_br_cube").find({}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {tag: "tag1", done: true, name: "jerry"}
			});
		});

		it("array dimension some+some -/> all", async function() {
			await db.collection("c2").insertOne({
				meta: {
					field1: 20,
					field2: {a: "b"}
				}
			});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.tags": ["tag1"],
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					},
					{
						path: "meta.br.completed",
						id: "done"
					},
					{
						path: "meta.br.name",
						id: "name"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.completed": true
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_br_shadow").find({}).count(), 1);
			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_shadow").find({}).next();
			assert.deepEqual(rest0, {
				d: {tag: ["tag1"], done: true}
			});

			let {_id: _id1, ...rest1} = await db.collection("olap_c2_br_cube").find({}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {tag: "tag1", done: true, name: "__null__"}
			});
		});

		it("array dimension all-some", async function() {
			await db.collection("c2").insertOne({
				meta: {
					field1: 20,
					field2: {a: "b"}
				}
			});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.tags": ["tag1"],
					"meta.br.completed": true
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					},
					{
						path: "meta.br.completed",
						id: "done"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$unset: {
					"meta.br.completed": 1,
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_br_shadow").find({}).count(), 1);
			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_shadow").find({}).next();
			assert.deepEqual(rest0, {
				d: {tag: ["tag1"]}
			});

			let {_id: _id1, ...rest1} = await db.collection("olap_c2_br_cube").find({}).next();
			assert.deepEqual(rest1, {
				count: 1,
				d: {tag: "tag1", done: "__null__"}
			});
		});

		it("array dimension all-some (array dim, cube empty)", async function() {
			await db.collection("c2").insertOne({
				meta: {
					field1: 20,
					field2: {a: "b"}
				}
			});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.tags": ["tag1"],
					"meta.br.completed": true
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					},
					{
						path: "meta.br.completed",
						id: "done"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$unset: {
					"meta.br.tags": 1,
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_br_shadow").find({}).count(), 1);
			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_shadow").find({}).next();
			assert.deepEqual(rest0, {
				d: {done: true}
			});

			assert.equal(await db.collection("olap_c2_br_cube").find({}).count(), 0);
		});

		it("array dimension all-all", async function() {
			await db.collection("c2").insertOne({
				meta: {
					field1: 20,
					field2: {a: "b"}
				}
			});

			await db.collection("c2").updateOne({}, {
				$set: {
					"meta.br.tags": ["tag1"],
					"meta.br.completed": true
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "br", model: {
				source: "c2",
				dimensions: [
					{
						path: "meta.br.tags[]",
						id: "tag"
					},
					{
						path: "meta.br.completed",
						id: "done"
					}
				],
				measures: []
			}});

			await db.collection("c2").updateOne({}, {
				$unset: {
					"meta.br": 1
				}
			});

			await new Promise(res => setTimeout(res, 100));

			await olap.updateAggregates();

			assert.equal(await db.collection("olap_c2_br_shadow").find({}).count(), 1);
			let {_id: _id0, ...rest0} = await db.collection("olap_c2_br_shadow").find({}).next();
			assert.deepEqual(rest0, {d: {}});

			assert.equal(await db.collection("olap_c2_br_cube").find({}).count(), 0);
		});
	});

	describe("Oplog buffering", async function() {
		it("should not leave trailing listeners", async function() {
			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await olap.startOplogBuffering();

			await new Promise(res => setTimeout(res, 100));

			await olap.stopOplogBuffering({});
		});

		it("should buffer oplogs for existing cubes", async function() {
			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
					measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await olap.startOplogBuffering();

			await new Promise(res => setTimeout(res, 100));

			olap.oplogBuffer = []; // due to imprecise Timestamps, the buffer may contain more oplogs from previous operations, but the system's idempotency allows that

			await db.collection("c1").deleteOne({c: "City 3"});

			await new Promise(res => setTimeout(res, 100));

			assert.equal(olap.oplogBuffer.length, 1);

			let {ts, o, o2, ...rest} = olap.oplogBuffer[0];
			assert.deepEqual(rest, {
				ns: "db1.c1"
			});
			assert(o._id !== undefined);

			await olap.stopOplogBuffering({});
		});

		it("should buffer oplogs for new cubes", async function() {
			await olap.startOplogBuffering();

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			await new Promise(res => setTimeout(res, 100));

			olap.oplogBuffer = []; // due to imprecise Timestamps, the buffer may contain more oplogs from previous operations, but the system's idempotency allows that

			await db.collection("c1").deleteOne({c: "City 3"});

			await new Promise(res => setTimeout(res, 100));

			assert.equal(olap.oplogBuffer.length, 1);

			let {ts, o, o2, ...rest} = olap.oplogBuffer[0];
			assert.deepEqual(rest, {
				ns: "db1.c1"
			});
			assert(o._id !== undefined);

			await olap.stopOplogBuffering({});
		});
	});

	describe("#getAggregates()", function() {
		it("should throw Error when extraneous cube is specified", async function() {
			await assert.rejects(async () => await olap.aggregate(), "Error: OLAP::getAggregates: no cube for undefined");
		});

		it("should return proper aggregates", async function() {
			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c1",
				dimensions: [
					{
						path: "c",
						id: "city"
					}
				],
				measures: [
					{
						path: "m",
						id: "males"
					},
					{
						path: "f",
						id: "females"
					}
				]
			}});

			let res = await olap.aggregate({
				colName: "c1",
				cubeName: "main",
				measures: ["males", "females"],
				dimensions: [/*{dimension: "city"}*/]
			});

			assert.equal(JSON.stringify(res), JSON.stringify([{
				"_count": 4,
				m: {
					"males": 1111,
					"females": 2222
				}
			}]));
		});

		it("should handle time axis aggregation", async function() {
			await db.collection("c3").insertMany([
				{
					timestamp: 1589011200000, // 05/09/2020, saturday, 08:00
					processLength: 1
				},
				{
					timestamp: 1589202000000, // 05/11/2020, monday, 13:00
					processLength: 2
				},
				{
					timestamp: 1589288400000, // 05/12/2020, tuesday, 13:00
					processLength: 10
				},
				{
					timestamp: 1589290200000, //  05/12/2020, tuesday, 13:30
					processLength: 20
				},
				{
					timestamp: 1589806800000, // 05/18/2020, monday, 13:00
					processLength: 100
				}
			]);

			await new Promise(res => setTimeout(res, 100));

			await olap.createCube({name: "main", model: {
				source: "c3",
				dimensions: [
					{
						path: "timestamp",
						id: "ts",
						type: "time",
						granularity: "hour"
					}
				],
				measures: [
					{
						path: "processLength",
						id: "procLength"
					}
				]
			}});

			let res = await olap.aggregate({
				colName: "c3",
				cubeName: "main",
				measures: ["procLength"],
				dimensions: [
					{
						id: "ts",
						granularity: "hour"
					}
				],
				filters: {"ts": {$range: {from: 1589288399999, to: 1589290200001}}}
			});

			assert.equal(JSON.stringify(res), JSON.stringify([{
				"_count": 2,
				m: {
					"procLength": 30
				},
				d: {
					"ts": 1589288400000
				}
			}]));
		});
	});
});
