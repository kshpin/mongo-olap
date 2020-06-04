let Cube = require("../src/Cube");
let assert = require("assert");

describe("Cube", function() {
	describe("#initNewCube()", function() {
		it("should initialize a new cube collection", async function() {
			let db = {
				collection: () => ({
					aggregate: () => ({
						toArray: async() => []
					}),
					insertOne: () => {},
					createIndex: () => {}
				})
			};
		});

		//
	});
});

