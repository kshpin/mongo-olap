let Ajv = require("ajv");
let ajv = new Ajv();

class InvalidRequestError extends Error {
	constructor(errors) {
		super();
		this.message = "invalid request";
		this.errors = errors;
	}
}

const compiledSchemas = new Map();

function validateSchema(schema, obj) {
	let validate;

	if (compiledSchemas.has(schema)) validate = compiledSchemas.get(schema);
	else {
		validate = ajv.compile(schema);
		compiledSchemas.set(schema, validate);
	}

	if (!validate(obj)) throw new InvalidRequestError(validate.errors);
}

module.exports = {InvalidRequestError, validateSchema};
