const bunyan = require("bunyan");
const config = require("./../config");

let stream;
switch (config.logger.dest) {
	case "file":
		stream = {
			path: config.logger.file
		};
		break;

	default:
		// give some error message or something
	case "console":
		stream = {
			stream: process.stdout
		};
}

const LEVELS = ["fatal", "error", "warn", "info", "debug", "trace"];
if (LEVELS.includes(config.logger.level.toLowerCase())) stream.level = config.logger.level.toLowerCase();
else stream.level = "info";

const log = bunyan.createLogger({
	name: "mongo-olap",
	streams: [stream]
});

/*
    name: <string>,                     // Required
    level: <level name or number>,      // Optional, see "Levels" section
    stream: <node.js stream>,           // Optional, see "Streams" section
    streams: [<bunyan streams>, ...],   // Optional, see "Streams" section
    serializers: <serializers mapping>, // Optional, see "Serializers" section
    src: <boolean>,                     // Optional, see "src" section

    // Any other fields are added to all log records as is.
    foo: 'bar',
*/

module.exports = log;
