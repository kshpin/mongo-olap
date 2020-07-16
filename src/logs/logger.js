const bunyan = require("bunyan");
const config = require("./../config");
const log = bunyan.createLogger({
	name: "olap",
	level: config.logger.level
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
