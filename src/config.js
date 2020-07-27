let fs = require("fs");

const removeEmpty = (obj) => {
	Object.keys(obj).forEach(key => {
		if (obj[key] && typeof obj[key] === "object") {
			removeEmpty(obj[key]);
			if (Object.keys(obj[key]).length === 0) delete obj[key];
		} else if (obj[key] === undefined) delete obj[key];
	});
};

let config = {
	mongo: {
		url:					 process.env.DB_URL						|| "mongodb://localhost:27017/",
		dbName:					 process.env.DB_NAME					|| "db1",
		retryInterval:			+process.env.DB_RETRY_INTERVAL			|| 1000
	},
	nats: {
		connection: {
			url:				 process.env.NATS_URL					|| "nats://localhost:4222/",
			user:				 process.env.NATS_AUTH_USER				|| undefined,
			pass:				 process.env.NATS_AUTH_PASS				|| undefined,
			token:				 process.env.NATS_AUTH_TOKEN			|| undefined,
			tls: {
				ca:				 process.env.NATS_TLS_CA				|| undefined,
				key:			 process.env.NATS_TLS_KEY				|| undefined,
				cert:			 process.env.NATS_TLS_CERT				|| undefined,
				servername:		 process.env.NATS_TLS_SERVERNAME		|| undefined,
			},
			pingInterval:		+process.env.NATS_PING_INTERVAL			|| 10000,
		},
		prefix:					 process.env.NATS_PREFIX				|| "olap"
	},
	logger: {
		level:					 process.env.LOG_LEVEL					|| "info",
		dest:					 process.env.LOG_DEST					|| "console",
		file:					 process.env.LOG_FILEPATH				|| "mongo-olap.log"
	}
};

removeEmpty(config);

if (config.nats.tls) {
	Object.entries(config.nats.tls).forEach(([key, val]) => {
		config.nats.tls[key] = [fs.readFileSync(val)];
	});
}

module.exports = config;
