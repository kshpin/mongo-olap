module.exports = {
	mongo: {
		url: process.env.DB_URL || "mongodb://localhost:27017/",
		dbName: process.env.DB_NAME || "db1",
		retryInterval: process.env.DB_RETRY_INTERVAL || 1000
	},
	nats: {
		url: process.env.NATS_URL || "nats://localhost:4222/",
		pingInterval: process.env.NATS_PING_INTERVAL || 10000,
		prefix: process.env.NATS_PREFIX || "main"
	},
	logger: {
		level: process.env.LOGGER_LEVEL || "info"
	}
};
