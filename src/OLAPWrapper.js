const logger = require("./logs/logger").child({
	module: "OLAPWrapper"
});

class OLAPWrapper {

	olap;

	processingRequests;

	constructor(olap) {
		this.olap = olap;
		this.processingRequests = 0;
	}

	async call(funcName, ...args) {
		try {
			this.processingRequests++;
			return await this.olap[funcName](...args);
		} finally {
			this.processingRequests--;
			this.olap.finishEmitter.emit("done");
		}
	}

	async cleanUp() {
		let log = logger.child({
			func: "cleanUp"
		});

		if (this.olap) {
			this.olap.stopAutoUpdate({exiting: true});

			if (this.processingRequests !== 0 || this.olap.currentlyUpdating) log.debug({stage: "waiting for jobs to complete"});
			while (this.processingRequests !== 0 || this.olap.currentlyUpdating) {
				await new Promise(res => this.olap.finishEmitter.once("done", res));

				log.trace({
					event: "job completed",
					requestsLeft: this.processingRequests,
					currentlyUpdatingAggregates: this.olap.currentlyUpdating
				});
			}

			await this.olap.stopOplogBuffering({exiting: true});
			log.debug({event: "oplog buffering stopped"});
		}
	}
}

module.exports = OLAPWrapper;
