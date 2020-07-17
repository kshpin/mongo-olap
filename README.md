# Cube
A MongoDB OLAP microservice for continuous pre-aggregation.

By defining a particular model for future aggregation requests, a lot of the information in the database may be aggregated to its maximum without losing information in terms of the model. This may speed up the aggregation process by several orders of magnitude, for a well chosen model.

This microservice relies on MongoDB's replication set setting, which allows using its `oplogs` to keep track of changes to the database, and thus keep the pre-aggregates up to date. To enable this, run `rs.initiate()` in MongoDB's shell.

NATS is the only currently supported interface to the OLAP service. Both it and MongoDB must be running, with their respective connection information in the following environment variables (defaults in comments):
```javascript
// mongo
process.env.DB_URL // "mongodb://localhost:27017/",
process.env.DB_NAME // "db1",
process.env.DB_RETRY_INTERVAL // 1000

// nats
process.env.NATS_URL // "nats://localhost:4222/",
process.env.NATS_PING_INTERVAL // 10000

// logger
process.env.LOGGER_LEVEL // "info"
```

## Model
The cube model is described in the following way:
```javascript
model: {
  source: "sourceDatabase.sourceCollection",
  dimensions: [
    {
      path: "accountId", // the location in the document in which to look for the values
      id: "accID" // an alias for the location, used in aggregation
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
      timeFormat: "ms" // the value is expected to represent the number of milliseconds since 1 January 1970 UTC
      granularity: "hour" // since time values are usually unique, a granularity is necessary for effective aggregation
                          // possible values: "hour", "day", "month", "year"
    }
  ],
  measures: [
    {
      path: "execTime", // described in the same way as dimensions, must point to a numerical value
      id: "execTime"
    }
  ]
}
```

## API
Embedding this module into your own project can be done by excluding `OLAPService.js`, and simply calling the public methods of `OLAP`.
```javascript
let olap = new OLAP(mongoClient, db, "olap_state", "olap_config");
// use olap as needed
```

NATS API (all parameters are single stringified JSON objects):
```javascript
"olap_createCube" // creates a cube
parameters: {
  name, // name of the cube
  model, // cube model (elaborated on below)
  principalEntity // the logical entity representing what the cube is aggregating
}

"olap_loadCubes" // loads cubes from configuration
parameters: {}

"olap_listCubes" // lists loaded cubes
parameters: {}

"olap_deleteCube" // deletes a cube
parameters: {
  colName, // name of the collection on which the cube is based
  cubeName // name of the cube to delete
}

"olap_startAutoUpdate" // begins auto updating the aggregates at an interval (off by default)
parameters: {
  interval // number of milliseconds between updates (default 30000)
}

"olap_stopAutoUpdate" // stops auto updating
parameters: {}

"olap_startOplogBuffering" // begins buffering oplogs, speeding up the update process (off by default, but highly recommended)
parameters: {}

"olap_stopOplogBuffering" // stops buffering oplogs
parameters: {}

"olap_updateAggregates" // updates aggregates once
parameters: {}

"olap_aggregate" // aggregates the cube
parameters: {
  colName, // name of collection on which the cube is based
  cubeName, // name of cube
  measures, // measures to include in aggregation
  dimensions, // dimensions to keep separate in aggregation
  filters // filters for including documents in aggregation
}
```

## Usage examples
// TODO finish examples
