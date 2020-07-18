# Cube
A MongoDB OLAP microservice for continuous pre-aggregation.

By defining a particular model for future aggregation requests, a lot of the information in the database may be aggregated to its maximum without losing information in terms of the model. This may speed up the aggregation process by several orders of magnitude, for a well chosen model.

This microservice relies on MongoDB's replication set setting, which allows using its `oplogs` to keep track of changes to the database, and thus keep the pre-aggregates up to date. To enable this, run `rs.initiate()` in MongoDB's shell.

[NATS](https://nats.io/) is the only currently supported interface to the OLAP service. Both it and MongoDB must be running, with their respective connection information in the following environment variables (defaults in comments):
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
      path: "accountId", // the location in the documents at which to look for the values
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
Refer to https://olap.com/learn-bi-olap/olap-bi-definitions/ for term definitions (in particular, dimensions and measures).

## API
// TODO give more examples of embedding

Embedding this module into your own project can be done by excluding `OLAPService.js`, and simply calling the public methods of `OLAP`.
```javascript
let olap = new OLAP(mongoClient, db, "olap_state", "olap_config");
// use olap as needed
```

NATS API (all parameters are single stringified JSON objects):
// TODO turn into table
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
Let's first create a Cube. The source collection stores information about website visits.
```javascript
nc.publish("olap_createCube", {
  name: "siteVisits",
  model: {
    source: "db1.col1", // this is the collection from which information is taken
    dimensions: [
      {
        path: "locale",
        id: "locale"
      },
      {
        path: "visits[].startTime", // visits is an array, each element of which has a "startTime" field
        id: "startTime",
        type: "time",
        timeFormat: "ms",
        granularity: "day" // aggregation will be available by year, month, and day, but not hour
      } // include only the dimensions you'll use, to optimize the aggregation time and cube storage space
    ],
    measures: [
      {
        path: "visits[].duration",
        id: "duration" 
      } // there is always an implicit measure "_count", giving the number of documents counted
    ]
  },
  principalEntity: "visit" // this doesn't impact aggregation, but keeping this in mind when choosing a model is important for coherency of aggregation results
});
```
Cube creation is the most computationally expensive step in the whole process, expect for it to take some time. Now the system is tracking changes to the source collection, but each update will take a long time if the changes aren't buffered.
```javascript
nc.publish("olap_startOplogBuffering", {});
```
Each time an aggregation request is received, the changes are processed automatically. However, it's a good idea to enable periodic updates to reduce response time when you need it most.
```javascript
nc.publish("olap_startAutoUpdate", {
  interval: 30000 // milliseconds
});
```
Now we can make an aggregation request.
```javascript
nc.publish("olap_aggregate", {
  colName: "col1",
  cubeName: "siteVisits", // the same collection may have several cubes optimized for different models
  dimensions: [ // include all the dimensions that need to be differentiated, all others will be collapsed
    {
      id: "locale"
    }
  ],
  measures: [ // include all the measures that need to be summed up, all others will be ignored
    {
      id: "duration"
    }
  ],
  filters: {
    startTime: {
      $range: { // only for dimensions marked {type: "time"}
        from: Date.now() - 31*24*60*60*1000 // include only visits in the last month
        // "from" and "to" fields optional
      }
    }
  }
}, inbox); // NATS inbox to listen for the response
```
This will return how many visits the site had, and the total visit time, by locale, in the last month.

We can also aggregate by the time field:
```javascript
nc.publish("olap_aggregate", {
  colName: "col1",
  cubeName: "siteVisits",
  dimensions: [
    {
      id: "startTime",
      granularity: "month" // since the model specified "day", "month" is allowed
    }
  ]
  // if we only want the number of visits, no need to include any measures as that one is included always
}, inbox); // NATS inbox to listen for the response
```
This will return how many visits the side had, irrespective of locale, by month.
