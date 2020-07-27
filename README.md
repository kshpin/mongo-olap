# Mongo-OLAP
An OLAP service for MongoDB with low aggregation times.

By defining a particular model for future aggregation requests, a lot of the information in the database may be aggregated to its maximum without losing information in terms of the model. This may speed up the aggregation process by several orders of magnitude, for a well chosen model. The only currently supported aggregation operation is `sum`, but more are on the way (`product`, `standardDeviation`, `min`, `max`).

This service relies on MongoDB's replication set setting, which allows using its `oplogs` to keep track of changes to the database, and thus keep the pre-aggregates up to date. To enable this, run `rs.initiate()` in MongoDB's shell.

[NATS](https://nats.io/) is the only currently supported interface to the OLAP service. Both it and MongoDB must be running, with their respective connection information in the following environment variables (defaults in comments):

| Environment variable | Default value | Meaning |
| --- | --- | --- |
| `DB_URL` | `mongodb://localhost:27017/` | The url through which to connect to MongoDB |
| `DB_NAME` | `db1` | The database name within MongoDB to use as the data source |
| `DB_RETRY_INTERVAL` | `1000` | Millisecond interval to try connecting to MongoDB again (only for first connection, disconnecting after a successful connection results in a fatal error) |
| `NATS_URL` | `nats://localhost:4222/` | The url through which to connect to NATS, including port |
| `NATS_USER` | | NATS username for authorization |
| `NATS_PASS` | | NATS password for authorization |
| `NATS_TOKEN` | | NATS token for authorization (mutually exclusive with user/pass) |
| `NATS_TLS_CA` | | NATS TLS CA file path |
| `NATS_TLS_KEY` | | NATS TLS key file path |
| `NATS_TLS_CERT` | | NATS TLS certificate file path |
| `NATS_TLS_SERVERNAME` | | NATS TLS server name |
| `NATS_PING_INTERVAL` | `10000` | Millisecond interval between checkAlive pings to NATS |
| `NATS_PREFIX` | `olap` | Prefix to the olap publish topic, to prevent collisions in case of multiple instances running<br>Format: `[prefix].[command]` |
| `LOG_LEVEL` | `info` | Logger level (possible options: `fatal`, `error`, `warn`, `info`, `debug`, `trace`) |
| `LOG_DEST` | `console` | Where the logs are to be printed (possible options: `console`, `file`) |
| `LOG_FILEPATH` | `mongo-olap.log` | If `LOG_DEST` is set to `file`, the filepath to which the logs are to be written |

MongoDB version: `3.5.5` or higher.
NATS version: `1.4.8` or higher.

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
      type: "time", // only used for "time"
      timeFormat: "ms", // the value is expected to represent the number of milliseconds since 1 January 1970 UTC
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

### NATS
All parameters are properties of an object in JSON format, stringified.

The main two requests are for creating a cube and aggregating it:
| Publish topic | Parameters |
| --- | --- |
| `"olap_createCube"`<br>creates a cube | `name` - name of the cube<br>`model` - cube model<br>`principalEntity` - the logical entity representing what the cube is aggregating |
| `"olap_aggregate"`<br>aggregates the cube | `colName` - name of collection on which the cube is based<br>`cubeName` - name of cube<br>`measures` - measures to include in aggregation<br>`dimensions` - dimensions to keep separate in aggregation<br>`filters` - filters for including documents in aggregation |

All other requests are optional and rarely used:
| Publish topic | Parameters |
| --- | --- |
| `"olap_loadCubes"`<br>loads cubes from configuration | |
| `"olap_listCubes"`<br>lists loaded cubes | |
| `"olap_deleteCube"`<br>deletes a cube | `colName` - name of the collection on which the cube is based<br>`cubeName` - name of the cube to delete |
| `"olap_startAutoUpdate"`<br>begins auto updating the aggregates at an interval (on by default) | `interval` - number of milliseconds between updates (default 30000) |
| `"olap_stopAutoUpdate"`<br>stops auto updating | |
| `"olap_startOplogBuffering"`<br>begins buffering oplogs, speeding up the update process (on by default) | |
| `"olap_stopOplogBuffering"`<br>stops buffering oplogs | |
| `"olap_updateAggregates"`<br>updates aggregates once | |

## Usage examples
First create a Cube. The source collection stores information about website visits.
```javascript
nc.publish("olap_main_createCube", {
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
Once the cube is created, we can already make aggregation requests:
```javascript
nc.publish("olap_main_aggregate", {
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
nc.publish("olap_main_aggregate", {
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

## License
MIT
