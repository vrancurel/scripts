const MongoDB = require('mongodb');
const LC = require('./lib/storage/metadata/mongoclient/LogConsumer.js');

const mongoConfig = {
       "replicaSetHosts": "localhost:27018,localhost:27019,localhost:27020",
       "writeConcern": "majority",
       "replicaSet": "rs0",
       "readPreference": "primary",
       "database": "metadata"
};

function doProcessEntries(l, cb) {
    var count = 0;
    l.log.on('data', rec => {
        //console.log(rec);
        count++;
    });
    l.log.on('error', err => {
        console.log('err', err);
        process.exit(1);
    });
    l.log.on('end', () => {
        console.log(count);
    });
    l.log.on('info', info => {
        cb(null, info);
    });
}

function doReadRecords(params, cb) {
    lc.readRecords(params, (err, l) => {
        if (err) {
            console.log(err);
            process.exit(1);
        }
        doProcessEntries(l, cb);
    });
}

function doProcess(params) {
    doReadRecords(params, (err, info) => {
        console.log(info);
        setTimeout(() => {
            console.log('timeout');
            params.startSeq = info.end;
            doProcess(params);
        }, 5000);
    });
}

const lc = new LC(mongoConfig, console);

lc.connectMongo(() => {
    console.log('connected');
    const params = {};
    params.limit = 10000;
    var ts = new MongoDB.Timestamp(0, 0);
    var tsSpec = {"ts": ts};
    params.startSeq = JSON.stringify(tsSpec);
    console.log(params);
    doProcess(params);
});
