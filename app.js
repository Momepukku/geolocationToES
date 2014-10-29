var fs = require('fs');
var csv = require('fast-csv');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

if(!process.argv[2]) {
    process.stdout.write('please specify csv file name as argument\n');
    process.exit(0);
}

client.ping({
  // ping usually has a 100ms timeout
  requestTimeout: 1000,

  // undocumented params are appended to the query string
  hello: 'elasticsearch!'
}, function(error) {
  if (error) {
    console.trace('elasticsearch cluster is down!');
  } else {
    console.log('All is well');
    setup(importData);
  }
});

function readCSVFile(filenName, onRecord) {
    var stream = fs.createReadStream(filenName);

    csv
     .fromStream(stream, {ignoreEmpty: true, headers: true})
     .on("record", function(data){
        onRecord(data);
     })
     .on("end", function(count){
        console.log("done read file (" + count + ")");
     });
}

function setup(callback) {
    //set mapping
    client.indices.putMapping(
        {
            index: 'test',
            type: 'pin',
            body: {
                'pin' : {
                    'properties' : {
                        'location' : {
                            'type' : 'geo_shape',
                            'tree': 'quadtree',
                            'precision': '1m'
                        }
                    }
                }
            }
        },
        function(err, result) {
            if(err)
                traceError(err);
            else
                callback();
        }
    );
}

function importData() {
    console.log('reading csv file...');
    readCSVFile(
        process.argv[2], 
        function(data){
            var data = csvDataToCoordinates(data);
            client.create(
                {
                    index: 'test',
                    type: 'pin',
                    body: data,
                },
                function(err, resp) {
                    if(err){
                        console.log('fail on ', data);
                        traceError(err);
                    }
                    else{
                        console.log('imported: ', data.name);
                    }
                }
            );
        }
    );
}

function csvDataToCoordinates(data) {
    return {
        'name': data.name,
        'location': JSON.parse(data.location)
    }
}

function traceError(err) {
    console.trace(err.message);
}
