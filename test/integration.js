var async = require('async');
var should = require('should');
var backend = require('..');
var MongoClient = require('mongodb').MongoClient;
var MongoServer = require('mongodb').Server;
var EventEmitter = require('events').EventEmitter;

var mongoName = "integration_test";
var mongoConnection = "mongodb://localhost:27017/"+mongoName+"?w=1";

describe('Mongo Backend - Integration', function(){
  var tests = [
    {
      description: "should do the goddamn thing its supposed to",
      input: {
        "gauges": {
          "server": {
            "ABC": {
              "cpu": 111,
              "mem": 222
            },
            "ZYX": {
              "cpu": 333
            }
          }
        },
        "counters": {
          "abc": 333,
          "zxy": 444
        },
        "timers": {
          "abc": [0, 1]
        }
      },
      output: {
        "gauges.server_1": {},
        "counters.abc_1":  { "v": 333,
                              "rate": 333
                           },
        "counters.xyz_1":  {},
        "timers.abc_1":    {}
      }
    }
  ];

  // initialize backend
  var startupTime = new Date()
    , config = {mongoName: mongoName, mongoPrefix: false, flushInterval: 1000}
    , backendEvents = new EventEmitter()
    , state = backend.init(startupTime, config, backendEvents);

  state.should.eql(true, 'Backend init failed.');

  before(function(done) {
    var that = this;
    MongoClient.connect(mongoConnection, function(err, client) {
      should.not.exist(err);
      that.client = client;

      that.getAll = function(collName, cb) {
        client.collection(collName, function (err, collection) {

          // Locate all the entries using find
          collection.find().toArray(cb);
        });
      };

      done()
    });
  });

  afterEach(function(done) {
    // clear the db
    this.client.dropDatabase(done);
  });

  after(function(){
    this.client.close()
  });

  // for each test
  for (var index in tests) {
    var test = tests[index];
    it(test.description, function(done){
      var that = this;

      // run the input
      backendEvents.emit('flush', new Date(), test.input);

      // verify the output
      setTimeout(function() {
        async.forEach(Object.keys(test.output), function(collName) {
          that.getAll(collName, function(err, results) {
            should.not.exist(err);
            results.should.include(test.output[collName]);
          });
        });
      }, 100);

    });
  };

});
