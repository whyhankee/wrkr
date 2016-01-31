/* jshint mocha: true */
'use strict';
var EventEmitter = require('events').EventEmitter;

var debug = require('debug')('wrkr:tests');
var expect = require('expect.js');
var randomstring = require('random-string');

// Overwrite process.env for the test database name
process.env.NODE_ENV = 'unittest';

var WrkrMongodb = require('../lib/wrkr_mongodb');
var Wrkr = require('../lib/wrkr');


// Initialize our workerInterface
var wrkr = new Wrkr({
  backend: new WrkrMongodb({
    host:              'localhost',
    port:              27017,
    name:              'wrkr' + (process.env.NODE_ENV ? '_'+process.env.NODE_ENV : ''),
    user:              '',
    pswd:              '',
    dbOpt:             {w: 1},  // Write concern

    pollInterval:      50,      // default: 500, regular polling time (waiting for new items)
    pollIntervalBusy:  5,       // default: 20 next-item-polling-interval after processing an item

    errorRetryTime:    500,     // default: 5000, on error retry timer
                                //    (not so happy with auto-retrying though)
  })
});


// Start testing
describe('backend - start', testStart);
describe('basic operations', testBasic);
describe('backend - stop', testStop);



function testStart() {
  it('starts backend', function (done) {
    wrkr.start(done);
  });
}

function testStop() {
  it('stops backend', function (done) {
    wrkr.stop(done);
  });
}

function testBasic() {
  var testEmitter = new EventEmitter();
  var testTid = randomstring();
  var testEventName = 'event_'+testTid;
  var testQueueName = 'queue_'+testTid;

  // Test function to emit received Wrkr messages (back to our tests)
  function emitEvent(event, done) {
    debug('emitEvent', event);
    testEmitter.emit(event.name, event);
    return done(null);
  }

  // Start tests
  it('subscribes testEvent to our testQueue', function (done) {
    wrkr.subscribe(testQueueName, testEventName, emitEvent, done);
  });

  it('sends our event', function (done) {
    // Sent event and wait
    var ourEvent = {
      name: testEventName,
      tid: testTid,
      onceEvery: 10
    };
    // twice, so we can see that onceEvery works (not really tested / proved)
    wrkr.publish(ourEvent, err => {
      if (err) return done(err);
      wrkr.publish(ourEvent, done);
    });
  });

  it('starts a listener to receive our events from the subscribed queue(s)', function (done) {
    wrkr.listen(done);
  });

  it('receives our event', function (done) {
    testEmitter.on(testEventName, function (event) {
      expect(event.id).not.to.be(undefined);
      expect(event.created).not.to.be.within(new Date(), new Date(Date.now() - 1000));
      expect(event.name).to.be(testEventName);
      expect(event.queue).to.be(testQueueName);
      expect(event.tid).to.be(testTid);
      return done(null);
    });
  });
}
