/* jshint mocha: true */
'use strict';
var EventEmitter = require('events').EventEmitter;

var debug = require('debug')('wrkr:tests');
var expect = require('expect.js');
var randomstring = require('random-string');

var WrkrMongodb = require('../lib/wrkr_mongodb');
var Wrkr = require('../lib/wrkr');


// Globals
//
var testEventName = 'event_'+randomstring();
var testQueueName = 'queue_'+randomstring();
var testTid       = 'tid_'+randomstring();


// We use this emitter to deliver our events back to the tests
var ourEmitter = new EventEmitter();

// Initialize our workerInterface
var wrkr = new Wrkr({
  store: new WrkrMongodb()
});


// Mocha's Before and After
//
before( function (done) {
  wrkr.start(done);
});
after( function (done) {
  wrkr.stop(done);
});


// Setup listener
//
describe('basic operations', function () {

  // Test function to emit received messages (back to our tests)
  function emitEvent(event, done) {
    debug('emitEvent', event);
    ourEmitter.emit(event.name, event);
    return done(null);
  }


  it('subscribe testEvent to our testQueue', function (done) {
    wrkr.subscribe(testQueueName, testEventName, emitEvent, done);
  });

  it('send our event', function (done) {
    // Sent event and wait
    var ourEvent = {
      name: testEventName,
      tid: testTid
    };
    wrkr.emit(ourEvent, done);
  });

  it('start a listener to receive our events from the subscribed queue(s)', function (done) {
    wrkr.listen(done);
  });

  it('receive our event', function (done) {
    ourEmitter.on(testEventName, function (event) {
      expect(event.id).not.to.be(undefined);
      expect(event.created).not.to.be(undefined);
      expect(event.name).to.be(testEventName);
      expect(event.queue).to.be(testQueueName);
      expect(event.tid).to.be(testTid);

      return done(null);
    });
  });
});
