/* jshint mocha: true */
'use strict';
var EventEmitter = require('events').EventEmitter;

var debug = require('debug')('wrkr:tests');
var expect = require('expect.js');
var randomstring = require('random-string');

var WrkrMongodb = require('../lib/wrkr_mongodb');
var Wrkr = require('../lib/wrkr');


// Initialize our workerInterface
var wrkr = new Wrkr({
  backend: new WrkrMongodb({
    host:              'localhost',
    port:              27017,
    name:              'wrkr_unittest',
    user:              '',
    pswd:              '',
    dbOpt:             {w: 1},  // Write concern

    pollInterval:      50,      // default: 500, regular polling time (waiting for new items)
    pollIntervalBusy:  5,       // default: 5 next-item-polling-interval after processing an item

    errorRetryTime:    500,     // default: 5000, on error retry timer
                                //    (not so happy with auto-retrying though)
  })
});


// Start testing
describe('backend - start', testStart);
describe('subscriptions', testSubscriptions);
describe('basic operations', testBasic);
describe('- OnceEvery', testOnceEvery);
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


function testSubscriptions() {
  var eventName = 'event_'+randomstring();
  var queueName = 'queue_'+randomstring();

  function fakeHandler() {}

  it('event should not have any subscriptions', function (done) {
    wrkr.subscriptions(eventName, function (err, subscriptions) {
      expect(err).to.be(null);
      expect(subscriptions).to.be.an(Array);
      expect(subscriptions.length).to.be(0);
      return done();
    });
  });

  it('subscribes the testqueue to the event', function (done) {
    wrkr.subscribe(queueName, eventName, fakeHandler, function (err) {
      expect(err).to.be(null);
      expect(wrkr.eventHandlers[eventName]).to.be(fakeHandler);
      return done();
    });
  });

  it('event should have a subscribed queue', function (done) {
    wrkr.subscriptions(eventName, function (err, subscriptions) {
      expect(err).to.be(null);
      expect(subscriptions).to.be.an(Array);
      expect(subscriptions.length).to.be(1);
      expect(subscriptions[0]).to.be(queueName);
      return done();
    });
  });

  it('unsubscribes the testqueue to the event', function (done) {
    wrkr.unsubscribe(queueName, eventName, done);
  });

  it('event should not have a subscribed queue anymore', function (done) {
    wrkr.subscriptions(eventName, function (err, subscriptions) {
      expect(err).to.be(null);
      expect(subscriptions).to.be.an(Array);
      expect(subscriptions.length).to.be(0);

      expect(wrkr.eventHandlers[eventName]).to.be(undefined);
      return done();
    });
  });
}


// TODO: It's Time to split this one up
//
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

  it('getQueueItems = 0', function (done) {
    wrkr.getQueueItems({name: testEventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(0);
      return done();
    });
  });

  it('publish our event', function (done) {
    var ourEvent = {
      name: testEventName,
      tid: testTid,
    };
    wrkr.publish(ourEvent, done);
  });

  it('getQueueItems = 1', function (done) {
    wrkr.getQueueItems({name: testEventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(1);
      expect(qitems[0].name).to.be(testEventName);
      expect(qitems[0].queue).to.be(testQueueName);
      return done();
    });
  });


  it('delete events', function (done) {
    wrkr.deleteQueueItems({name: testEventName, tid: testTid}, done);
  });

  it('getQueueItems = 0', function (done) {
    wrkr.getQueueItems({name: testEventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(0);
      return done();
    });
  });

  it('publish our event (again)', function (done) {
    var ourEvent = {
      name: testEventName,
      tid: testTid,
    };
    wrkr.publish(ourEvent, done);
  });

  it('start a listener to receive our events from the subscribed queue(s)', function (done) {
    wrkr.listen(function (err) {
      if (err) return done(err);
    });

    testEmitter.on(testEventName, function (event) {
      expect(event.id).not.to.be(undefined);
      expect(event.created).not.to.be.within(new Date(), new Date(Date.now() - 1000));
      expect(event.name).to.be(testEventName);
      expect(event.queue).to.be(testQueueName);
      expect(event.tid).to.be(testTid);
      return done(null);
    });
  });

  // it('receives our event', function (done) {
  // });
  //
  it('getQueueItems = 0', function (done) {
    wrkr.getQueueItems({name: testEventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(0);
      return done();
    });
  });
}


function testOnceEvery() {
  var testTid = randomstring();
  var testEventName = 'event_'+testTid;
  var testQueueName = 'queue_'+testTid;
  var onceEverySecs = 30;

  // Start tests
  it('subscribes testEvent to our testQueue', function (done) {
    wrkr.subscribe(testQueueName, testEventName, done);
  });

  // Publish two messages
  it('publish two events with .onceEvery property', function (done) {
    var ourEvent = {
      name: testEventName,
      onceEvery: onceEverySecs * 1000,
      tid: testTid
    };
    wrkr.publish(ourEvent, function (err) {
      if (err) return done(err);
      wrkr.publish(ourEvent, done);
    });
  });

  it('getQueueItems = 1 - should only be 1 queued with a proper .when property', function (done) {
    wrkr.getQueueItems({name: testEventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(1);
      expect(qitems[0].name).to.be(testEventName);
      expect(qitems[0].queue).to.be(testQueueName);

      let whenStart = new Date(Date.now() + (onceEverySecs-5)*1000);
      let whenEnd = new Date(Date.now() + onceEverySecs*1000);
      expect(qitems[0].when).to.be.within(whenStart, whenEnd);

      return done();
    });
  });

}
