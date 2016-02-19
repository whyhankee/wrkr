'use strict';
var EventEmitter = require('events').EventEmitter;
var async = require('async');
var debug = require('debug')('wrkr:tests');
var expect = require('expect.js');
var randomstring = require('random-string');

var Wrkr = require('../lib/wrkr');


function TestSuite(backend) {
  this.wrkr = new Wrkr({backend: backend});
}


TestSuite.prototype.start = function start(done) {
  debug('backend start');
  this.wrkr.start(function (err) {
    expect(err).to.be(null);
    debug('started');
    return done(err);
  });
};


TestSuite.prototype.required = function required(done) {
  debug('required features');
  async.series([
    this.testSubscriptions.bind(this),
    this.testEventHandling.bind(this),
    this.testPublish.bind(this),
    this.testOnceEvery.bind(this),
  ], done);
};


TestSuite.prototype.stop = function stop(done) {
  debug('backend stop');
  this.wrkr.stop(function (err) {
    expect(err).to.be(null);
    debug('stopped');
    return done(err);
  });
};



TestSuite.prototype.testSubscriptions = function testSubscriptions(done) {
  var self = this;
  var eventName = 'event_'+randomstring();
  var queueName = 'queue_'+randomstring();

  function fakeHandler() {}

  debug('testSubscriptions');
  return async.series([
    zeroSubscriptions,
    subscribe,
    oneSubscription,
    unsubscribe,
    zeroSubscriptions
  ], done);

  function zeroSubscriptions(cb) {
    debug('- zeroSubscriptions');
    self.wrkr.subscriptions(eventName, function (err, subscriptions) {
      expect(err).to.be(null);
      expect(subscriptions).to.be.an(Array);
      expect(subscriptions.length).to.be(0);
      return cb();
    });
  }

  function oneSubscription(done) {
    debug('- oneSubscription');
    self.wrkr.subscriptions(eventName, function (err, subscriptions) {
      expect(err).to.be(null);
      expect(subscriptions).to.be.an(Array);
      expect(subscriptions.length).to.be(1);
      expect(subscriptions[0]).to.be(queueName);
      return done();
    });
  }

  function subscribe(done) {
    debug('- subscribe');
    self.wrkr.subscribe(queueName, eventName, fakeHandler, function (err) {
      expect(err).to.be(null);
      expect(self.wrkr.eventHandlers[eventName]).to.be(fakeHandler);
      return done();
    });
  }

  function unsubscribe(done) {
    debug('- unsubscribe');
    self.wrkr.unsubscribe(queueName, eventName, function (err) {
      expect(err).to.be(null);
      return done();
    });
  }
};



TestSuite.prototype.testEventHandling = function testEventHandling(done) {
  var self = this;
  var eventName = 'event_'+randomstring();
  var queueName = 'queue_'+randomstring();
  var testTid = 'tid_'+randomstring();

  function fakeHandler() {}

  debug('testEventHandling');
  return async.series([
    zeroQitems,
    subscribe,
    publishEvent,
    oneQitem,
    deleteQitem,
    zeroQitems
  ], done);

  function zeroQitems(done) {
    debug('- zeroQitems');
    self.wrkr.getQueueItems({name: eventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(0);
      return done();
    });
  }

  function oneQitem(done) {
    debug('- oneQitem');
    self.wrkr.getQueueItems({name: eventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(1);
      expect(qitems[0].name).to.be(eventName);
      expect(qitems[0].queue).to.be(queueName);
      return done();
    });
  }

  function subscribe(done) {
    debug('- subscribe');
    self.wrkr.subscribe(queueName, eventName, fakeHandler, function (err) {
      expect(err).to.be(null);
      return done();
    });
  }

  function publishEvent(done) {
    debug('- publishEvent');
    var ourEvent = {
      name: eventName,
      tid: testTid,
    };
    self.wrkr.publish(ourEvent, function (err) {
      expect(err).to.be(null);
      return done();
    });
  }

  function deleteQitem(done) {
    debug('- deleteQitem');
    self.wrkr.deleteQueueItems({name: eventName, tid: testTid}, function (err) {
      expect(err).to.be(null);
      return done();
    });
  }
};


TestSuite.prototype.testPublish = function testPublish(done) {
  var self = this;
  var testEmitter = new EventEmitter();
  var testTid = randomstring();
  var testEventName = 'event_'+testTid;
  var testQueueName = 'queue_'+testTid;

  // Test function to emit received Wrkr messages (back to our tests)
  function emitEvent(event, cb) {
    debug('emit received event', event);
    testEmitter.emit(event.name, event);
    return cb(null);
  }

  debug('testPublish');
  return async.series([
    subscribe,
    publish,
    listenEvent,
    zeroQitems
  ], done);

  // Start tests
  function subscribe(cb) {
    self.wrkr.subscribe(testQueueName, testEventName, emitEvent, cb);
  }

  function publish(cb) {
    var ourEvent = {
      name: testEventName,
      tid: testTid,
    };
    self.wrkr.publish(ourEvent, cb);
  }

  function listenEvent(cb) {
    // get our message from our test eventEmitter
    testEmitter.once(testEventName, function (event) {
      expect(event.id).not.to.be(undefined);
      expect(event.created).not.to.be.within(new Date(), new Date(Date.now() - 1000));
      expect(event.name).to.be(testEventName);
      expect(event.queue).to.be(testQueueName);
      expect(event.tid).to.be(testTid);
      return cb(null);
    });

    // Start listening
    self.wrkr.listen(function (err) {
      if (err) return cb(err);
    });
  }

  function zeroQitems(cb) {
    self.wrkr.getQueueItems({name: testEventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(0);
      return cb();
    });
  }
};


TestSuite.prototype.testOnceEvery = function testOnceEvery(done) {
  var self = this;
  var testTid = randomstring();
  var testEventName = 'event_'+testTid;
  var testQueueName = 'queue_'+testTid;
  var onceEverySecs = 30;

  debug('testOnceEvery');
  return async.series([
    subscribe,
    publishTwoEvents,
    zeroQitemsWithProperWhen
  ], done);

  function subscribe(cb) {
    self.wrkr.subscribe(testQueueName, testEventName, cb);
  }

  function publishTwoEvents(cb) {
    var ourEvent = {
      name: testEventName,
      onceEvery: onceEverySecs * 1000,
      tid: testTid
    };
    self.wrkr.publish(ourEvent, function (err) {
      if (err) return cb(err);
      self.wrkr.publish(ourEvent, cb);
    });
  }

  function zeroQitemsWithProperWhen(cb) {
    self.wrkr.getQueueItems({name: testEventName, tid: testTid}, function (err, qitems) {
      expect(err).to.be(null);
      expect(qitems).to.be.an(Array);
      expect(qitems.length).to.be(1);
      expect(qitems[0].name).to.be(testEventName);
      expect(qitems[0].queue).to.be(testQueueName);

      var whenStart = new Date(Date.now() + (onceEverySecs-5)*1000);
      var whenEnd = new Date(Date.now() + onceEverySecs*1000);
      expect(qitems[0].when).to.be.within(whenStart, whenEnd);
      return cb();
    });
  }
};


module.exports = TestSuite;