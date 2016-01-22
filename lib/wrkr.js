'use strict';
var debug = require('debug')('wrkr:main');


// Wrkr
//
function Wrkr(options, subscriptions) {
  var self = this;
  this.store = options.store;
  this.eventHandlers = {};

  // Check options
  if (!this.store) {
    throw new Error('no store passed');
  }

  debug('store', options.store);
}


Wrkr.prototype.start = function start(opt, done) {
  this.store.start(this, opt, done);
};


Wrkr.prototype.stop = function stop(done) {
  this.store.stop(done);
};


// Events
//    name:     name of the event
//    tid:      target id of object
//    when:     when to process this item
//    headers:  extra (transport related) headers
//    payload:  extra data properties
//
Wrkr.prototype.publish = function publish(events, done) {
  var eventList = Array.isArray(events) ? events : [events];
  debug('publish', eventList);
  this.store.publish(eventList, done);
};


Wrkr.prototype.subscribe = function(queueName, eventName, handler, done) {
  var self = this;
  debug('subscribe', queueName, eventName);

  // Already a handler? Should also have been subscribed by then ..
  if (self.eventHandlers[eventName]) {
    throw new Error(`handler for event ${eventName} on queue '${queueName}' already exists`);
  }

  self.eventHandlers[eventName] = handler;
  this.store.subscribe(queueName, eventName, done);
};


Wrkr.prototype.unsubscribe = function(queueName, eventName, done) {
  debug('unsubscribe', queueName, eventName);
  return done(new Error('notImplemtedYet'));
};


Wrkr.prototype.listen = function (done) {
  debug('listen');
  this.store.listen(done);
};


Wrkr.prototype._dispatch = function _dispatch(event, done) {
  var self = this;
  debug('_dispatching event', event);

  if (!self.eventHandlers[event.name]) {
    debug('noImplementedYet', event.name);
    return done();
  }
  self.eventHandlers[event.name](event, function (err) {
    // TODO: For testing purposes
    if (err) throw err;

    return done(err);
  });
};




// Exports
//
module.exports = Wrkr;
