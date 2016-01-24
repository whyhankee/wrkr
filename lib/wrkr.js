'use strict';
var events = require('events');
var util = require('util');

var debug = require('debug')('wrkr');


/********************************************************************
  Wrkr
 ********************************************************************/

 /**
  * Creates an instance of Wrkr.
  *
  * @constructor
  * @this {Wrkr}
  * @param {object} options           options for the worker
  * @param {object} options.backend   backend to use for this worker
  */
function Wrkr(options) {
  this.backend = options.backend;
  this.eventHandlers = {};

  // Check options
  if (!this.backend) {
    throw new Error('no backend passed');
  }

  debug('Wrkr options', options);
}
util.inherits(Wrkr, events.EventEmitter);


// Start and Stop backend operations
//
Wrkr.prototype.start = function start(opt, done) {
  if (typeof opt === 'function' && done === undefined) {
    done = opt;
    opt = {};
  }

  this.backend.start(this, opt, done);
};


Wrkr.prototype.stop = function stop(done) {
  this.backend.stop(done);
};


// Event methods, Event object contains:
//    name:     name of the event
//    tid:      target id of object
//    when:     when to process this item
//    headers:  extra (transport related) headers
//    payload:  extra data properties
//
Wrkr.prototype.publish = function publish(events, done) {
  var eventList = Array.isArray(events) ? events : [events];
  debug('publish', eventList);
  this.backend.publish(eventList, done);
};


Wrkr.prototype.subscribe = function(queueName, eventName, handler, done) {
  var self = this;
  debug('subscribe', queueName, eventName);

  // Avoid duplication subscribe actions to the database.
  if (!self.eventHandlers[eventName]) {
    this.backend.subscribe(queueName, eventName, done);
  }
  self.eventHandlers[eventName] = handler;
};


Wrkr.prototype.unsubscribe = function(queueName, eventName, done) {
  debug('unsubscribe', queueName, eventName);
  return done(new Error('notImplemtedYet'));
};


Wrkr.prototype.listen = function (done) {
  debug('listen');
  this.backend.listen(done);
};


/********************************************************************
  'private' functions for the backends
 ********************************************************************/

Wrkr.prototype._dispatch = function _dispatch(event, done) {
  var self = this;
  debug('_dispatching event', event);

  if (!self.eventHandlers[event.name]) {
    debug('event not handled - ', event.name);
    return done(null);
  }
  self.eventHandlers[event.name](event, function (err, followUp) {
    if (err) self._reportError(err);

    return done(err, followUp);
  });
};


Wrkr.prototype._reportError = function _reportError(err) {
  var handled = this.emit('error', err) === true;
  if (!handled) {
    // Errors really need to be handled
    throw err;
  }
};


/********************************************************************
  Exports
 ********************************************************************/

module.exports = Wrkr;
