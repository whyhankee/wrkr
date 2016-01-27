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

  this.is_started = false;

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
  var self = this;
  if (typeof opt === 'function' && done === undefined) {
    done = opt;
    opt = {};
  }

  this.backend.start(this, opt, function (err) {
    if (!err) {
      self.is_started = true;
    }
    return done(err);
  });
};


Wrkr.prototype.stop = function stop(done) {
  var self = this;

  this.backend.stop(function (err) {
    if (!err) {
      self.is_started = false;
    }
    return done(err);
  });
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

  if (!this.is_started) {
    return done(new Error('Wrkr not started'));
  }

  debug('publish', eventList);
  this.backend.publish(eventList, done);
};


Wrkr.prototype.subscribe = function(queueName, eventName, handler, done) {
  var self = this;

  if (!this.is_started) {
    return done(new Error('Wrkr not started'));
  }

  debug('subscribe', queueName, eventName);

  // Avoid duplication subscribe actions to the database.
  if (!self.eventHandlers[eventName]) {
    this.backend.subscribe(queueName, eventName, done);
  }
  self.eventHandlers[eventName] = handler;
};


Wrkr.prototype.unsubscribe = function(queueName, eventName, done) {
  if (!this.is_started) {
    return done(new Error('Wrkr not started'));
  }

  debug('unsubscribe', queueName, eventName);
  return done(new Error('notImplemtedYet'));
};


Wrkr.prototype.listen = function (done) {
  if (!this.is_started) {
    return done(new Error('Wrkr not started'));
  }

  debug('listen');
  this.backend.listen(done);
};


/********************************************************************
  'private' functions (for the backends)
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
