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
  if (typeof opt === 'function' && done === undefined) {
    done = opt;
    opt = {};
  }

  if (this.is_started) return done(new Error('Wrkr already started'));

  this.backend.start(this, opt, (err) =>  {
    if (!err) this.is_started = true;
    return done(err);
  });
};


Wrkr.prototype.stop = function stop(done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  this.backend.stop( (err) => {
    if (!err) this.is_started = false;
    return done(err);
  });
};


Wrkr.prototype.subscribe = function(queueName, eventName, handler, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  debug('subscribe', queueName, eventName);

  // Avoid duplicate subscribe actions to the database.
  if (!this.eventHandlers[eventName]) {
    this.backend.subscribe(queueName, eventName, done);
  }
  this.eventHandlers[eventName] = handler;
};


Wrkr.prototype.unsubscribe = function(queueName, eventName, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  debug('unsubscribe', queueName, eventName);

  // Remove method handler from list
  if (this.eventHandlers[eventName]) {
    this.eventHandlers[eventName] = undefined;
  }
  this.backend.unsubscribe(queueName, eventName, done);
};


Wrkr.prototype.subscriptions = function subscriptions(eventName, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  this.backend.subscriptions(eventName, function (err, queues) {
    debug(`subscriptions for ${eventName}`, queues);
    return done(err, queues);
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
  if (!this.is_started) return done(new Error('Wrkr not started'));

  var eventList = Array.isArray(events) ? events : [events];
  debug('publish', eventList);
  this.backend.publish(eventList, done);
};


Wrkr.prototype.getQueueItems = function getQueueItems(filter, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  var knownKeys = ['event', 'queue', 'tid'];
  var filterSpec = {};

  // Build filterSpec from known filter fields
  knownKeys.forEach( (k) => {
    if (!filter[k]) return;
    filterSpec[k] = filter[k];
  });

  this.backend.getQueueItems(filterSpec, function (err, qitems) {
    debug('getQueueItems', filter, qitems);
    return done(err, qitems);
  });
};


Wrkr.prototype.listen = function (done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  debug('listen');
  this.backend.listen(done);
};


/********************************************************************
  'private' functions (for the backends)
 ********************************************************************/

Wrkr.prototype._dispatch = function _dispatch(event, done) {
  debug('_dispatching event', event);

  if (!this.eventHandlers[event.name]) {
    debug('event not handled - ', event.name);
    return done(null);
  }
  this.eventHandlers[event.name](event, (err) => {
    if (err) this._reportError(err);

    debug(`ack event received for ${event.name}`);
    return done(err, event);
  });
};


Wrkr.prototype._reportError = function _reportError(err) {
  var handled = this.emit('error', err) === true;
  if (!handled) {
    // Errors really need to be handled (or logged somewhere)
    throw err;
  }
};


/********************************************************************
  Exports
 ********************************************************************/

module.exports = Wrkr;
