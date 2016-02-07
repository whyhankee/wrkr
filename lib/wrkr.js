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
  * @param {object} options.archiveMethod   archive-strategy to use ('delete', 'markDone')
  */
function Wrkr(options) {
  this.backend = options.backend;
  this.archiveMethod = options.archiveMethod || 'delete';
  this.eventHandlers = {};

  this.is_started = false;
  this.currentEvent = null;

  // Check options
  if (!this.backend) {
    throw new Error('no backend passed');
  }

  debug('Wrkr options', options);
}
util.inherits(Wrkr, events.EventEmitter);


/********************************************************************
  Start and Stop backend operations
 ********************************************************************/

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
  var self = this;
  if (!this.is_started) return done(new Error('Wrkr not started'));

  waitProcessing( function (err) {
    if (err) return done(err);

    stopBackend(done);
  });

  function waitProcessing(cb) {
    if (self.currentEvent === null) return cb();

    // Wait for the currentEvent to complete
    var waitInterval = setInterval(checkProcessing, 100);
    function checkProcessing() {
      debug(' stop() .. waiting for in process qitem');
      if (self.currentEvent === null) {
        clearInterval(waitInterval);
        return cb(null);
      }
    }
  }

  function stopBackend(cb) {
    self.backend.stop( (err) => {
      if (!err) self.is_started = false;
      return cb(err);
    });
  }
};


/********************************************************************
  Subsribe / Unsubscribe
 ********************************************************************/

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


/********************************************************************
  Publish
 ********************************************************************/

// Event methods, Event object contains:
//    name:     name of the event
//    tid:      target id of object (like userid, or other item id)
//    when:     when to process this item
//    payload:  extra data properties (optional)
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


Wrkr.prototype.deleteQueueItems = function deleteQueueItems(filter, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  var knownKeys = ['event', 'queue', 'tid'];
  var filterSpec = {};

  // Build filterSpec from known filter fields
  knownKeys.forEach( (k) => {
    if (!filter[k]) return;
    filterSpec[k] = filter[k];
  });

  this.backend.deleteQueueItems(filterSpec, function (err, qitems) {
    debug('deleteQueueItems', filter, qitems);
    return done(err, qitems);
  });
};


/********************************************************************
  Listen
 ********************************************************************/

Wrkr.prototype.listen = function (done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  debug('listen');
  this.backend.listen(done);
};


/********************************************************************
  'private' functions (for the backends)
 ********************************************************************/

Wrkr.prototype._dispatch = function _dispatch(event, done) {
  var self = this;

  debug('_dispatching event', event);
  this.currentEvent = {
    name: event.name,
    tid: event.tid,
    started: new Date()
  };

  // Not handled?
  if (!this.eventHandlers[event.name]) {
    debug(`no eventHandlers for ${event.name}`);
    return onDispatchDone(null);
  } else {
    return this.eventHandlers[event.name](event, onDispatchDone);
  }

  // Dispatched, archive when done
  //  on error, the item will be retried
  function onDispatchDone(err) {
    if (err) {
      debug(`error received for ${event.name} (${event.tid}): `+err ? err.message : null);
      self._reportError(err);
      return onArchiveDone();
    }

    debug(`ack event ${event.name} (${event.tid})`, self.archiveMethod);
    if (self.archiveMethod === 'markDone') {
      return self.backend.archiveArchive(event, onArchiveDone);
    } else if (self.archiveMethod === 'delete') {
      return self.backend.archiveDelete(event, onArchiveDone);
    } else {
      throw new Error('invalid archive method: '+self.archiveMethod);
    }
  }

  function onArchiveDone(err) {
    if (err) {
      debug(`archive error for ${event.name} error: ${err.message}`);
      // report error and continue
      self._reportError(err);
    }

    debug(`archive complete - ${event.name} (${event.tid})`);
    self.currentEvent = null;
    return done();
  }
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
