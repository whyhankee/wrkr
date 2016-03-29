'use strict;';
var events = require('events');
var util = require('util');

var async = require('neo-async');
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
  * @param {object} options.archiveMethod   archive-strategy ('delete', 'markDone')
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
    return done(err || null);
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
      if (err) return cb(err);

      self.is_started = false;
      return cb(null);
    });
  }
};


/********************************************************************
  Subsribe / Unsubscribe
 ********************************************************************/

Wrkr.prototype.subscribe = function(queueName, eventName, handler, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  if (done === undefined && typeof handler === 'function') {
    done = handler;
    handler = undefined;
  }

  debug('subscribe', queueName, eventName);

  if (handler) {
    this.eventHandlers[eventName] = handler;
  }
  this.backend.subscribe(queueName, eventName, done);
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

  // TODO: cache this
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
  debug('publishing', events);

  var self = this;
  var eventList = Array.isArray(events) ? events : [events];
  var queueItems = [];

  return async.each(eventList, createQitems, function (err) {
    if (err) return done(err);

    debug('publish', queueItems);
    if (queueItems.length === 0) return done(null);

    return self.backend.publish(queueItems, done);
  });

  function createQitems(event, cb) {
    return async.series([processOnceEvery, addQueueItems], cb);

    // Restricts send an event/tid combonation for a given time
    function processOnceEvery(cb) {
      if (!event.onceEvery) return cb();

      // Are there events scheduled?
      self.getQueueItems({name: event.name, tid: event.tid}, function (err, qitems) {
        if (err) return cb(err);

        if (qitems.length === 0) {
          event.when = new Date(Date.now() + event.onceEvery);
        } else {
          event._ignore = 'onceEvery - duplicate';
        }
        return cb();
      });
    }

    function addQueueItems(cb) {
      if (event._ignore) {
        debug('event not published, reason: '+event._ignore);
        return cb();
      }

      self.subscriptions(event.name, function (err, queueNames) {
        if (err) return cb(err);

        queueNames.forEach(function (qName) {
          queueItems.push({
            queue:      qName,
            created:    new Date(),
            when:       event.when || new Date(),
            name:       event.name,
            tid:        event.tid,
            payload:    event.payload || {},
          });
        });
        return cb();
      });
    }
  }

};


Wrkr.prototype.getQueueItems = function getQueueItems(filter, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  var knownKeys = ['name', 'queue', 'tid'];
  var filterSpec = {};

  // Build filterSpec from known filter fields
  knownKeys.forEach( (k) => {
    if (!filter[k]) return;
    filterSpec[k] = filter[k];
  });
  // at least one option, name should be one of them
  if (Object.keys(filterSpec).length < 1 || !filterSpec.name) {
    debug('invalid search spec ', filterSpec);
    return done(new Error('getQueueItems - invalid search spec'));
  }
  filterSpec.when = {$gte: new Date()};

  this.backend.getQueueItems(filterSpec, function (err, qitems) {
    debug('getQueueItems', filter, qitems);
    return done(err, qitems);
  });
};


Wrkr.prototype.deleteQueueItems = function deleteQueueItems(filter, done) {
  if (!this.is_started) return done(new Error('Wrkr not started'));

  var knownKeys = ['name', 'queue', 'tid'];
  var filterSpec = {};

  // Build filterSpec from known filter fields
  knownKeys.forEach( (k) => {
    if (!filter[k]) return;
    filterSpec[k] = filter[k];
  });
  // at least one option, name should be one of them
  if (Object.keys(filterSpec).length < 1 || !filterSpec.name) {
    debug('invalid search spec ', filterSpec);
    return done(new Error('deleteQueueItems - invalid search spec'));
  }

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
    dispatched: new Date()
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
      debug(`error received for ${event.name} (${event.tid}): `+err ? err.message:null);
      self._reportError(err);
      return onArchiveDone();
    }

    debug(`ack event ${event.name} (${event.tid})`, self.archiveMethod);
    if (self.archiveMethod === 'markDone') {
      return self.backend.doneQueueItem(event, onArchiveDone);
    } else if (self.archiveMethod === 'delete') {
      return self.backend.deleteQueueItems(event, onArchiveDone);
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

    debug(`archive (${self.archiveMethod}) complete - ${event.name} (${event.tid})`);
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
