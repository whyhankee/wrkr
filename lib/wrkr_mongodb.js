'use strict';
var async = require('neo-async');
var mongoose = require('mongoose');
var debug = require('debug')('wrkr:mongodb');


/********************************************************************
  Schema's & Models
 ********************************************************************/

var subscriptionSchema = {
  eventName:  { type: String, unique: true },
  queues:     []
};


var qItemsSchema = {
  queue:      {type: String, index: true},
  created:    {type: Date, index: true, default: Date.now},
  when:       {type: Date, index: true, sparse: true},
  done:       {type: Date, index: true, sparse: true},

  name:       {type: String},
  tid:        {type: String, index: true},
  payload:    {type: mongoose.Schema.Types.Mixed}

  // The stack with encountered errors
  // [
  //  {
  //    date: new Date(),
  //    message: String,
  //    stack: [String],
  //  }
  // ]
  // errorList:  [mongoose.Schema.Types.Mixed]
};



/********************************************************************
  WrkrMongodb
 ********************************************************************/

var optDefaults = {
  host:              'localhost',
  port:              27017,
  name:              'wrkr' + (process.env.NODE_ENV ? '_'+process.env.NODE_ENV : ''),
  user:              '',
  pswd:              '',
  dbOpt:             {w: 1},  // Write concern

  pollInterval:      500,     // regular polling time (waiting for new items)
  pollIntervalBusy:  5,       // quick-fetch-next-polling-interval after processing an item

  errorRetryTime:    5000,    // on error retry timer - not so happy with (auto-retrying though)
};


function WrkrMongodb(opt) {
  opt = opt || {};
  this.opt = {};
  this.wrkr = null;
  this.db = null;

  this.listenQueues = {};       // queues the server shpuld listen on
  this.pollTimer = null;        // the poll-timer, so that we can stop it;

  // Mongoose Models
  this.Subscription = null;
  this.QueueItems = null;

  // Set (default) options
  Object.keys(optDefaults).forEach( (k) => {
    this.opt[k] = (opt[k] === undefined) ? optDefaults[k] : opt[k];
  });
}


/********************************************************************
  Starting and Stopping
 ********************************************************************/

WrkrMongodb.prototype.start = function (wrkr, opt, done) {
  this.wrkr = wrkr;

  // Create mongodb connectionString
  let dbUserPswd = '';
  if (this.opt.user && this.opt.pswd) {
    dbUserPswd = `${this.opt.user}:${this.opt.pswd}@`;
  }
  let dbConnectStr = `mongodb://${dbUserPswd}${this.opt.host}:${this.opt.port}/${this.opt.name}`;

  // Connect
  debug('starting wrkr_mongodb :'+dbConnectStr);
  this.db = mongoose.createConnection();
  this.db.open(dbConnectStr, this.opt.dbOpt, (err) => {
    if (err) return done(err);

    // Make models
    this.Subscription = this.db.model('wrkr_subscriptions',
      new mongoose.Schema(subscriptionSchema)
    );
    this.QueueItems = this.db.model('wrkr_qitems',
      new mongoose.Schema(qItemsSchema)
    );

    debug('connected to mongodb');
    return done();
  });
};


WrkrMongodb.prototype.stop = function stop(done) {
  debug('stopping wrkr_mongodb');

  // Stop pollTimer (if started)
  //    this.pollTimer = null indicates that we are stopping,
  //    poll() should not set a new timeout
  if (this.pollTimer) {
    clearTimeout(this.pollTimer);
    this.pollTimer = null;
  }

  this.db.close(done);
};


/********************************************************************
  Subscriptions functions
 ********************************************************************/

WrkrMongodb.prototype.subscribe = function (queueName, eventName, done) {
  debug('subscribe', queueName, eventName);
  this.listenQueues[queueName] = true;

  var find = {eventName: eventName};
  var update = {
    $addToSet: {queues: queueName}
  };
  // debug('subscribe', find, update);
  this.Subscription.findOneAndUpdate(find, update, {upsert: true}, function (err) {
    return done(err || null);
  });
};


WrkrMongodb.prototype.unsubscribe = function(queueName, eventName, done) {
  debug('unsubscribe', queueName, eventName);
  this.listenQueues[queueName] = true;

  var find = {eventName: eventName};
  var update = {
    $pull: {queues: queueName}
  };
  // debug('unsubscribe', find, update);
  this.Subscription.findOneAndUpdate(find, update, {upsert: true}, function (err) {
    return done(err || null);
  });
};


WrkrMongodb.prototype.subscriptions = function subscriptions(eventName, done) {
  var find = {eventName: eventName};
  this.Subscription.findOne(find, function (err, subscription) {
    var result = subscription ? subscription.queues : [];
    debug(`subscriptions for ${eventName}`, result);
    return done(err, result);
  });
};


/********************************************************************
  Publishing functions
 ********************************************************************/


WrkrMongodb.prototype.publish = function publish(events, done) {
  var self = this;
  var queueItems = [];

  debug('publish', events);

  // For each event,
  //    get the queues
  //    create the queue-items
  //    write to the db in 1 batch
  return async.eachLimit(events, 5, createQueueItems, (err) => {
    if (err) return done(err);

    debug('publish queueitems', queueItems);

    // If there are items to insert, insert them (0 items === error)
    if (queueItems.length === 0) {
      return done();
    }

    // Note, mongoose has no single-operation batch insert
    //  so we go the mongo-native way here
    this.QueueItems.collection.insert(queueItems);
    return done();
  });

  function createQueueItems(event, cb) {
    if (!event.name) return cb(new Error('publish: missing event.name'));

    // Hacky: checkOnceEvery should be checked first for each event
    //  This functionality should move to the Wrkr
    checkOnceEvery(function (err) {
      if (err) return cb(err);

      // ignore flag (from checkOnceEvery), log and ignore
      if (event._ignore) {
        debug(`*ignoring* event ${event.name} - ${event._ignore}`);
        return cb();
      }

      // Create qitems for the event (one for each queue)
      self._queuesForEvent(event.name, function (err, queueNames) {
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
    });

    // onceEvery
    //    onceEvery could be (1 minute === 60*1000)
    //    it checks if there are queueitems scheduled for the tid
    //    will only insert one if no queueitem are found (no duplicates)
    function checkOnceEvery(cb) {
      if (!event.onceEvery) return cb();

      var timeFrame = new Date(Date.now() + event.onceEvery);
      var findExisting = {
        name: event.name,
        tid: event.tid,
        when: {$exists: true}
      };
      self.QueueItems.count(findExisting, (err, count) => {
        if (err) return cb(err);

        // Event is already scheduled (in one on more queues)
        if (count) {
          event._ignore = 'checkOnce - already scheduled';
          return cb();
        }

        // No event found (yet)
        //  note: since it's not atomic, not completely bulletproof
        //    not completely bulletproof means hacky, lets see if this works first
        // update .when to the onceEvery so it gets processed when the time's up
        debug(`onceEvery - althering when for event ${event.name} by ${event.onceEvery}`);
        event.when = timeFrame;
        return cb();
      });
    }
  }
};


/********************************************************************
  Event andling
 ********************************************************************/

WrkrMongodb.prototype.getQueueItems = function getQueueItems(filter, done) {
  var mongoFilter = Object.create(filter);
  mongoFilter.done = {$exists: false};
  this.QueueItems.find(mongoFilter, {}, {created: 1}, function (err, qitems) {
    return done(err || null, qitems);
  });
};


WrkrMongodb.prototype.deleteQueueItems = function deleteQueueItems(filter, done) {
  var mongoFilter = Object.create(filter);
  mongoFilter.done = {$exists: false};
  debug('deleteQueueItems', mongoFilter);
  this.QueueItems.remove(mongoFilter, function (err) {
    return done(err || null);
  });
};


/********************************************************************
  Listen functions
 ********************************************************************/

WrkrMongodb.prototype.listen = function listen(done) {
  var self = this;
  var listenQueues = Object.keys(self.listenQueues);

  debug('listen - start polling - polltimer: started');

  self.pollTimer = Date.now();
  setImmediate(function () {
    poll();
  });
  return done(null);

  function updatePollTimer(timeoutMs) {
    // debug('polltimer: update', timeoutMs);
    if (self.pollTimer === null) {
      return debug('listen - updatePollTimer: detected stoppage');
    }
    self.pollTimer = setTimeout(poll, timeoutMs);
  }

  function poll() {
    // First we have to check if there's work to be done
    var searchSpec = {
      when:  {$lte: new Date()},
      queue: {$in: listenQueues}
    };
    var sort = {
      created: 1
    };
    // The darn errorRetryTimer, implement error state
    var updateSpec = {
      when: new Date(Date.now() + self.opt.errorRetryTime)
    };
    var sortOptions = {sort: sort};
    // debug('polling', searchSpec, updateSpec, sort);
    self.QueueItems.findOneAndUpdate(searchSpec, updateSpec, sortOptions, (err, event) => {
      if (err) throw err;

      // No more events to process, go to regular polling interval
      if (!event) return updatePollTimer(self.opt.pollInterval);

      // Prepare event to dispatch to Wrkr
      var eventObj = event.toObject();
      eventObj.id = event.id;
      delete eventObj._id;

      debug('dispatching event to Wkrk', eventObj);
      self.wrkr._dispatch(eventObj, (err) => {
        // Errors should have been handles by Wrkr
        // Archiving will also be called by Wrkr
        if (err) {
          debug('ignoring error '+err.message);
        }
        // Just start a new polltimer
        return updatePollTimer(self.opt.pollIntervalBusy);
      });
    });
  }
};



/********************************************************************
  Archiving functions
 ********************************************************************/

 // Mark processed qitem as done
 WrkrMongodb.prototype.archiveMarkDone = function archiveMarkDone(event, done) {
   debug(`archiving processed qitem ${event.name} ${event.tid}`);
   let updateSpec = {
     $unset: { when: 1 },
     $set:   { done: new Date() }
   };
   this.QueueItems.findByIdAndUpdate(event.id, updateSpec, function (err) {
     if (err) return done(err);

     debug(`archived processed qitem ${event.name} (${event.tid})`);
     return done(null);
   });
 };


 // Delete processed qitem when done
 WrkrMongodb.prototype.archiveDelete = function archiveDelete(event, done) {
   this.QueueItems.findByIdAndRemove(event.id, function (err) {
     if (err) return done(err);

     debug(`deleted processed qitem ${event.name} (${event.tid})`);
     return done(null);
   });
 };


/********************************************************************
  Internal functions
 ********************************************************************/

 // TODO: Implement caching
WrkrMongodb.prototype._queuesForEvent = function _queuesForEvent(eventName, done) {
  this.Subscription.findOne({eventName: eventName}, function (err, event) {
    if (err) return done(err);

    var queues = (event && event.queues) ? event.queues : [];
    debug('_queuesForEvent '+eventName, queues);
    return done(null, queues);
  });
};


/********************************************************************
  Exports
 ********************************************************************/

module.exports = WrkrMongodb;
