'use strict';
var async = require('neo-async');
var mongoose = require('mongoose');
var debug = require('debug')('wrkr:mongodb');


/*************************************************************
  Schema's & Models
 ************************************************************/

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
  headers:    {type: mongoose.Schema.Types.Mixed},
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



/*************************************************************
  WrkrMongodb
 ************************************************************/

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
  var self = this;
  opt = opt || {};
  this.opt = {};
  this.wrkr = null;
  this.db = null;

  // the server should listen to queues this service has subscribed to
  this.listenQueues = {};

  // the poll-timer, so that we can stop it;
  this.pollTimer = null;

  // Mongoose Models
  this.Subscription = null;
  this.QueueItems = null;

  // Set (default) options
  Object.keys(optDefaults).forEach(function (k) {
    self.opt[k] = (opt[k] === undefined) ? optDefaults[k] : opt[k];
  });

  // Create mongodb connectionString
  var dbUserPswd = '';
  if (this.opt.user && this.opt.pswd) {
    dbUserPswd = `${self.opt.user}:${self.opt.pswd}@`;
  }
  this.dbConnectStr = `mongodb://${dbUserPswd}${self.opt.host}:${self.opt.port}/${self.opt.name}`;
}


//  opt       = object (Wrkr options)
//
WrkrMongodb.prototype.start = function (wrkr, opt, done) {
  var self = this;
  this.wrkr = wrkr;

  if (typeof opt === 'function' && done === undefined) {
    done = opt;
    opt = {};
  }

  // Start
  debug('starting wrkr_mongodb');
  this.db = mongoose.createConnection();
  this.db.open(this.dbConnectStr, this.opt.dbOpt, function (err) {
    if (err) return done(err);

    // Make models
    self.Subscription = self.db.model('wrkr_subscriptions', new mongoose.Schema(subscriptionSchema));
    self.QueueItems   = self.db.model('wrkr_qitems', new mongoose.Schema(qItemsSchema));

    debug('connected to mongodb');
    return done();
  });
};


WrkrMongodb.prototype.stop = function stop(done) {
  debug('stopping wrkr_mongodb');

  if (this.pollTimer) {
    clearTimeout(this.pollTimer);
    this.pollTimer = null;
  }

  // TODO: we should wait untill the currently in-process qitem is done
  this.db.close(done);
};


WrkrMongodb.prototype.queuesForEvent = function queuesForEvent(eventName, done) {

  // TODO: use memoize to cache this
  this.Subscription.findOne({eventName: eventName}, function (err, event) {
    if (err) return done(err);

    var queues = (event && event.queues) ? event.queues : [];
    debug('queuesForEvent', eventName, queues);
    return done(null, queues);
  });
}



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
}


WrkrMongodb.prototype.emit = function emit(events, done) {
  var self = this;
  var queueItems = [];

  // For each event, get the
  async.eachLimit(events, 5, sendEvent, done);
  return;

  // Fetc

  function sendEvent(event, cb) {
    if (!event.name) throw new Error('emit: missing event.name');

    async.waterfall([getQueues, sendQueueItem], done);
    return;

    function getQueues(cb) {
      self.queuesForEvent(event.name, cb);
    }
    function sendQueueItem(queues, cb) {
      // Note, mongoose has no single-operation batch insert
      //  so we go the mongo-native way here
      queues.forEach(function (q) {
        queueItems.push({
          queue:      q,
          created:    new Date(),
          when:       event.when || new Date(),
          name:       event.name,
          tid:        event.tid,
          headers:    event.headers || {},
          payload:    event.payload || {},
        });
      });
      debug('emit', queueItems);

      // If there are items to insert, insert them (0 items === error)
      if (queueItems.length === 0) return cb();
      self.QueueItems.collection.insert(queueItems, cb);
    }
  }
};


WrkrMongodb.prototype.listen = function listen(done) {
  var self = this;
  var listenQueues = Object.keys(self.listenQueues);
  debug('listen', listenQueues);

  // Start polling
  debug('listen - start polling - polltimer: started');
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
    var searchEventSpec = {
      when: {$lte: new Date()},
      queue: {$in: listenQueues}
    };
    var sort = {
      created: 1
    };
    var updateSpec = {
      // TODO: do we really want to reschedule as default,
      //  or keep a 'processing/parked' stage?
      when: new Date(Date.now() + self.opt.errorRetryTime)
    }

    // debug('polling', searchEventSpec, updateSpec, sort);
    self.QueueItems.findOneAndUpdate(searchEventSpec, updateSpec, {sort: sort}, function (err, event) {
      // debug('query result', err, event);
      if (err) throw err;
      if (!event) {
        return updatePollTimer(self.opt.pollInterval);
      }

      // There's, work, now we can start a nice waterfall;
      async.waterfall([dispatch, updateStatus], onDone);
      return;

      function dispatch(cb) {
        debug('dispatching', event);

        self.wrkr._dispatch(event, function (err) {
          debug('dispatched', event.name);
          return cb(err, event);
        });
      }
      function updateStatus(event, cb) {
        let updateSpec = {
          $unset: { when: 1 },
          $set:   { done: new Date() }
        };
        self.QueueItems.findByIdAndUpdate(event.id, updateSpec, {new: 1}, function (err, savedEvent) {
          if (err) throw err;
          debug('marked as processed', savedEvent);
          return cb(null, savedEvent)
        });
      }
      function onDone(err, savedEvent) {
        // Get next item with quick-fetch timer
        updatePollTimer(self.opt.pollIntervalBusy);

        // TODO: push errors on the errorList of the queueitem

        // For testing, waiting for the eventEmitter interface
        if (err) throw err;

        // done = no callback!
      }
    });
  }
};




// Exports
//
module.exports = WrkrMongodb;
