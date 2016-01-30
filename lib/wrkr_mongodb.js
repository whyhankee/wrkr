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
  var self = this;
  opt = opt || {};
  this.opt = {};
  this.wrkr = null;
  this.db = null;

  // the server should listen to queues this service has subscribed to
  this.listenQueues = {};

  // the poll-timer, so that we can stop it;
  this.pollTimer = null;

  // track inProcess qitem-state
  //    also tells us if an event is being processed right now
  this.currentEvent = null;

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

  // Start
  debug('starting wrkr_mongodb :'+this.dbConnectStr);
  this.db = mongoose.createConnection();
  this.db.open(this.dbConnectStr, this.opt.dbOpt, function (err) {
    if (err) return done(err);

    // Make models
    self.Subscription = self.db.model(
      'wrkr_subscriptions',
      new mongoose.Schema(subscriptionSchema)
    );
    self.QueueItems   = self.db.model(
      'wrkr_qitems',
      new mongoose.Schema(qItemsSchema)
    );

    debug('connected to mongodb');
    return done();
  });
};


WrkrMongodb.prototype._waitForQitem = function _waitForQitem(done) {
  var self = this;
  if (this.currentEvent === null) return done();

  var waitInterval = setInterval(checkProcessing, 20);
  function checkProcessing() {
    debug(' stop() .. waiting for in process qitem');
    if (self.currentEvent === null) {
      clearInterval(waitInterval);
      return done();
    }
  }
};


WrkrMongodb.prototype.stop = function stop(done) {
  var self = this;
  debug('stopping wrkr_mongodb');

  // Stop pollTimer (if started)
  //    this.pollTimer = null indicates that we are stopping, poll() should not set a new timeout
  if (this.pollTimer) {
    clearTimeout(this.pollTimer);
    this.pollTimer = null;
  }

  // Wait until the currently in-process qitem is done
  //  TODO: fix this nasty thing, causes errors when stopping (on tests)
  this._waitForQitem(function () {
    self.db.close(done);
  });
};


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


WrkrMongodb.prototype.publish = function publish(events, done) {
  var self = this;
  var queueItems = [];

  // For each event, get the
  return async.eachLimit(events, 5, createQueueItems, function (err) {
    if (err) return done(err);

    debug('publish', queueItems);

    // If there are items to insert, insert them (0 items === error)
    if (queueItems.length === 0) return done();

    // Note, mongoose has no single-operation batch insert
    //  so we go the mongo-native way here
    self.QueueItems.collection.insert(queueItems, done);
  });

  function createQueueItems(event, cb) {
    if (!event.name) return cb(new Error('publish: missing event.name'));

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
  }
};


WrkrMongodb.prototype.listen = function listen(done) {
  var self = this;
  var listenQueues = Object.keys(self.listenQueues);
  debug('listen', listenQueues);

  // Start polling
  debug('listen - start polling - polltimer: started');

  // Avoid initial 'stoppage detected' mechanism
  self.pollTimer = Date.now();

  // Start poll()
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
    var updateSpec = {
      when: new Date(Date.now() + self.opt.errorRetryTime)
    };
    var sortOptions = {sort: sort};
    // debug('polling', searchSpec, updateSpec, sort);
    self.QueueItems.findOneAndUpdate(searchSpec, updateSpec, sortOptions, function (err, event) {
      if (err) throw err;

      // No more events to process, go to regular polling interval
      if (!event) return updatePollTimer(self.opt.pollInterval);

      // Work found, now we can start a nice waterfall;
      async.waterfall([dispatch, updateStatus], onDone);
      return;

      function dispatch(cb) {
        // Add regular id, hide mongo's _id
        var eventObj = event.toObject();
        eventObj.id = event.id;
        delete eventObj._id;

        debug('dispatching', eventObj);

        self.currentEvent = eventObj.name;
        self.wrkr._dispatch(eventObj, function (err) {
          return cb(err, event);
        });
      }

      function updateStatus(event, cb) {
        let updateSpec = {
          $unset: { when: 1 },
          $set:   { done: new Date() }
        };
        let opt = {new: 1};
        self.QueueItems.findByIdAndUpdate(event.id, updateSpec, opt, function (err, savedEvent) {
          if (err) return cb(err);

          debug('marked as processed', savedEvent);
          return cb(null, savedEvent);
        });
      }

      function onDone(/* err, savedEvent */) {
        // mark 'no qitem in process'
        self.currentEvent = null;

        // TODO: push errors on the errorList of the queueitem
        //    The worker should have reported the errors

        // Get next item with quick-fetch timer
        updatePollTimer(self.opt.pollIntervalBusy);

        // done (no callback!)
      }
    });
  }
};



/********************************************************************
  Internal functions
 ********************************************************************/

WrkrMongodb.prototype._queuesForEvent = function _queuesForEvent(eventName, done) {

  // TODO: use memoize to cache this
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
