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


var queueSchema = {
  queue:      {type: String, index: true},
  when:       {type: Date, index: true, sparse: true},
  done:       {type: Date, index: true, sparse: true},

  eventName:  {type: String},
  tid:        {type: String, index: true},

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

   pollInterval:      333,     // regular polling time (waiting for new items)
   pollIntervalBusy:  5,       // quick-fetch-next-polling-interval after processing an item

   errorRetryTime:    5000,    // on error retry timer
 };


function WrkrMongodb(opt) {
  var self = this;
  opt = opt || {};
  this.opt = {};
  this.wrkr = null;
  this.db = null;

  // the server should listen to queues this service has subscribed to
  this.listenQueues = [];

  // the poll-timer, so that we can stop it;
  this.pollTimer = null;

  // Mongoose Models
  this.Subscription = null;
  this.Queue = null;

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
    self.Subscription = self.db.model('subscriptions', new mongoose.Schema(subscriptionSchema));
    self.Queue        = self.db.model('queues', new mongoose.Schema(queueSchema));

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

  this.db.close(done);
};


WrkrMongodb.prototype.queuesForEvent = function queuesForEvent(eventName, done) {
  this.Subscription.findOne({eventName: eventName}, function (err, event) {
    if (err) return done(err);

    var queues = (event && event.queues) ? event.queues : [];
    debug('queuesForEvent', eventName, queues, typeof done);
    return done(null, queues);
  });
}



WrkrMongodb.prototype.subscribe = function (queueName, eventName, done) {
  debug('subscribe', queueName, eventName);
  this.listenQueues.push(queueName);

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
    async.waterfall([getQueues, sendQueueItem], done);
    return;

    function getQueues(cb) {
      self.queuesForEvent(event.name, cb);
    }
    function sendQueueItem(queues, cb) {
      queues.forEach(function (q) {
        queueItems.push({
          queue:     q,
          when:      event.when || new Date(),
          eventName: event.name,
          tid:       event.tid,
          data:      event.d || null
        });
      });
      debug('emit', queueItems);
      self.Queue.collection.insert(queueItems, cb);
    }
  }
};


WrkrMongodb.prototype.listen = function listen(done) {
  var self = this;

  debug('listen', self.listenQueues);

  // setup polling - start with busy polling interval
  //    goes to regular polling speed if there are no items left
  debug('polltimer: started', this.opt.pollIntervalBusy);
  this.pollTimer = setTimeout(poll, this.opt.pollIntervalBusy);
  return done(null);

  function updatePollTimer(timeoutMs) {
    debug('polltimer: update', timeoutMs);
    if (self.pollTimer === null) {
      return debug('listen - updatePollTimer: detected stoppage');
    }
    self.pollTimer = setTimeout(poll, timeoutMs);
  }

  function poll() {
    var searchEventSpec = {
      when: {$lte: new Date()},
      queue: {$in: self.listenQueues}
    };
    var updateSpec = {
      when: new Date(Date.now() + self.opt.errorRetryTime)
    };
    debug('polling', searchEventSpec, updateSpec);
    self.Queue.findOneAndUpdate(searchEventSpec, updateSpec, function (err, event) {
      debug('query result', err, event.id, event.eventName);
      if (err) throw err;
      if (!event) {
        return updatePollTimer(self.opt.pollInterval);
      }
      debug('dispatching', event);
      self.wrkr._dispatch(event, function (err) {
        // For testing
        if (err) throw err;

        // TODO: push errors back to the qitem

        // Mark item as done
        let updateSpec = {
          $unset: { when: 1 },
          $set:   { done: new Date() }
        };
        self.Queue.findByIdAndUpdate(event.id, updateSpec, {new: 1}, function (err, savedEvent) {
          if (err) throw err;

          debug('marked as processed', savedEvent);
          // done (no callback)
        });

        // Get next item with quick-fetch timer
        return updatePollTimer(self.opt.pollIntervalBusy);
      });
    });
  }
};




// Exports
//
module.exports = WrkrMongodb;
