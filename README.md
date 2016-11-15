## Worker - Experimenting with NodeJS pub-sub, post-processing and scheduling.

### Current state

* **Disclaimer: In maintenance**.

* FollowUp project <https://github.com/whyhankee/dbwrkr>

### General idea in code

*We make a worker and connect to 'some storage', in this case MongoDB*
```
var wrkr = require('wrkr');

var wrkr = new wrkr.Wrkr({
	backend: new WrkrMongodb()
});
wrkr.start(function (err) {
	if (err) throw err;
});
```

*When some work is done, we publish an event about what we did:*

```
// some user decided to sign-up for our application
//   we stored the user in the database
wrkr.publish({
  name: 'ourapp.user.added',
  tid:  idOfUser
});
```

*Some service/application might want to followUp on that event, in this case, our Customer Satisfaction Team*

It subscribes a queue to the event and passes the function that will handle the event. The subscription will be registered in the database and the queue will start to receive the events. The handler will be called when events arrive. Acknowledge the event as handled when you want, before or after processing.

```
ourWorkQueue = 'ourApp.CustomerSatisfactionTeam'
wrkr.subScribe(ourWorkQueue, 'ourapp.user.added', onUserAdded, function (err) {
  if (err) throw err;
});

function onUserAdded(event, ack) {
  // AddUserMetrics({userId: event.tid});
  // sendWarmWelcomeEmail({userId: event.tid});

  // followUp in a week
  wrkr.publish({
    name: ourapp.cst.user.welcome.followup,
    tid: event.tid,
    when: new Date(Date.now()+human-interval('7 days'))
  });

  // Let others know we did the followUp
  wrkr.publish({
    name: 'ourapp.cst.user.sentWelcomeEmail',
    tid: event.tid
  });

  return ack();
};
```

## Testing, developing and debugging

The Wrkr module itself has no tests. This would either require mocking, or create an in-momory storage engine. I would rather spend to the energy in the latter. The wrkr-tests uses the Wrkr for running an engine tests.

So, with a stable engine (mongodb) you could test this module by running the engine tests.


## TODO:

* Stabilize API, tests, functionality
  * subscriptions based on patterns (e.g. regular expressions)

* Make MemoryBackend
  * for testing the Wkr itself
  * .. and other tests ofc.

## Changelog

v0.0.12

  * Fix delete-spec now includes queue (was deleting unprocessed items)
  * Added debug statements for start() & stop()

v0.0.11

  * getQueueItems - Changed Qitem minimal find spec (fix, take two)

v0.0.10

  * getQueueItems - Changed minimal find spec

v0.0.9

  * Separating the tests was a bad idea, reverting this.

v0.0.8

  * Seperated mongodb and tests in their own packages. Take a look at the mongodb-engine to see how it works.

v0.0.7 and before

  * Initial work ..
