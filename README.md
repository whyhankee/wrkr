## Worker - Experimenting with NodeJS pub-sub, post-processing and scheduling.

### What?

  * Experimenting with a pub-sub system that also schedules events for later (post-) processing.

  * The pub-sub system would make it easy for multiple processes to communicate with each other by sending events. Other processes can pickup the events, do what need to do (and let others know by sending an event).

  * This way, by reacting to events you create loose coupling, little components that are easy to maintain, deploy and removed.

  * Create a general use API (Wrkr) that can be used with a backend-plugin that works best for your  environment, each will have their benefits.

  * It's not build for performance, it's for flexibility.

### Current state

  * Disclaimer: This is *very alpha* everything could happen.

  * Wrkr currently only has one backend: MongoDB (and it could really be improved!).

  * It should be easy to implement backend-plugins for other databases.

  * I would also like to implement other messaging solution (e.g. RabbitMQ / MQTT) however that would require a solution for the scheduler part.

  * For further notes see the TODO list below


### General idea in code

  *We make a worker and connect to 'some storage', in this case MongoDB*

    var wrkr = new Wrkr({
  		backend: new WrkrMongodb()
  	});
  	wrkr.start(function (err) {
  		if (err) throw err;
  	});

  *When some work is done, we publish an event about what we did:*

    // some user decided to sign-up for our application
    //   we stored the user in the database
    wrkr.publish({
      name: 'ourapp.user.added',
      tid:  idOfUser
    });

  *Some service/application might want to followUp on that event, in this case, our Customer Satisfaction Team*

  It subscribes a queue to the event and passes the function that will handle the event. The subscription will be registered in the database and the queue will start to receive the events. The handler will be called when events arrive. Acknowledge the event as handled when you want, before or after processing.

    ourWorkQueue = 'ourApp.CustomerSatisfactionTeam'
    wrkr.subScribe(ourWorkQueue, 'ourapp.user.added', onUserAdded, function (err) {
      if (err) throw err;
    });

    function onUserAdded(event, ack) {
      // sendWarmWelcomeEmail({userId: event.tid});

      // followUp in a week
      wrkr.publish({
        name: ourapp.cst.user.welcome.followup,
        tid: event.tid,
        when: '7 days'
      });

      // Let others know we did the followUp
      wrkr.publish({
        name: 'ourapp.cst.user.sentWelcomeEmail',
        tid: event.tid
      });

      return ack();
    };

####  The basic tests contain some more code, still working on the rest

## Testing and debugging

just run

    npm run tdd

or

    DEBUG=wrkr:* npm run tdd


## TODO:

* Implement: When '7 days' (human-interval) syntax

* Fix stop() closing database when a qitem is dispatched, cannot update afterwards

* Move backend tests to its own package so we have a standard interface tests for the backend engines.

* Move MongoDB backend to its own package

* Make MemoryBackend
  * for testing (or in-app queues)

* MongoDB backend - Replace mongoose by regular mongo driver
  Mongoose was easy to setup, it's overkill though

* Think about archiving processed queue-items
  * delete immediately, or, maybe: reuse (after xx time)?
  * cleanup timer
  * Storage: delete or move to another table (or db)

* Implement other backends like RabbitMQ / MQTT
  * Find solution for scheduled events
