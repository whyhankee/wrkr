## Worker - some experimenting with NodeJS eventSourcing

### General idea

  *We make a worker and connect to 'some storage', in this case MongoDB*

 	var wrkr = new Wrkr({
  		store: new WrkrMongodb()
	});
	wrkr.start(function (err) {
		if (err) throw err;
	});

  *When some work is done, we publish an event about what we did:*

    // some user decided to sign-up for our application
    //   we stored the user in the database
    var whatDidWeDo = 'ourapp.user.added';
    var IdOfUser: randomString();
    wrkr.emit({
      name: whatDidWeDo,
      tid:  idOfUser
    });

  *Some service/application might want to followUp on that event:*

    ourWorkQueue = 'ourApp.CustomerSatisfactionTeam'
    wrkr.subScribe(ourWorkQueue, 'ourapp.user.added', onUserAdded, function (err) {
      if (err) throw err;
    });

    function onUserAdded(event, done) {
      // sendWarmWelcomeEmail({userId: event.tid});

      // followUp in a week
      wrkr.emit({
        name: ourapp.user.followup,
        tid: event.tid,
        when: '7 days'
      });

      // Let others know we did the followUp
      wrkr.emit({
        name: 'ourapp.cst.user.followedUp',
        tid: event.tid
      });

      return done();
    };

  *Someone else want to do some metrics on the entire system:*

    wrkr.subScribe('metrics', /ourapp.*/, updateAppStats, function (err) {
      if (err) throw err;
    });)

    function updateAppStats(event, done) {
      // do something
      return done();
    };

### ... we do this because?

  * Loose coupling - This way, by reacting to others events you create little components that are easy to maintain.
  * By using a shared storage engine it's easy to scale for redundancy, it up you to choose the communication that is best for your environment, each will have their benefits.

  Storage engine ideas / thinking about:

  * MongoDB (Currently default because I *need* a MongoDB implementation )

    * Memory (ofcourse, easy for testing)
    * RethinkDB

	How do these work with scheduled events?

    * Redis
    * RabbitMQ / AMQP

	Note: A shared testing framework/solution must be devised to have a standard interface tests for storage engines.

### Side note, it's not build for speed, it's for flexibility and ease-of-use.


####  The tests contain some more code, still working on the rest

## Testing and debugging

just run

    npm run tdd

or

    DEBUG=wrkr:* npm run tdd


## TODO:

* wrkr should be EventEmitter based for warnings and errors
* Seperate MongoDB store to it's own package
* Make MemoryStore for testing (or in-app queues)
