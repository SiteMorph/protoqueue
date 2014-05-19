protoqueue A lightweight task / queue system
============================================

A very simple task queueing library which uses the SiteMorph/protostore backing
to support prefix matched queue events and scheduling time. The task queue has
a few features that are useful:

- Persistence of tasks.
- At least once execution.
- Future scheduled execution.
- Retry forever (or mark done).
- Path prefix registration event listeners.
- Multiple task listener support.
- All done / some failed roll back call (Cohort agrees).
- Makes use of task 'vectors' to signal that a held task has been completed by
  another task worker set.
- Check if task is already queued (supports at most once semantics).

The proto queue depends on the protostore project. Before you can build the
queue you must first clone protostore and run mvn intall.

The architecture of the system is somewhat different from a traditional queue
in that all of the coordinators read from the queue and trigger task workers.
As such there can be an unbounded number of orchestrators reading / writing to
the queue. From the point of view of a task listener, it is doing some work but
can consider it's 'window' to work unique across all orchestrators as tasks are
claimed.

To use the code perform a local install by running the maven install target:

mvn install

To run tests use the maven test target

mvn test

Code is laid out in standard maven directory structure. The SQL in src/main
provides a template for a task queue table used to store the crud task queue
data.

Note that this project is in the initial phases and should be considered
experimental.

Note that this project uses linux specific protoc commands for protobuf 
compilation. 

Note that you must have protobuf compiler installed.

See unit tests for usage. src/test/java/

Note that logging is via slf4j. Please use your relevant adapter if you 
require logging.

Future Work
-----------
Task claims using newer protostore task vector to claim and error on roll back.