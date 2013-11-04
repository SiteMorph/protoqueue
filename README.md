protoqueue
==========

A very simple queueing library which uses the SiteMorph/protostore backing to
support prefix matched queue events and scheduling time.

To use the code perform a local install by running the maven install target:

mvn install

To run tests use the maven test target

mvn test

Code is laid out in standard maven directory structure. The SQL in src/main
provides a template for a task queue table used to store the crud task queue
data.

Note that this project is in the initial phases and has only had limited test.

Note that this project uses linux specific protoc commands for protobuf 
compilation. 

Note that you must have protobuf compiler installed.

See unit tests for usage. src/test/java/

Note that logging is via slf4j. Please use your relevant adapter if you 
require logging.
