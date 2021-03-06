-- Prototype of a task queue managed by the crud urn backed storage.

CREATE TABLE TaskQueue (
  urn VARCHAR(36) NOT NULL PRIMARY KEY,
  path VARCHAR(2048) NOT NULL,
  runTime BIGINT NOT NULL,
  data VARCHAR(4096),
  vector BIGINT NOT NULL,
  claim VARCHAR(255),
  claimTimeout BIGINT
) CHARACTER SET utf8;