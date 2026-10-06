# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Part 1 of the offset_committed_seed workflow: two independent PostgreSQL
# sources, over t1 and t3, and a load generator source over bids, each with a
# reported committed offset. The sources advance their uppers every 50ms, so
# the slot position, which trails by up to one 1s feedback interval, is almost
# always behind the committed LSN when a table is added.

$ set-sql-timeout duration=60s

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;
DROP PUBLICATION IF EXISTS mz_source_b;
CREATE TABLE t1 (id SERIAL PRIMARY KEY, v TEXT);
CREATE TABLE t2 (id SERIAL PRIMARY KEY, v TEXT);
CREATE TABLE t3 (id SERIAL PRIMARY KEY, v TEXT);
CREATE TABLE t4 (id SERIAL PRIMARY KEY, v TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;
ALTER TABLE t2 REPLICA IDENTITY FULL;
ALTER TABLE t3 REPLICA IDENTITY FULL;
ALTER TABLE t4 REPLICA IDENTITY FULL;
INSERT INTO t1 (v) SELECT 'x' FROM generate_series(1, 100);
INSERT INTO t2 (v) SELECT 'y' FROM generate_series(1, 100);
INSERT INTO t3 (v) SELECT 'x' FROM generate_series(1, 100);
INSERT INTO t4 (v) SELECT 'y' FROM generate_series(1, 100);
CREATE PUBLICATION mz_source FOR TABLE t1, t2;
CREATE PUBLICATION mz_source_b FOR TABLE t3, t4;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  WITH (TIMESTAMP INTERVAL '50ms');
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

> CREATE SOURCE mz_source_b
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source_b')
  WITH (TIMESTAMP INTERVAL '50ms');
> CREATE TABLE t3 FROM SOURCE mz_source_b (REFERENCE t3);

> SELECT count(*) FROM t1
100

> SELECT count(*) FROM t3
100

> CREATE SOURCE auction FROM LOAD GENERATOR AUCTION (TICK INTERVAL '100ms');
> CREATE TABLE bids FROM SOURCE auction (REFERENCE bids);

> SELECT count(*) > 0 FROM bids
true

# Both tables have reported a committed offset that a later seed could undercut.
> SELECT t.name, bool_and(u.offset_committed > 0)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('t1', 't3', 'bids')
  GROUP BY t.name
bids true
t1 true
t3 true
