# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Part 2 of the offset_committed_seed workflow, run while the workflow holds
# ACCESS EXCLUSIVE locks on the primary key indexes of t2 and t4 and keeps
# inserting into t1 and t3: add a table to each source. The PostgreSQL tables'
# snapshots block on the locks, the load generator replays from offset 0 for
# the new table.

$ set-sql-timeout duration=60s

> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE t2);
> CREATE TABLE t4 FROM SOURCE mz_source_b (REFERENCE t4);

# t2 and t4 are snapshotting and stay so until the locks are released.
> SELECT t.name, bool_and(NOT u.snapshot_committed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('t2', 't4')
  GROUP BY t.name
t2 true
t4 true

> CREATE TABLE accounts FROM SOURCE auction (REFERENCE accounts);

> SELECT count(*) > 0 FROM accounts
true
