# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduction for SS-532: dropping and recreating a constraint
# upstream with the same shape as before puts the Materialize table into a
# permanent error, even though the final upstream shape matches what was
# recorded.
#
# Postgres keys carry the constraint oid,
# so the recreate is detected. The problem is that there is no recovery
# path short of recreating the Materialize table.
#
# This file asserts the current behavior. The stall marked SS-532 is the
# behavior the issue wants changed.

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET pg_schema_validation_interval = '2s'

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
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE recreate_uq (id int NOT NULL, wallet text NOT NULL, CONSTRAINT recreate_uq_pk PRIMARY KEY (id), CONSTRAINT recreate_uq_wallet UNIQUE (wallet));
INSERT INTO recreate_uq VALUES (1, 'a');
ALTER TABLE recreate_uq REPLICA IDENTITY FULL;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE recreate_uq FROM SOURCE mz_source (REFERENCE recreate_uq);

> SELECT * FROM recreate_uq;
1 a

#
# Drop a UNIQUE constraint, write and remove a row that violates it, then
# re-add it under the same name over the same columns. All of it commits in
# one transaction, so the periodic schema validator only ever sees the final
# shape, which matches what Materialize recorded.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
ALTER TABLE recreate_uq DROP CONSTRAINT recreate_uq_wallet;
INSERT INTO recreate_uq VALUES (2, 'a');
DELETE FROM recreate_uq WHERE id = 2;
ALTER TABLE recreate_uq ADD CONSTRAINT recreate_uq_wallet UNIQUE (wallet);
INSERT INTO recreate_uq VALUES (3, 'b');
COMMIT;

# SS-532: the new constraint has a new oid, so the recreate is reported as an
# alteration and the table stalls permanently, although the final shape is
# identical to the recorded one.
! SELECT * FROM recreate_uq;
regex:incompatible schema change on public\.recreate_uq \(oid \d+\): UNIQUE constraint "recreate_uq_wallet" \(wallet\) was renamed or recreated upstream

> DROP SOURCE mz_source CASCADE;
