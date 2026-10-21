# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduction for SS-532: dropping and recreating a constraint
# upstream with the same shape as before.
#
# Constraint changes are not recorded in `cdc.ddl_history`, so the source
# re-reads each table's PRIMARY KEY and UNIQUE constraints on every poll that
# sees new changes and compares them by name, type, and column set. A drop
# and re-add that completes between two polls is therefore invisible, and
# rows written in the window that violated the key are ingested as if the
# key held at those timestamps. `sys.key_constraints` exposes `object_id`
# and `create_date`, which would tell the recreated constraint apart.
#
# This file asserts the current behavior. The outcome marked SS-532 is the
# behavior the issue wants changed.

$ set-sql-timeout duration=60s

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;

CREATE TABLE recreate_uq (id INT NOT NULL CONSTRAINT recreate_uq_pk PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT recreate_uq_wallet UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'recreate_uq', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO recreate_uq VALUES (1, 'a');

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sql_server_src FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE recreate_uq FROM SOURCE sql_server_src (REFERENCE dbo.recreate_uq);

> SELECT * FROM recreate_uq;
1 a

# Materialize planned with the key, so a DISTINCT over it is free.
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT wallet FROM recreate_uq;
Explained Query:
  Project (#1)
    ReadStorage materialize.public.recreate_uq

Source materialize.public.recreate_uq

Target cluster: quickstart

#
# Drop a UNIQUE constraint, write and remove a row that violates it, then
# re-add it under the same name over the same columns. All of it commits in
# one transaction, so no poll can observe the intermediate state.
#

$ sql-server-execute name=sql-server split-lines=false
BEGIN TRANSACTION;
ALTER TABLE recreate_uq DROP CONSTRAINT recreate_uq_wallet;
INSERT INTO recreate_uq VALUES (2, 'a');
DELETE FROM recreate_uq WHERE id = 2;
ALTER TABLE recreate_uq ADD CONSTRAINT recreate_uq_wallet UNIQUE (wallet);
INSERT INTO recreate_uq VALUES (3, 'b');
COMMIT;

# SS-532: the drop and the re-add are invisible. The table keeps ingesting,
# and at the timestamps between the insert and the delete of id 2 it held
# two rows with wallet 'a' under a schema that promised wallet is unique.
> SELECT * FROM recreate_uq;
1 a
3 b

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'recreate_uq';
running

> DROP SOURCE sql_server_src CASCADE;
