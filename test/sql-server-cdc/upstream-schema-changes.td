# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Perform various schema updates to the upstream table


$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE foo (id BIGINT IDENTITY(1,1), name VARCHAR(16), value VARCHAR(32));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO foo VALUES ('a', 'apple'), ('b', 'banana');


> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sql_server_src
  FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE foo1 FROM SOURCE sql_server_src (REFERENCE foo);

> SELECT * FROM foo1;
1 a apple
2 b banana

$ sql-server-execute name=sql-server
ALTER TABLE foo ADD meta_col VARCHAR(32);
INSERT INTO foo VALUES ('c', 'cherry', 'wild');

> SELECT * FROM foo1;
1 a apple
2 b banana
3 c cherry

$ sql-server-execute name=sql-server
ALTER TABLE foo ALTER COLUMN meta_col VARCHAR(64);
INSERT INTO foo VALUES ('d', 'date', 'ajwa');

# Altering the newly added `meta_col` column does not brick `foo1` subsource because `foo1` is
# not following/replicating `meta_col`, so schema updates involving it are inconsequential.
> SELECT * FROM foo1;
1 a apple
2 b banana
3 c cherry
4 d date

> DROP TABLE foo1;

# The upstream table `foo` was initially created without the `meta_col` column, as was the CDC
# table.  In the current state, the upstream table now has the `meta_col` column, but the CDC table
# does not.  Source creation must fail in this case as the snapshot and table description would
# contain this column, but the CDC events would not.  Rewinding to the initial LSN could result in
# negative accumulations.


! CREATE TABLE foo2 FROM SOURCE sql_server_src (REFERENCE dbo.foo);
contains:missing columns

> CREATE TABLE foo2 FROM SOURCE sql_server_src (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = (meta_col));
> SELECT * FROM foo2;
1 a apple
2 b banana
3 c cherry
4 d date

# To support the new column without downtime, we can create a new CDC capture instance for the table
# in the upstream SQL server db that will contain the new column.

$ sql-server-execute name=sql-server
USE test;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @capture_instance = 'dbo_foo2', @supports_net_changes = 0;

> CREATE SOURCE sql_server_src2
  FROM SQL SERVER CONNECTION sql_server_conn;

> CREATE SCHEMA v2;

> CREATE TABLE v2.foo2 FROM SOURCE sql_server_src2 (REFERENCE dbo.foo);

> SELECT * FROM v2.foo2;
1 a apple <null>
2 b banana <null>
3 c cherry wild
4 d date ajwa

$ sql-server-execute name=sql-server
ALTER TABLE foo DROP COLUMN id;

# DROP COLUMN stalls the source
! SELECT * FROM foo2;
contains:incompatible schema change on dbo.foo: column "id" was dropped, renamed, or recreated upstream

> DROP TABLE foo2;

# The id column was dropped, so it exists in the CDC table, but not in the upstream.
# The SQL Server source decodes rows using column name instead of index, and since the id column
# will not exist in the table description, it will not be read from the CDC rows.
> CREATE TABLE foo3 FROM SOURCE sql_server_src (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = (meta_col));
> SELECT * FROM foo3;
a apple
b banana
c cherry
d date

> DROP TABLE foo3;

$ sql-server-execute name=sql-server
USE test;
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'foo', @capture_instance = 'all';
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;

> CREATE TABLE foo4 FROM SOURCE sql_server_src (REFERENCE dbo.foo);
> SELECT * FROM foo4;
a apple <null>
b banana <null>
c cherry wild
d date ajwa

$ sql-server-execute name=sql-server
INSERT INTO foo VALUES ('e', 'elderberry', 'montypython');

> SELECT * FROM foo4;
a apple <null>
b banana <null>
c cherry wild
d date ajwa
e elderberry montypython

$ sql-server-execute name=sql-server
CREATE TABLE bar (id BIGINT IDENTITY(1,1), name VARCHAR(16), value VARCHAR(32), age BIGINT, location VARCHAR(32), meta_col VARCHAR(32));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'bar', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO bar VALUES ('a', 'apple', 5, 'orchard', 'blah'), ('b', 'banana', 7, 'tree', 'blahblah');

> CREATE TABLE bar1 FROM SOURCE sql_server_src (REFERENCE dbo.bar) WITH (EXCLUDE COLUMNS = (meta_col, name));

$ sql-server-execute name=sql-server
ALTER TABLE bar DROP COLUMN meta_col;

# Dropping an excluded column is okay
> SELECT * FROM bar1;
1 apple 5 orchard
2 banana 7 tree

$ sql-server-execute name=sql-server
ALTER TABLE bar DROP COLUMN name, location;

# Dropping a non-excluded column stalls the source
! SELECT * FROM bar1;
contains:incompatible schema change on dbo.bar: column "location" was dropped, renamed, or recreated upstream

#
# Constraint changes. SQL Server records none of these in cdc.ddl_history, so
# they are found by the periodic schema validation rather than by a DDL event.
#

$ sql-server-execute name=sql-server
CREATE TABLE addk (id INT NOT NULL CONSTRAINT addk_pk PRIMARY KEY, wallet VARCHAR(64) NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'addk', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO addk VALUES (1, 'a');
CREATE TABLE nn (id INT NOT NULL CONSTRAINT nn_pk PRIMARY KEY, note VARCHAR(64), wallet VARCHAR(64) NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'nn', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO nn VALUES (1, NULL, 'a');
CREATE TABLE uq (id INT NOT NULL CONSTRAINT uq_pk PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT uq_wallet UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'uq', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO uq VALUES (1, 'a');
CREATE TABLE readd (id INT NOT NULL CONSTRAINT readd_pk PRIMARY KEY, f2 VARCHAR(10));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'readd', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO readd VALUES (1, 'one');

> CREATE TABLE addk FROM SOURCE sql_server_src (REFERENCE dbo.addk);
> CREATE TABLE nn FROM SOURCE sql_server_src (REFERENCE dbo.nn);
> CREATE TABLE uq FROM SOURCE sql_server_src (REFERENCE dbo.uq);
> CREATE TABLE readd FROM SOURCE sql_server_src (REFERENCE dbo.readd);

# Adding a UNIQUE constraint upstream, and dropping it again, is not a change:
# it was never recorded as a key.
$ sql-server-execute name=sql-server
ALTER TABLE addk ADD CONSTRAINT addk_wallet UNIQUE (wallet);
INSERT INTO addk VALUES (2, 'b');
ALTER TABLE addk DROP CONSTRAINT addk_wallet;
INSERT INTO addk VALUES (3, 'b');

> SELECT * FROM addk;
1 a
2 b
3 b

> SELECT * FROM nn;
1 <null> a

# Adding a NOT NULL constraint upstream only narrows the data and is not a change.
$ sql-server-execute name=sql-server
UPDATE nn SET note = 'n';
ALTER TABLE nn ALTER COLUMN note VARCHAR(64) NOT NULL;
INSERT INTO nn VALUES (2, 'm', 'b');

> SELECT * FROM nn;
1 n a
2 m b

# Dropping a NOT NULL constraint stalls the table.
$ sql-server-execute name=sql-server
ALTER TABLE nn ALTER COLUMN wallet VARCHAR(64) NULL;

! SELECT * FROM nn;
contains:incompatible schema change on dbo.nn: the NOT NULL constraint on column "wallet" was dropped upstream
hint:WITH (EXCLUDE ALL CONSTRAINTS)

# Dropping a UNIQUE constraint stalls the table. The hint names the constraint
# so a planned drop can be excluded up front, and is surfaced in the status.
$ sql-server-execute name=sql-server
ALTER TABLE uq DROP CONSTRAINT uq_wallet;

! SELECT * FROM uq;
contains:incompatible schema change on dbo.uq: UNIQUE constraint "uq_wallet" (wallet) was dropped upstream
hint:WITH (EXCLUDE CONSTRAINTS ('uq_wallet')) before the upstream drop

> SELECT details->'hints'->>0 LIKE '%EXCLUDE CONSTRAINTS (''uq_wallet'')%' FROM mz_internal.mz_source_statuses WHERE name = 'uq';
true

# Dropping a column and adding one back under the same name is not a no-op:
# the change table keeps the old column and receives NULL for it from then on.
$ sql-server-execute name=sql-server
ALTER TABLE readd DROP COLUMN f2;
ALTER TABLE readd ADD f2 VARCHAR(10);

! SELECT * FROM readd;
contains:incompatible schema change on dbo.readd: column "f2" was dropped, renamed, or recreated upstream

> DROP SOURCE sql_server_src CASCADE;
