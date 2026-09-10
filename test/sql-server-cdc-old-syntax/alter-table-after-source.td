# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test ALTER TABLE -- source will error out for tables which existed when the source was created
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}';

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE;
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;


$ sql-server-execute name=sql-server
CREATE TABLE add_columns (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'add_columns', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO add_columns VALUES (1);

CREATE TABLE remove_column (f1 INTEGER, f2 VARCHAR(2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'remove_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO remove_column VALUES (2, 'ab');

CREATE TABLE alter_column (f1 INTEGER, f2 VARCHAR(2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_column VALUES (2, 'ab');

CREATE TABLE alter_drop_nullability (f1 INTEGER NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_drop_nullability', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_drop_nullability VALUES (1);

CREATE TABLE alter_add_nullability (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_add_nullability', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_add_nullability VALUES (1);

CREATE TABLE alter_add_pk (f1 INTEGER NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_add_pk', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_add_pk VALUES (1);

CREATE TABLE alter_drop_unique (f1 INTEGER, CONSTRAINT alter_drop_unique_f1_key UNIQUE (f1));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_drop_unique', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_drop_unique VALUES (1);

CREATE TABLE alter_add_unique (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_add_unique', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_add_unique VALUES (1);

CREATE TABLE alter_extend_column (f1 VARCHAR(2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_extend_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_extend_column VALUES ('ab');

CREATE TABLE alter_decimal (f1 DECIMAL(5,2));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_decimal', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_decimal VALUES (123.45);

CREATE TABLE alter_table_rename (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_rename', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_rename VALUES (1);

CREATE TABLE alter_table_rename_column (f1 VARCHAR(10), f2 VARCHAR(10));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_rename_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_rename_column (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_change_attnum (f1 VARCHAR(10), f2 VARCHAR(10));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_change_attnum', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_change_attnum (f1, f2) VALUES ('f1_orig','f2_orig');

CREATE TABLE alter_table_supported (f1 int, f2 int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_table_supported', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO alter_table_supported (f1, f2) VALUES (1, 1);

CREATE TABLE truncate_table (f1 int, f2 int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'truncate_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO truncate_table (f1, f2) VALUES (1, 1);

CREATE TABLE drop_table (f1 int, f2 int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'drop_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO drop_table (f1, f2) VALUES (1, 1);


> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

#
# Add column

> SELECT * FROM add_columns;
1

$ sql-server-execute name=sql-server
ALTER TABLE add_columns ADD f2 varchar(2);
INSERT INTO add_columns VALUES (2, 'ab');

> SELECT * from add_columns;
1
2


#
# Remove column

> SELECT * from remove_column;
2 ab

$ sql-server-execute name=sql-server
ALTER TABLE remove_column DROP COLUMN f2;

INSERT INTO remove_column VALUES (3);

! SELECT * from remove_column;
contains:incompatible schema change on dbo.remove_column: column "f2" was dropped, renamed, or recreated upstream


#
# Alter column type

> SELECT * from alter_column;
2 ab

$ sql-server-execute name=sql-server
ALTER TABLE alter_column ALTER COLUMN f2 CHAR(2);
INSERT INTO alter_column VALUES (3, 'cd');

! SELECT * from alter_column;
contains:incompatible schema change on dbo.alter_column: the type of column "f2" changed upstream

#
# Drop NOT NULL

> SELECT * from alter_drop_nullability
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_drop_nullability ALTER COLUMN f1 INTEGER NULL;

! SELECT * FROM alter_drop_nullability WHERE f1 IS NOT NULL;
contains:incompatible schema change on dbo.alter_drop_nullability: the NOT NULL constraint on column "f1" was dropped upstream

# We have guaranteed that this column is not null so the optimizer eagerly
# returns the empty set.
> SELECT * FROM alter_drop_nullability WHERE f1 IS NULL;


#
# Add NOT NULL

> SELECT * from alter_add_nullability
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_add_nullability ALTER COLUMN f1 INTEGER NOT NULL;
INSERT INTO alter_add_nullability VALUES (1);

# Adding a NOT NULL constraint upstream only narrows the data and is not a change.
> SELECT * FROM alter_add_nullability;
1
1

#
# Add PK

> SELECT * from alter_add_pk
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_add_pk ADD PRIMARY KEY(f1);
INSERT INTO alter_add_pk VALUES (2);

> SELECT * FROM alter_add_pk;
1
2


#
# Drop unique

> SELECT * from alter_drop_unique
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_drop_unique DROP CONSTRAINT alter_drop_unique_f1_key;

! SELECT f1 FROM alter_drop_unique;
contains:incompatible schema change on dbo.alter_drop_unique: UNIQUE constraint "alter_drop_unique_f1_key" (f1) was dropped upstream
hint:WITH (EXCLUDE CONSTRAINTS ('alter_drop_unique_f1_key')) before the upstream drop


#
# Add unique
> SELECT * from alter_add_unique
1

$ sql-server-execute name=sql-server
ALTER TABLE alter_add_unique ADD UNIQUE(f1);
INSERT INTO alter_add_unique VALUES (2);

> SELECT * FROM alter_add_unique;
1
2


#
# Extend column


> SELECT * from alter_extend_column
ab

$ sql-server-execute name=sql-server
ALTER TABLE alter_extend_column ALTER COLUMN f1 VARCHAR(20);


! SELECT * FROM alter_extend_column;
contains:incompatible schema change on dbo.alter_extend_column: the type of column "f1" changed upstream


#
# Alter decimal
> SELECT * from alter_decimal
123.45

$ sql-server-execute name=sql-server
ALTER TABLE alter_decimal ALTER COLUMN f1 DECIMAL(6,1);

! SELECT * FROM alter_decimal;
contains:incompatible schema change on dbo.alter_decimal: the type of column "f1" changed upstream


#
# Alter table rename

> SELECT * from alter_table_rename;
1

$ sql-server-execute name=sql-server
EXEC sp_rename 'alter_table_rename', 'alter_table_renamed';

# TODO: Should fail
# ! SELECT * FROM alter_table_rename;
# contains:altered

#
# Change column attnum

> SELECT * from alter_table_change_attnum;
f1_orig f2_orig

# Ensure simple name swap doesn't fool schema detection
$ sql-server-execute name=sql-server
ALTER TABLE alter_table_change_attnum DROP COLUMN f2;
ALTER TABLE alter_table_change_attnum ADD f2 VARCHAR(10);

! SELECT * FROM alter_table_change_attnum;
contains:incompatible schema change on dbo.alter_table_change_attnum: column "f2" was dropped, renamed, or recreated upstream

> SELECT * from alter_table_supported;
1 1

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_supported ADD f3 int;
INSERT INTO alter_table_supported (f1, f2, f3) VALUES (2, 2, 2);

> SELECT * from alter_table_supported;
1 1
2 2

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_supported DROP COLUMN f3;
INSERT INTO alter_table_supported (f1, f2) VALUES (3, 3);

> SELECT * from alter_table_supported;
1 1
2 2
3 3

$ sql-server-execute name=sql-server
ALTER TABLE alter_table_supported DROP COLUMN f2;

! SELECT * from alter_table_supported;
contains:incompatible schema change on dbo.alter_table_supported: column "f2" was dropped, renamed, or recreated upstream

#
# Drop table

> SELECT * from drop_table;
1 1

$ sql-server-execute name=sql-server
DROP TABLE drop_table;

# The CDC stream fails on the missing change table, and schema validation
# reports the capture instance as gone. Whichever lands first stalls the table.
> SELECT
    status,
    error like '%Invalid object name%' or error like '%no longer captured by capture instance%' as expected_error
  FROM mz_internal.mz_source_statuses
  WHERE name = 'drop_table';
stalled true

> DROP SOURCE mz_source CASCADE;
