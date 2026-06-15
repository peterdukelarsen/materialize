# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

#
# Demonstrates incorrect handling of ALTER / RENAME / TRUNCATE TABLE binlog
# events for upstream tables whose names contain backticks or periods.
#
# The replication DDL handler (`table_ident` in
# src/storage/src/source/mysql/replication/events.rs) takes the table-name
# token of the query, strips *all* backticks, and splits on *every* period,
# keeping only the first two components. But in MySQL a backtick inside a
# quoted identifier is escaped by doubling it (`tab``1` names the table tab`1)
# and a quoted identifier may contain any number of periods (`dot.tbl` is a
# single unqualified name). So:
#
#   ALTER TABLE `tab``1` ...    is looked up as table "tab1"      (wrong name)
#   ALTER TABLE `dot.tbl` ...   is looked up as `dot`.`tbl`       (wrong schema)
#   ALTER TABLE `a.b.c` ...     is looked up as `a`.`b`           (lost ".c")
#
# This same broken `table_ident` parser is used for ALTER, RENAME and TRUNCATE
# (only DROP TABLE goes through a real SQL parser and is handled correctly).
#
# Two distinct failure modes result, both of which corrupt the served data:
#
#   * The misparsed name matches nothing we track, so the DDL goes undetected.
#     The table keeps serving its old rows (RENAME/TRUNCATE) or fails to decode
#     the next row against a now-stale schema (ALTER).
#
#   * The misparsed name collides with a *different* table we do track, so the
#     DDL is attributed to the wrong table. The innocent table is needlessly
#     inspected while the table that actually changed is never checked.
#
# Row data is never misrouted (RowsEvents carry the real db/table names), so
# the corruption is always in the form of undetected schema changes: stale
# data, missing errors, or decode failures -- never rows landing in the wrong
# Materialize table.
#
# This file asserts the CURRENT (incorrect) behavior, next to identically
# treated control tables with plain names that show the correct behavior.
# Once the bug is fixed, the "BUG:" sections should behave like their
# controls and this test must be updated.
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
# A second database used to prove that a dotted name is misparsed into a
# *different*, real table in another schema (the "collision" case below).
DROP DATABASE IF EXISTS coll;
CREATE DATABASE coll;
USE public;

CREATE TABLE control_drop (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO control_drop VALUES (1, 'one');

# Table named tab`1: the backtick is escaped by doubling it.
CREATE TABLE `tab``1` (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO `tab``1` VALUES (1, 'one');

# Table named dot.tbl: a single identifier containing a period.
CREATE TABLE `dot.tbl` (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO `dot.tbl` VALUES (1, 'one');

# Table named a.b.c: a single identifier containing *two* periods. The parser
# splits on every period and keeps only the first two components, silently
# discarding ".c".
CREATE TABLE `a.b.c` (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO `a.b.c` VALUES (1, 'one');

CREATE TABLE control_rename (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO control_rename VALUES (1, 'one');

# Table named re.named`tbl: both a period and an escaped backtick.
CREATE TABLE `re.named``tbl` (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO `re.named``tbl` VALUES (1, 'one');

CREATE TABLE control_trunc (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO control_trunc VALUES (1, 'one');

# Table named trunc.tbl: a single identifier containing a period, used to show
# that TRUNCATE is misparsed exactly like ALTER/RENAME.
CREATE TABLE `trunc.tbl` (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO `trunc.tbl` VALUES (1, 'one');

# Collision setup: `public`.`coll.tbl` is a single dotted identifier, while
# `coll`.`tbl` is a genuinely separate table in the `coll` schema. The parser
# turns the former into the latter, so an ALTER of `public`.`coll.tbl` is
# attributed to the unrelated `coll`.`tbl`.
CREATE TABLE `coll.tbl` (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO `coll.tbl` VALUES (1, 'one');
CREATE TABLE coll.tbl (f1 INTEGER, f2 VARCHAR(8));
INSERT INTO coll.tbl VALUES (100, 'hundred');

# Plain table used to prove the replication stream has advanced past a given
# upstream write.
CREATE TABLE marker (f1 INTEGER);
INSERT INTO marker VALUES (1);

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE control_drop FROM SOURCE mz_source (REFERENCE public.control_drop);
> CREATE TABLE t_backtick FROM SOURCE mz_source (REFERENCE public."tab`1");
> CREATE TABLE t_period FROM SOURCE mz_source (REFERENCE public."dot.tbl");
> CREATE TABLE t_twodots FROM SOURCE mz_source (REFERENCE public."a.b.c");
> CREATE TABLE control_rename FROM SOURCE mz_source (REFERENCE public.control_rename);
> CREATE TABLE t_renamed FROM SOURCE mz_source (REFERENCE public."re.named`tbl");
> CREATE TABLE control_trunc FROM SOURCE mz_source (REFERENCE public.control_trunc);
> CREATE TABLE t_trunc FROM SOURCE mz_source (REFERENCE public."trunc.tbl");
> CREATE TABLE t_collide_dotted FROM SOURCE mz_source (REFERENCE public."coll.tbl");
> CREATE TABLE t_collide_real FROM SOURCE mz_source (REFERENCE coll.tbl);
> CREATE TABLE marker FROM SOURCE mz_source (REFERENCE public.marker);

# The snapshot path resolves these names correctly; only the replication DDL
# handler is broken.
> SELECT * FROM control_drop;
1 one

> SELECT * FROM t_backtick;
1 one

> SELECT * FROM t_period;
1 one

> SELECT * FROM t_twodots;
1 one

> SELECT * FROM control_rename;
1 one

> SELECT * FROM t_renamed;
1 one

> SELECT * FROM control_trunc;
1 one

> SELECT * FROM t_trunc;
1 one

> SELECT * FROM t_collide_dotted;
1 one

> SELECT * FROM t_collide_real;
100 hundred

> SELECT f1 FROM marker;
1

#
# 1) Incompatible schema change: DROP COLUMN of a replicated column.
#

# Control: the ALTER is detected from the binlog query event and the table
# errors with an incompatible-schema-change error, without any further writes
# to the table.
$ mysql-execute name=mysql
ALTER TABLE control_drop DROP COLUMN f2;

! SELECT * FROM control_drop;
contains:incompatible schema change

# BUG: the same ALTER against tab`1 is not detected: the handler strips the
# escaped backtick and looks up the non-existent table "tab1". No error is
# produced for the DDL itself; the failure only surfaces later, as a decode
# error once the next row for the table arrives.
$ mysql-execute name=mysql
ALTER TABLE `tab``1` DROP COLUMN f2;
INSERT INTO `tab``1` (f1) VALUES (2);

! SELECT * FROM t_backtick;
contains:unable to decode

# BUG: the same ALTER against dot.tbl is misattributed: the handler splits the
# identifier on the period and looks up table `tbl` in schema `dot`. Again the
# DDL goes undetected and the error only appears as a decode failure on the
# next row.
$ mysql-execute name=mysql
ALTER TABLE `dot.tbl` DROP COLUMN f2;
INSERT INTO `dot.tbl` (f1) VALUES (2);

! SELECT * FROM t_period;
contains:unable to decode

# BUG: a.b.c has two periods; the handler keeps only the first two components
# and looks up table `b` in schema `a`, silently discarding ".c". Undetected
# again, surfacing as a decode error on the next row.
$ mysql-execute name=mysql
ALTER TABLE `a.b.c` DROP COLUMN f2;
INSERT INTO `a.b.c` (f1) VALUES (2);

! SELECT * FROM t_twodots;
contains:unable to decode

#
# 2) Misattribution to a different, real table (the dangerous "incorrect data"
#    case): the dotted name `public`.`coll.tbl` is parsed as `coll`.`tbl`,
#    which is a *separate table we actually track*.
#

# BUG: altering `public`.`coll.tbl` is charged to the unrelated `coll`.`tbl`.
# That table is inspected (and found unchanged, so it is left healthy) while
# the table that was actually altered is never checked, and corrupts on its
# next row.
$ mysql-execute name=mysql
ALTER TABLE `coll.tbl` DROP COLUMN f2;
INSERT INTO `coll.tbl` (f1) VALUES (2);

# The table that was actually altered breaks...
! SELECT * FROM t_collide_dotted;
contains:unable to decode

# ...while the unrelated table the ALTER was misattributed to keeps serving its
# original, untouched data. The DDL was applied to the wrong identity.
> SELECT * FROM t_collide_real;
100 hundred

#
# 3) RENAME TABLE: goes entirely undetected, silently stalling the table.
#

# Control: the rename is detected and the table errors as dropped.
$ mysql-execute name=mysql
RENAME TABLE control_rename TO control_renamed;

! SELECT * FROM control_rename;
contains:table was dropped

# BUG: renaming re.named`tbl is not detected (the handler looks up
# `re`.`namedtbl`). Subsequent writes go to the new upstream name, which is
# not tracked, so the Materialize table silently stops advancing: no error,
# just stale data.
$ mysql-execute name=mysql
RENAME TABLE `re.named``tbl` TO now_plain;
INSERT INTO now_plain VALUES (2, 'two');
INSERT INTO marker VALUES (2);

# Wait until replication has made it past the rename: the marker row was
# written after it on the same binlog stream.
> SELECT f1 FROM marker;
1
2

# BUG: this should error with "table was dropped" like the control above;
# instead it still serves the pre-rename snapshot with no indication that
# anything is wrong.
> SELECT * FROM t_renamed;
1 one

#
# 4) TRUNCATE TABLE: same broken parser, so a dotted name is again undetected
#    and the table keeps serving rows that no longer exist upstream.
#

# Control: the truncate is detected and the table errors as truncated.
$ mysql-execute name=mysql
TRUNCATE TABLE control_trunc;

! SELECT * FROM control_trunc;
contains:table was truncated

# BUG: truncating `trunc.tbl` is misparsed (the period is treated as a schema
# separator) and goes undetected. The upstream row is gone, but Materialize
# keeps serving the pre-truncate snapshot row with no error -- stale,
# incorrect data.
$ mysql-execute name=mysql
TRUNCATE TABLE `trunc.tbl`;
INSERT INTO marker VALUES (3);

# Wait until replication has made it past the truncate.
> SELECT f1 FROM marker;
1
2
3

# BUG: this should error with "table was truncated" like the control above;
# instead it still serves the now-deleted row.
> SELECT * FROM t_trunc;
1 one

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

# Some tables are in an errored state, so drop the source to keep the
# post-test validation happy.
> DROP SOURCE mz_source CASCADE;
