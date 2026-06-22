# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A numeric column declared with a max scale (here numeric(38, 1), i.e. at most
# one fractional digit) must enforce that scale on values ingested via COPY
# INTO before they reach an Iceberg sink. DuckDB authors a Parquet file holding
# a decimal value with two fractional digits (1.29). On COPY INTO the value is
# cast to numeric(38, 1) and rounded to 1.3, and that rounded value is what the
# Iceberg sink writes into its decimal(38, 1) column.

# demonstrates behavior on insertion into a table

> CREATE TABLE test (foo NUMERIC(38, 3));
> INSERT INTO test VALUES (1.0004);
> INSERT INTO test VALUES (1.0033);
> SELECT * FROM test;
1
1.003

> CREATE SECRET access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION aws_conn TO AWS (
    ACCESS KEY ID = 'tduser',
    SECRET ACCESS KEY = SECRET access_key_secret,
    ENDPOINT = 'http://${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

> CREATE CONNECTION polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

# Author a Parquet file with a decimal value carrying two fractional digits, so
# the source bytes really have a scale of 2 (not something MZ produced).
$ duckdb-execute name=iceberg
CREATE SECRET s3_secret (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
COPY (SELECT 1.0033::DECIMAL(38,4) AS foo) TO 's3://test-bucket/copy_numeric_input/data.parquet' (FORMAT PARQUET);

> COPY INTO test FROM 's3://test-bucket/copy_numeric_input' (FORMAT PARQUET, AWS CONNECTION = aws_conn);

> SELECT * FROM test;
1
1.003
1.0033


> CREATE SINK copy_test
    FROM test
    INTO ICEBERG CATALOG CONNECTION polaris (
        NAMESPACE 'default_namespace',
        TABLE 'copy_numeric_table'
    )
    KEY (foo) NOT ENFORCED
    MODE UPSERT
    WITH (COMMIT INTERVAL '1s');

# Iceberg sinks commit data asynchronously; wait for at least one commit interval.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=iceberg
SET unsafe_enable_version_guessing = true;

# The rounded value lands in the Iceberg decimal(38, 1) column.
$ duckdb-query name=iceberg
SELECT id, n FROM iceberg_scan('s3://test-bucket/default_namespace/test') ORDER BY id
1 1.3

# Verify via the Polaris REST catalog as well.
$ duckdb-execute name=iceberg
CREATE SECRET polaris_secret (TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', ENDPOINT 'http://polaris:8181/api/catalog');
ATTACH 'default_catalog' AS polaris (TYPE iceberg, SECRET polaris_secret, ACCESS_DELEGATION_MODE 'none');

$ duckdb-query name=iceberg
SELECT id, n FROM polaris.default_namespace.copy_numeric_table ORDER BY id
1 1.3
