CREATE OR REPLACE TEMPORARY VIEW tab1 USING jdbc OPTIONS (
    url "jdbc:duckdb:ducklake:postgres:postgresql://$ENV{POSTGRES_USERNAME}:$ENV{POSTGRES_PASSWORD}@$ENV{POSTGRES_HOST}:$ENV{POSTGRES_PORT}/$ENV{DUCKLAKE_CATALOG_DB};session_init_sql_file=$ENV{SESSION_INIT_SQL_FILE};",
    dbtable "tab1",

    partitionColumn "pickup_at",
    lowerBound "2008-08-08 09:13:28",
    upperBound "2033-04-27 13:08:32",
    numPartitions "7"
);

SELECT COUNT(*) FROM tab1;
SELECT SUM(total_amount) FROM tab1;
SELECT * FROM tab1 ORDER BY pickup_at LIMIT 4;
