INSERT
INTO t_datasource_parameters (db_type,
                              parameters)
SELECT *
FROM (SELECT 'MARIADB' AS db_type,
             '{
    "cachePrepStmts": "true",
        "prepStmtCacheSize": "250",
        "prepStmtCacheSqlLimit": "2048",
        "useServerPrepStmts": "true",
        "useLocalSessionState": "true",
        "useLocalTransactionState": "0",
        "rewriteBatchedStatements": "false",
        "cacheResultSetMetadata": "true",
        "cacheServerConfiguration": "true",
        "elideSetAutoCommits": "true",
        "maintainTimeStats": "false",
        "useSSL": "false",
        "allowLoadLocalInfile": "true",
        "allowMultiQueries": "true",
        "readOnlyPropagatesToServer": "false",
        "zeroDateTimeBehavior": "convertToNull",
        "connectionTimeZone": "Asia/Shanghai",
        "useUnicode": "true",
        "characterEncoding": "UTF-8",
        "tinyInt1isBit": "false",
        "noDatetimeStringSync": "true",
        "netTimeoutForStreamingResults": "3600",
        "autoReconnect": "true",
        "failOverReadOnly": "false",
        "autoClosePStmtStreams": "false",
        "useCursorFetch": "true",
        "yearIsDateType": "true",
        "connectionAttributes": "program_name:migrate-task-mariadb-connection"
}'        AS parameters) AS tmp
WHERE NOT EXISTS(
        SELECT db_type
        FROM t_datasource_parameters
        WHERE db_type = 'MARIADB'
    )
LIMIT 1;