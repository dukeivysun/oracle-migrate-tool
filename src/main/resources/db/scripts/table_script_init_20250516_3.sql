INSERT
INTO t_datasource_parameters (db_type,
                              parameters)
SELECT *
FROM (SELECT 'POSTGRESQL' AS db_type,
             '{
    "useUnicode": "true",
        "characterEncoding": "UTF-8",
        "allowEncodingChanges": "true",
        "cachePrepStmts": "true",
        "prepStmtCacheSize": "250",
        "prepStmtCacheSqlLimit": "2048",
        "useServerPrepStmts": "true",
        "useLocalSessionState": "true",
        "useLocalTransactionState": "true",
        "rewriteBatchedStatements": "true",
        "cacheResultSetMetadata": "true",
        "cacheServerConfiguration": "true",
        "elideSetAutoCommits": "true",
        "maintainTimeStats": "false",
        "ApplicationName": "migrate-task-postgresql-connection"
}'           AS parameters) AS tmp
WHERE NOT EXISTS(
        SELECT db_type
        FROM t_datasource_parameters
        WHERE db_type = 'POSTGRESQL'
    )
LIMIT 1;