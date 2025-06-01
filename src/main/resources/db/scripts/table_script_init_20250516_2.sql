INSERT
INTO t_datasource_parameters (db_type,
                              parameters)
SELECT *
FROM (SELECT 'ORACLE' AS db_type,
             '{
     "useUnicode": "true",
         "characterEncoding": "UTF-8",
         "remarksReporting": "true",
         "v$session.program": "migrate-task-oracle-connection"
 }'      AS parameters) AS tmp
WHERE NOT EXISTS(
        SELECT db_type
        FROM t_datasource_parameters
        WHERE db_type = 'ORACLE'
    )
LIMIT 1;