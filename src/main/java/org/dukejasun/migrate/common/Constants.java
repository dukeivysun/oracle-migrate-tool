package org.dukejasun.migrate.common;

import java.math.BigInteger;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

/**
 * @author dukedpsun
 */
public interface Constants {
    char HALF_ROW_LINE = '-';
    char FULL_ROW_LINE = '－';
    char COLUMN_LINE = '|';
    char CORNER = '+';
    char HALF_SPACE = '\u0020';
    char FULL_SPACE = '\u3000';
    char LF = '\n';
    char SPACE = ' ';
    String EMPTY = "";
    String SCHEMA_TABLE_KEY = "{0}.{1}";
    String SCHEMA_TABLE_KEY2 = "{0}.{1}@{2}";
    String TRANSMISSION_THREAD_NAME = "transmission-task-pool-%d";
    String TOTAL_MIGRATE_THREAD_NAME = "transmission-total-migrate-pool-%d";
    String STRUCTURE_MIGRATE_THREAD_NAME = "transmission-structure-migrate-pool-%d";
    String READ_DATA_FILE_THREAD_NAME = "transmission-read-data-file-pool-%d";
    String CAPTURE_DATA_RECORD_THREAD_NAME = "transmission-migrate-statistics-record-pool-%d";
    String INCREMENT_CAPTURE_THREAD_NAME = "transmission-increment-capture-data-pool-%d";
    String FETCH_TABLE_METADATA_THREAD_NAME = "transmission-fetch-table-metadata-pool-%d";
    String SINK_THREAD_NAME = "transmission-write-target-db-pool-%d";
    String TOTAL_SUB_THREAD_NAME = "{0}.{1}@transmission-sub-total-migrate-pool-%d";
    String TOTAL_DATA_COMPARE_THREAD_NAME = "transmission-total-data-compare-pool-%d";
    String COMPARE_THREAD_NAME = "transmission-compare-parent-pool-%d";
    String COMPARE_TABLE_SUB_THREAD_NAME = "{0}.{1}@transmission-compare-parent-pool-%d";
    String DDL_CREATE_TABLE_KEY = "CREATE TABLE";
    String DDL_ALTER_TABLE_KEY = "ALTER TABLE";
    String DDL_CREATE_INDEX_KEY = "CREATE INDEX";
    String TEMP_SUFFIX = ".tmp";
    String DATA_SUFFIX = ".txt";
    String CSV_SUFFIX = ".csv";
    String ERROR_SUFFIX = ".err";
    String CONDITION_TYPE = "scn";
    String RAC_CONDITION_TYPE = "rac_scn";
    String DATA_LOG_TYPE = "file";
    String CALLBACK_TASK_TYPE = "callback_task";
    String CALLBACK_CAPTURE_RECORD_TYPE = "callback_capture_record";
    String INCREMENT_REPLICATOR_METRICS = "increment_metrics";
    String CALLBACK_RECORD_TYPE = "callback_sink_record";
    String WRITE_DATA_TYPE = "write";
    String COMMA = ",";
    String DOUBLE_QUOTATION = "";
    String NULL_VALUE = "null";
    String TOTAL_ERROR_MESSAGE = "({0},{1}]范围全量迁移失败!{2}";
    String TOTAL_ERROR_MESSAGE_1 = "分区:{0}全量迁移失败!{1}";
    String TOTAL_ERROR_MESSAGE_2 = "闪回:{0}全量迁移失败!{1}";
    String TOTAL_ERROR_MESSAGE_3 = "全量迁移失败!{0}";
    String INCREMENT_RESULT_STATISTICS = "任务Id:【{}】增量数据同步\t数据采集片段索引:{}\t数据下沉索引:{}\t共运行:{}秒.";
    String TOTAL_RESULT_STATISTICS = "{}表全量迁移\t源库数据数量:{}\t迁移后目标数据数量:{}\t异常:{}";
    String STRUCTURE_RESULT_STATISTICS = "{}\t对象数量:{}\t成功数量:{}\t失败数量:{}";
    String TRANSMISSION_RESULT_STATISTICS = "汇总:\t迁移对象数量:{}\t成功数量:{}\t失败数量:{}\t迁移用时:{}秒.";
    String TRUNCATE_TABLE = "TRUNCATE TABLE {0}.{1}";
    String INSERT_DML = "INSERT INTO {0} ({1}) VALUES({2})";
    String INSERT_SQL = "INSERT INTO {0}.{1} ({2}) VALUES ";
    String START_INCREMENT_MESSAGE = "任务Id:【{}】开始第【{}】个SCN片段[{}-{})的数据采集.";
    String FINISH_INCREMENT_MESSAGE = "任务Id:【{}】完成第【{}】个SCN片段[{}-{})的数据采集.";
    String CREATE_SCHEMA_OR_DATABASE_SQL = "CREATE SCHEMA IF NOT exists {0}";
    BigInteger MAX_VALUE = new BigInteger("9223372036854775807");
    DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern("dd-MMM-yy hh.mm.ss").optionalStart().appendPattern(".").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false).optionalEnd().appendPattern(" a").toFormatter(Locale.ENGLISH);
    String[] DATABASE_INTEGER_TYPES = {"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"};
    String[] DECIMAL_TYPE_LIST = {"NUMBER", "NUMERIC", "DECIMAL"};

    /**
     * Oracle常量
     */
    interface OracleConstants {
        String EXISTS_TABLE = "SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME ='LOG_MINING_FLUSH'";
        String CREATE_FLUSH_TABLE = "CREATE TABLE LOG_MINING_FLUSH (LAST_SCN NUMBER(19,0))";
        String INSERT_FLUSH_TABLE = "INSERT INTO LOG_MINING_FLUSH VALUES (0)";
        String UPDATE_FLUSH_TABLE = "UPDATE LOG_MINING_FLUSH SET LAST_SCN = ";
        String DELETE_FLUSH_TABLE = "DELETE FROM LOG_MINING_FLUSH";
        String QUERY_TABLE_COUNT = "SELECT COUNT(1) FROM LOG_MINING_FLUSH";
        String QUERY_CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";
        String QUERY_CURRENT_SCN_WITH_RAC = "SELECT d.CURRENT_SCN, t.XID, t.START_SCN FROM V$DATABASE d LEFT OUTER JOIN GV$TRANSACTION t ON t.START_SCN < d.CURRENT_SCN ";
        String ALL_TABLE_METADATA = "SELECT ATS.OWNER, ATS.TABLE_NAME, ATC.COMMENTS, ATS.TABLESPACE_NAME FROM ALL_TABLES ATS LEFT JOIN ALL_TAB_COMMENTS ATC ON ATS.OWNER = ATC.OWNER AND ATS.TABLE_NAME = ATC.TABLE_NAME WHERE ATS.OWNER IN({0})";
        String ALL_COLUMN_METADATA_ORACLE_2 = "SELECT t.COLUMN_ID                                      AS columnNo,\n" + "       t.owner                                          AS schemaName,\n" + "       t.table_name                                     AS tableName,\n" + "       t.column_name                                    AS columnName,\n" + "       t.data_type                                      AS typeName,\n" + "       NVL(data_precision, t.data_length)               AS LENGTH,\n" + "       t.data_scale                                     AS SCALE,\n" + "       decode((SELECT count(1)\n" + "               FROM dba_constraints a,\n" + "                    dba_cons_columns b\n" + "               WHERE a.constraint_name = b.constraint_name\n" + "                 AND a.owner = t.owner\n" + "                 AND a.table_name = t.table_name\n" + "                 AND b.column_name = t.column_name\n" + "                 AND a.constraint_type = 'P'), 0, 0, 1) AS primary_key,\n" + "       decode((SELECT count(1)\n" + "               FROM dba_constraints a,\n" + "                    dba_cons_columns b\n" + "               WHERE a.constraint_name = b.constraint_name\n" + "                 AND a.owner = t.owner\n" + "                 AND a.table_name = t.table_name\n" + "                 AND b.column_name = t.column_name\n" + "                 AND a.constraint_type = 'U'), 0, 0, 1) AS unique_key,\n" + "       decode(t.nullable, 'N', 0, 1)                    AS nullable,\n" + "       data_default                                     AS defaultValue,\n" + "       t.VIRTUAL_COLUMN,\n" + "       (\n" + "           SELECT max(comments)\n" + "           FROM dba_col_comments z\n" + "           WHERE z.owner = t.owner\n" + "             AND z.table_name = t.table_name\n" + "             AND z.column_name = t.column_name)         AS comments\n" + "FROM DBA_TAB_COLS t\n" + "WHERE 1 = 1\n" + " AND t.HIDDEN_COLUMN='NO'\n" + "  AND t.owner NOT IN\n" + "      ('ANONYMOUS', 'APEX_030200', 'APEX_PUBLIC_USER', 'APPQOSSYS', 'AUD_SYS', 'CTXSYS', 'DBSNMP', 'EXFSYS',\n" + "       'FLOWS_FILES', 'HR', 'IX', 'MDSYS', 'MGMT_VIEW', 'OE', 'OLAPSYS', 'ORACLE_OCM', 'ORDDATA', 'ORDPLUGINS',\n" + "       'ORDSYS', 'OWBSYS_AUDIT', 'OUTLN', 'OWBSYS', 'PM', 'SH', 'SI_INFORMTN_SCHEMA', 'SPATIAL_CSW_ADMIN_USR',\n" + "       'SPATIAL_WFS_ADMIN_USR', 'SYS', 'SYSMAN', 'SYSTEM', 'WMSYS', 'XDB', 'XS$NULL', 'UNKNOWN ')\n" + "  AND t.owner = ?\n" + "  AND table_name = ?\n" + "ORDER BY t.owner,\n" + "         t.table_name,\n" + "         t.column_id";
        String ALL_COLUMN_METADATA_ORACLE = "SELECT t.COLUMN_ID                                      AS columnNo,\n" + "       t.owner                                          AS schemaName,\n" + "       t.table_name                                     AS tableName,\n" + "       t.column_name                                    AS columnName,\n" + "       t.data_type                                      AS typeName,\n" + "       NVL(data_precision, t.data_length)               AS LENGTH,\n" + "       t.data_scale                                     AS SCALE,\n" + "       decode((SELECT count(1)\n" + "               FROM dba_constraints a,\n" + "                    dba_cons_columns b\n" + "               WHERE a.constraint_name = b.constraint_name\n" + "                 AND a.owner = t.owner\n" + "                 AND a.table_name = t.table_name\n" + "                 AND b.column_name = t.column_name\n" + "                 AND a.constraint_type = 'P'), 0, 0, 1) AS primary_key,\n" + "       decode((SELECT count(1)\n" + "               FROM dba_constraints a,\n" + "                    dba_cons_columns b\n" + "               WHERE a.constraint_name = b.constraint_name\n" + "                 AND a.owner = t.owner\n" + "                 AND a.table_name = t.table_name\n" + "                 AND b.column_name = t.column_name\n" + "                 AND a.constraint_type = 'U'), 0, 0, 1) AS unique_key,\n" + "       decode(t.nullable, 'N', 0, 1)                    AS nullable,\n" + "       data_default                                     AS defaultValue,\n" + "       t.VIRTUAL_COLUMN,\n" + "       (\n" + "           SELECT max(comments)\n" + "           FROM dba_col_comments z\n" + "           WHERE z.owner = t.owner\n" + "             AND z.table_name = t.table_name\n" + "             AND z.column_name = t.column_name)         AS comments\n" + "FROM DBA_TAB_COLS t\n" + "WHERE 1 = 1\n" + "  AND T.USER_GENERATED = 'YES'\n" + "  AND t.owner NOT IN\n" + "      ('ANONYMOUS', 'APEX_030200', 'APEX_PUBLIC_USER', 'APPQOSSYS', 'AUD_SYS', 'CTXSYS', 'DBSNMP', 'EXFSYS',\n" + "       'FLOWS_FILES', 'HR', 'IX', 'MDSYS', 'MGMT_VIEW', 'OE', 'OLAPSYS', 'ORACLE_OCM', 'ORDDATA', 'ORDPLUGINS',\n" + "       'ORDSYS', 'OWBSYS_AUDIT', 'OUTLN', 'OWBSYS', 'PM', 'SH', 'SI_INFORMTN_SCHEMA', 'SPATIAL_CSW_ADMIN_USR',\n" + "       'SPATIAL_WFS_ADMIN_USR', 'SYS', 'SYSMAN', 'SYSTEM', 'WMSYS', 'XDB', 'XS$NULL', 'UNKNOWN ')\n" + "  AND t.owner = ?\n" + "  AND table_name = ?\n" + "ORDER BY t.owner,\n" + "         t.table_name,\n" + "         t.column_id";
        String QUERY_INDEX_NAMES = "SELECT DISTINCT INDEX_NAME FROM SYS.ALL_INDEXES WHERE UNIQUENESS ='NONUNIQUE' AND TABLE_NAME =? AND TABLE_OWNER =? AND OWNER =?";
        String QUERY_PARTITION_NAMES = "SELECT USER_TAB_PARTITIONS.PARTITION_NAME FROM USER_TAB_PARTITIONS ,dba_part_key_columns WHERE dba_part_key_columns.name=USER_TAB_PARTITIONS.TABLE_NAME AND USER_TAB_PARTITIONS.TABLE_NAME=? AND dba_part_key_columns.owner =? ORDER BY USER_TAB_PARTITIONS.PARTITION_POSITION ASC";
        String ADD_LOG_FILE = "DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME=>''{0}'',OPTIONS => DBMS_LOGMNR.ADDFILE);";
        String SQL_START_LOGMINER = "BEGIN DBMS_LOGMNR.START_LOGMNR(startScn => ?, endScn => ?,  DICTFILENAME => ?, OPTIONS =>  DBMS_LOGMNR.SKIP_CORRUPTION + DBMS_LOGMNR.NO_SQL_DELIMITER + DBMS_LOGMNR.NO_ROWID_IN_STMT + DBMS_LOGMNR.DDL_DICT_TRACKING + DBMS_LOGMNR.CONTINUOUS_MINE + DBMS_LOGMNR.STRING_LITERALS_IN_STMT); END;";
    }

    /**
     * MySQL常量
     */
    interface MySQLConstants {
        String LOAD_DATA_INFILE = "LOAD DATA LOCAL INFILE ''{0}'' {1} INTO TABLE {2}.{3} FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY ''\"'' LINES TERMINATED BY ''\\n'' ({4})";
        String LOAD_DATA_MEMORY = "LOAD DATA LOCAL INFILE '''' {0} INTO TABLE {1}.{2} FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY ''\"'' LINES TERMINATED BY ''\\n'' ({3})";
    }
}
