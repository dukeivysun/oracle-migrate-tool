package org.dukejasun.migrate.service.capture.increment.capture.impl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleDeleteStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleInsertStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleUpdateStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.dukejasun.migrate.cache.CachedOperation;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.enums.OperationType;
import org.dukejasun.migrate.model.connection.OracleConnection;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;
import org.dukejasun.migrate.model.dto.event.DbCaptureRecordDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.OriginDataDTO;
import org.dukejasun.migrate.model.vo.LogFile;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("captureClusterDataByTransactional")
public class CaptureClusterDataByTransactional {
    private long startScn;
    private long endScn;
    private CaptureConditionDTO captureConditionDTO;
    private List<Object> originDataList;
    private final CommonConfig commonConfig;
    private final CachedOperation<String, Object> cachedOperation;
    private final Map<String, DataTypeConvert> dataTypeConvertMap;
    private final Map<String, Long> map = new HashMap<>();
    private final AtomicInteger atomicInteger = new AtomicInteger(1);

    @Autowired
    public CaptureClusterDataByTransactional(CachedOperation<String, Object> cachedOperation, CommonConfig commonConfig, Map<String, DataTypeConvert> dataTypeConvertMap) {
        this.commonConfig = commonConfig;
        this.dataTypeConvertMap = dataTypeConvertMap;
        this.cachedOperation = cachedOperation;
    }

    public String captureDataFromLog(@NotNull CaptureConditionDTO captureConditionDTO, List<Object> originDataList) {
        log.info(Constants.START_INCREMENT_MESSAGE, captureConditionDTO.getTaskId(), captureConditionDTO.getIndex(), captureConditionDTO.getStartScn().toString(), captureConditionDTO.getEndScn().toString());
        try {
            this.captureConditionDTO = captureConditionDTO;
            this.startScn = captureConditionDTO.getStartScn().longValue();
            this.endScn = captureConditionDTO.getEndScn().longValue();
            this.map.put("startScn", startScn);
            this.map.put("endScn", endScn);
            this.originDataList = originDataList;
            DatasourceDTO datasourceDTO = captureConditionDTO.getDataSourceDTO();
            captureDataFromOracleLog(datasourceDTO, Integer.parseInt(datasourceDTO.getVersion().substring(0, datasourceDTO.getVersion().indexOf("."))));
            TimeUnit.MILLISECONDS.sleep(300L);
        } catch (Exception e) {
            log.error("任务Id:【{}】第【{}】个SCN片段[{}-{})的数据采集异常!", captureConditionDTO.getTaskId(), captureConditionDTO.getIndex(), captureConditionDTO.getStartScn().toString(), captureConditionDTO.getEndScn().toString(), e);
            throw new RuntimeException(MessageFormat.format("任务Id:【{0}】第【{1}】个SCN片段[{2}-{3})的数据采集异常:{4}", captureConditionDTO.getTaskId(), captureConditionDTO.getIndex(), captureConditionDTO.getStartScn().toString(), captureConditionDTO.getEndScn().toString(), e.getMessage()));
        }
        return generatorCaptureRecord();
    }

    private void analyseDataFromResultSet(@NotNull ResultSet resultSet) throws SQLException {
        long lastScn = 0;
        String taskId = captureConditionDTO.getTaskId();
        while (resultSet.next()) {
            if (atomicInteger.intValue() > 0) {
                break;
            }
            String redoSql = getSqlRedo(resultSet);
            if (StringUtils.isNotBlank(redoSql)) {
                BigDecimal bigDecimal = resultSet.getBigDecimal(1);
                BigDecimal operationCode = resultSet.getBigDecimal(3);
                Timestamp timestamp = resultSet.getTimestamp(4);
                String xId = resultSet.getString(5);
                String tableName = resultSet.getString(7);
                String schemaName = resultSet.getString(8);
                String rowId = resultSet.getString(11);
                int rollback = resultSet.getInt(12);
                String undoSql = resultSet.getString(14);
                lastScn = bigDecimal.longValue();
                int code = operationCode.intValue();
                String schemaAndTableName = MessageFormat.format(Constants.SCHEMA_TABLE_KEY, schemaName, tableName);
                CaptureTableDTO captureTableDTO = CaptureTableCache.INSTANCE.get(schemaAndTableName);
                if (log.isDebugEnabled() && !"commit".equalsIgnoreCase(redoSql) && !"rollback".equalsIgnoreCase(redoSql)) {
                    log.info("rollback:{},scn:{},rowId:{},redoSql:{},undoSql:{}", rollback, lastScn, rowId, redoSql, undoSql);
                }
                if (Objects.isNull(cachedOperation.get(xId))) {
                    cachedOperation.set(xId, Lists.newLinkedList());
                }
                OriginDataDTO originDataDTO = null;
                if (rollback == 0) {
                    switch (code) {
                        case 1:
                            List<SQLStatement> sqlStatementList = getSQLStatementList(redoSql);
                            SQLObject sqlObject = sqlStatementList.get(0);
                            try {
                                originDataDTO = analyseInsertDml(taskId, sqlObject, captureTableDTO, lastScn, timestamp);
                                originDataDTO.setRowId(rowId);
                                originDataDTO.setXId(xId);
                            } catch (Exception e) {
                                log.error("解析DML:{}失败,当前SCN:{}", redoSql, lastScn);
                                originDataDTO = null;
                            }
                            break;
                        case 2:
                            sqlStatementList = getSQLStatementList(redoSql);
                            sqlObject = sqlStatementList.get(0);
                            try {
                                originDataDTO = analyseDeleteDml(taskId, sqlObject, captureTableDTO, lastScn, timestamp);
                                originDataDTO.setRowId(rowId);
                                originDataDTO.setXId(xId);
                            } catch (Exception e) {
                                log.error("解析DML:{}失败,当前SCN:{}", redoSql, lastScn);
                                originDataDTO = null;
                            }
                            break;
                        case 3:
                            sqlStatementList = getSQLStatementList(redoSql);
                            sqlObject = sqlStatementList.get(0);
                            try {
                                originDataDTO = analyseUpdateDml(taskId, sqlObject, captureTableDTO, lastScn, timestamp);
                                originDataDTO.setRowId(rowId);
                                originDataDTO.setXId(xId);
                            } catch (Exception e) {
                                log.error("解析DML:{}失败,当前SCN:{}", redoSql, lastScn);
                                originDataDTO = null;
                            }
                            break;
                        case 5:
                            if (StringUtils.containsIgnoreCase(redoSql, OperationType.TRUNCATE.getCode())) {
                                originDataDTO = generatorOriginDataDTO(taskId, captureTableDTO, lastScn, timestamp, OperationType.TRUNCATE);
                                originDataDTO.setRowId(rowId);
                                originDataDTO.setXId(xId);
                                originDataDTO.setDdl(redoSql);
                            }
                            break;
                        case 7:
                            Object valueObject = cachedOperation.get(xId);
                            if (Objects.nonNull(valueObject)) {
                                synchronized (this) {
                                    if (valueObject instanceof List) {
                                        originDataList.add(valueObject);
                                    } else {
                                        originDataList.add(valueObject);
                                    }
                                    cachedOperation.delete(xId);
                                }
                            }
                            break;
                        case 34:
                            log.info("MISSING_SCN:{},当前SCN:{}", redoSql, lastScn);
                            break;
                        case 36:
                            if (Objects.nonNull(cachedOperation.get(xId))) {
                                cachedOperation.delete(xId);
                            }
                            break;
                        default:
                            break;
                    }
                }
                if (Objects.nonNull(originDataDTO)) {
                    if (Objects.nonNull(cachedOperation.get(xId))) {
                        cachedOperation.put(xId, originDataDTO);
                    }
                }
            }
        }
        this.map.put("startScn", lastScn == 0L ? endScn : lastScn);
    }

    @Contract("_ -> new")
    private @NotNull List<SQLStatement> getSQLStatementList(String sql) {
        return SQLUtils.parseStatements(sql, JdbcConstants.ORACLE.name());
    }

    private @NotNull OriginDataDTO generatorOriginDataDTO(String taskId, @NotNull CaptureTableDTO captureTableDTO, long lastScn, @NotNull Timestamp timestamp, OperationType operationType) {
        OriginDataDTO originDataDTO = new OriginDataDTO();
        originDataDTO.setSchema(captureTableDTO.getTargetSchema());
        originDataDTO.setTable(captureTableDTO.getTargetName());
        originDataDTO.setType(operationType);
        originDataDTO.setTypeList(captureTableDTO.getDataTypeList());
        originDataDTO.setTaskId(taskId);
        originDataDTO.setTimestamp(timestamp.getTime());
        originDataDTO.setCaptureTimestamp(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());
        originDataDTO.setScn(BigInteger.valueOf(lastScn));
        originDataDTO.setPkColumnName(captureTableDTO.getPrimaryKeyList());
        return originDataDTO;
    }

    private @NotNull OriginDataDTO analyseUpdateDml(String taskId, SQLObject sqlObject, CaptureTableDTO captureTableDTO, long lastScn, Timestamp timestamp) {
        OriginDataDTO originDataDTO = generatorOriginDataDTO(taskId, captureTableDTO, lastScn, timestamp, OperationType.UPDATE);
        originDataDTO.setPkColumnName(captureTableDTO.getPrimaryKeyList());
        if (sqlObject instanceof OracleUpdateStatement) {
            OracleUpdateStatement oracleUpdateStatement = (OracleUpdateStatement) sqlObject;
            SQLExpr sqlExpr = oracleUpdateStatement.getWhere();
            LinkedHashMap<String, Object> linkedHashMap = Maps.newLinkedHashMap();
            if (sqlExpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
                generatorWhereCondition(sqlBinaryOpExpr, linkedHashMap);
            }
            LinkedHashMap<String, Object> linkedAfterMap = Maps.newLinkedHashMap();
            List<SQLUpdateSetItem> itemList = oracleUpdateStatement.getItems();
            for (SQLUpdateSetItem sqlUpdateSetItem : itemList) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlUpdateSetItem.getColumn();
                String simpleName = sqlUpdateSetItem.getValue().getClass().getSimpleName();
                dataTypeConvertMap.get(simpleName + "_CONVERT").generatorColumnNameAndValue(sqlIdentifierExpr, (SQLExprImpl) sqlUpdateSetItem.getValue(), linkedAfterMap);
            }
            if (!CollectionUtils.isEmpty(linkedHashMap)) {
                LinkedHashMap<String, Object> linkResult = Maps.newLinkedHashMap();
                ListIterator<Map.Entry<String, Object>> iterator = new ArrayList<>(linkedHashMap.entrySet()).listIterator(linkedHashMap.size());
                while (iterator.hasPrevious()) {
                    Map.Entry<String, Object> previous = iterator.previous();
                    String key = previous.getKey();
                    Object value = previous.getValue();
                    linkResult.put(key, value);
                }
                linkedHashMap.clear();
                linkedHashMap.putAll(linkResult);
                originDataDTO.setBefore(linkedHashMap);
                linkResult.putAll(linkedAfterMap);
                originDataDTO.setAfter(linkResult);
            }
        }
        return originDataDTO;
    }

    private @NotNull OriginDataDTO analyseDeleteDml(String taskId, SQLObject sqlObject, CaptureTableDTO captureTableDTO, long lastScn, Timestamp timestamp) {
        OriginDataDTO originDataDTO = generatorOriginDataDTO(taskId, captureTableDTO, lastScn, timestamp, OperationType.DELETE);
        originDataDTO.setPkColumnName(captureTableDTO.getPrimaryKeyList());
        if (sqlObject instanceof OracleDeleteStatement) {
            OracleDeleteStatement oracleDeleteStatement = (OracleDeleteStatement) sqlObject;
            SQLExpr sqlExpr = oracleDeleteStatement.getWhere();
            LinkedHashMap<String, Object> linkedHashMap = Maps.newLinkedHashMap();
            if (sqlExpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
                generatorWhereCondition(sqlBinaryOpExpr, linkedHashMap);
            }
            originDataDTO.setAfter(Maps.newLinkedHashMap());
            if (!CollectionUtils.isEmpty(linkedHashMap)) {
                LinkedHashMap<String, Object> linkResult = Maps.newLinkedHashMap();
                ListIterator<Map.Entry<String, Object>> iterator = new ArrayList<>(linkedHashMap.entrySet()).listIterator(linkedHashMap.size());
                while (iterator.hasPrevious()) {
                    Map.Entry<String, Object> previous = iterator.previous();
                    String key = previous.getKey();
                    Object value = previous.getValue();
                    linkResult.put(key, value);
                }
                linkedHashMap.clear();
                originDataDTO.setBefore(linkResult);
            }
        }
        return originDataDTO;
    }

    private @NotNull OriginDataDTO analyseInsertDml(String taskId, SQLObject sqlObject, CaptureTableDTO captureTableDTO, long lastScn, Timestamp timestamp) {
        OriginDataDTO originDataDTO = generatorOriginDataDTO(taskId, captureTableDTO, lastScn, timestamp, OperationType.INSERT);
        originDataDTO.setPkColumnName(captureTableDTO.getPrimaryKeyList());
        if (sqlObject instanceof OracleInsertStatement) {
            OracleInsertStatement oracleInsertStatement = (OracleInsertStatement) sqlObject;
            List<SQLExpr> columnList = oracleInsertStatement.getColumns();
            List<SQLExpr> valuesClauseList = oracleInsertStatement.getValuesList().get(0).getValues();
            LinkedHashMap<String, Object> linkedHashMap = Maps.newLinkedHashMapWithExpectedSize(columnList.size());
            for (int i = 0, n = columnList.size(); i < n; i++) {
                SQLExpr sqlExpr = columnList.get(i);
                if (sqlExpr instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
                    SQLExpr sqlValueExpr = valuesClauseList.get(i);
                    if (Objects.nonNull(sqlValueExpr)) {
                        String simpleName = ((SQLExprImpl) sqlValueExpr).getClass().getSimpleName();
                        dataTypeConvertMap.get(simpleName + "_CONVERT").generatorColumnNameAndValue(sqlIdentifierExpr, (SQLExprImpl) sqlValueExpr, linkedHashMap);
                    }
                }
            }
            originDataDTO.setAfter(linkedHashMap);
            originDataDTO.setBefore(Maps.newLinkedHashMapWithExpectedSize(columnList.size()));
        }
        return originDataDTO;
    }

    private @Nullable String getSqlRedo(@NotNull ResultSet resultSet) throws SQLException {
        int lobLimitCounter = 9;
        String redoSql = resultSet.getString(2);
        if (StringUtils.isBlank(redoSql)) {
            return null;
        }
        StringBuilder result = new StringBuilder(redoSql);
        int csf = resultSet.getInt(6);
        while (csf == 1) {
            resultSet.next();
            if (lobLimitCounter-- == 0) {
                log.warn("由于连接器限制为 {} MB,LOB值被截断.", 40);
                break;
            }
            redoSql = resultSet.getString(2);
            result.append(redoSql);
            csf = resultSet.getInt(6);
        }
        return result.toString();
    }

    private String generatorCaptureRecord() {
        DbCaptureRecordDTO dbCaptureRecordDTO = new DbCaptureRecordDTO();
        dbCaptureRecordDTO.setTaskId(captureConditionDTO.getTaskId());
        dbCaptureRecordDTO.setCaptureIndex(captureConditionDTO.getIndex());
        dbCaptureRecordDTO.setCaptureStartScn(captureConditionDTO.getStartScn().toString());
        dbCaptureRecordDTO.setCaptureEndScn(captureConditionDTO.getEndScn().toString());
        return dbCaptureRecordDTO.toString();
    }

    private void captureDataFromOracleLog(@NotNull DatasourceDTO datasourceDTO, int version) throws SQLException {
        String url = MessageFormat.format(DatabaseExpandType.ORACLE.getUrlTemplate(), datasourceDTO.getHost(), String.valueOf(datasourceDTO.getPort()), datasourceDTO.getDatabaseName());
        try (OracleConnection oracleConnection = new OracleConnection(url, datasourceDTO.getUsername(), datasourceDTO.getPassword())) {
            modifySessionDateFormat(oracleConnection);
            while (atomicInteger.intValue() > 0) {
                startScn = map.get("startScn");
                endScn = map.get("endScn");
                if (atomicInteger.intValue() > 0) {
                    atomicInteger.set(0);
                }
                if (version < 12) {
                    setLogFilesForMining(oracleConnection);
                    startMiningSession(oracleConnection, startScn, endScn);
                } else {
                    startMiningSession(oracleConnection, startScn, endScn);
                }
                queryLogMinerContents(oracleConnection, startScn, endScn);
                endMiningSession(oracleConnection);
            }
        }
    }

    private @NotNull String generatorLogMinerQuery() {
        StringBuilder builder = new StringBuilder();
        builder.append(IConstants.LOG_CONSTANTS_QUERY_CONDITION1);
        final String pdbName = captureConditionDTO.getPdbName();
        if (StringUtils.isNotBlank(pdbName)) {
            builder.append("AND SRC_CON_NAME = '").append(pdbName.toUpperCase()).append('\'').append(' ');
        }
        builder.append(MessageFormat.format(IConstants.LOG_CONSTANTS_QUERY_CONDITION, captureConditionDTO.getSchemas(), captureConditionDTO.getTables()));
        return builder.toString();
    }

    private void modifySessionDateFormat(@NotNull OracleConnection oracleConnection) throws SQLException {
        try (PreparedStatement preparedStatement = oracleConnection.connection().prepareStatement(IConstants.SQL_ALTER_NLS_SESSION_PARAMETERS)) {
            preparedStatement.execute();
        }
    }

    private void endMiningSession(@NotNull OracleConnection oracleConnection) {
        try {
            oracleConnection.executeWithoutCommitting(IConstants.END_LOGMINER);
        } catch (SQLException e) {
            if (StringUtils.containsIgnoreCase(e.getMessage(), "ORA-01307")) {
                log.info("LogMiner挖掘会话已关闭.");
                return;
            }
            log.error("关闭LogMiner挖掘会话失败!", e);
        }
    }

    private void queryLogMinerContents(OracleConnection oracleConnection, long startScn, Long endScn) throws SQLException {
        try (PreparedStatement statement = oracleConnection.connection().prepareStatement(generatorLogMinerQuery(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            statement.setFetchSize(commonConfig.getFetchSize());
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, String.valueOf(startScn));
            statement.setString(2, String.valueOf(endScn));
            try (ResultSet resultSet = statement.executeQuery()) {
                analyseDataFromResultSet(resultSet);
            }
        }
    }

    private void generatorWhereCondition(@NotNull SQLBinaryOpExpr sqlBinaryOpExpr, LinkedHashMap<String, Object> linkedHashMap) {
        if (sqlBinaryOpExpr.getRight() instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlRightOpExpr = (SQLBinaryOpExpr) sqlBinaryOpExpr.getRight();
            String simpleName = ((SQLExprImpl) sqlRightOpExpr.getRight()).getClass().getSimpleName();
            dataTypeConvertMap.get(simpleName + "_CONVERT").generatorColumnNameAndValue((SQLIdentifierExpr) sqlRightOpExpr.getLeft(), (SQLExprImpl) sqlRightOpExpr.getRight(), linkedHashMap);
            generatorWhereCondition((SQLBinaryOpExpr) sqlBinaryOpExpr.getLeft(), linkedHashMap);
        } else {
            String simpleName = ((SQLExprImpl) sqlBinaryOpExpr.getRight()).getClass().getSimpleName();
            dataTypeConvertMap.get(simpleName + "_CONVERT").generatorColumnNameAndValue((SQLIdentifierExpr) sqlBinaryOpExpr.getLeft(), (SQLExprImpl) sqlBinaryOpExpr.getRight(), linkedHashMap);
        }
    }

    private void startMiningSession(@NotNull OracleConnection oracleConnection, long startScn, long endScn) throws SQLException {
        try (CallableStatement callableStatement = oracleConnection.connection().prepareCall(IConstants.SQL_START_LOGMINER)) {
            callableStatement.setString(1, String.valueOf(startScn));
            callableStatement.setString(2, String.valueOf(endScn));
            callableStatement.execute();
        }
    }

    private void setLogFilesForMining(@NotNull OracleConnection oracleConnection) throws SQLException {
        removeLogFilesFromMining(oracleConnection);
        List<String> logFilesNames = captureConditionDTO.getLogFiles().stream().map(LogFile::getFileName).collect(Collectors.toList());
        StringBuilder buffer = new StringBuilder();
        buffer.append("BEGIN").append('\n');
        logFilesNames.forEach(logFileName -> buffer.append(MessageFormat.format(Constants.OracleConstants.ADD_LOG_FILE, logFileName)).append('\n'));
        buffer.append("END;");
        oracleConnection.executeWithoutCommitting(buffer.toString());
        buffer.setLength(0);
    }

    private void removeLogFilesFromMining(OracleConnection oracleConnection) throws SQLException {
        try (PreparedStatement preparedStatement = oracleConnection.connection().prepareStatement("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS"); ResultSet result = preparedStatement.executeQuery()) {
            LinkedList<String> logFilesNames = new LinkedList<>();
            while (result.next()) {
                logFilesNames.add(result.getString(1));
            }
            if (logFilesNames.size() > 0) {
                StringBuilder buffer = new StringBuilder();
                buffer.append("BEGIN").append('\n');
                for (int i = 0, n = logFilesNames.size(); i < n; i++) {
                    String logFileName = logFilesNames.get(i);
                    buffer.append(" DBMS_LOGMNR.REMOVE_LOGFILE(logfilename=>'").append(logFileName);
                    if (i != n - 1) {
                        buffer.append("',");
                    } else {
                        buffer.append('\n');
                    }
                }
                buffer.append("END;");
                oracleConnection.executeWithoutCommitting(buffer.toString());
                buffer.setLength(0);
            }
        }
    }
}
