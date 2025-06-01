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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.enums.OperationType;
import org.dukejasun.migrate.handler.FileConvertHandler;
import org.dukejasun.migrate.model.connection.OracleConnection;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;
import org.dukejasun.migrate.model.dto.event.DbCaptureRecordDTO;
import org.dukejasun.migrate.model.dto.event.DbIncrementMetricsDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.OriginDataDTO;
import org.dukejasun.migrate.model.vo.LogFile;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import org.dukejasun.migrate.utils.EncryptUtil;
import org.dukejasun.migrate.utils.FileUtil;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
public abstract class AbstractCaptureDataCallable implements Callable<String> {
    private long startScn;
    @Getter
    private long endScn;
    @Getter
    private Logger logger;
    @Getter
    private final CaptureConditionDTO captureConditionDTO;
    private final CommonConfig commonConfig;
    private final DataProducer dataProducer;
    private final Map<String, DataTypeConvert> dataTypeConvertMap;
    @Getter
    private final Map<String, Long> map = new HashMap<>();
    @Getter
    private final AtomicInteger atomicInteger = new AtomicInteger(1);
    private final AtomicReference<OriginDataDTO> atomicReference = new AtomicReference<>();

    public AbstractCaptureDataCallable(CaptureConditionDTO captureConditionDTO, CommonConfig commonConfig, Map<String, DataTypeConvert> dataTypeConvertMap, DataProducer dataProducer) {
        this.captureConditionDTO = captureConditionDTO;
        this.commonConfig = commonConfig;
        this.dataTypeConvertMap = dataTypeConvertMap;
        this.dataProducer = dataProducer;
    }

    @Override
    public String call() {
        log.info(Constants.START_INCREMENT_MESSAGE, captureConditionDTO.getTaskId(), captureConditionDTO.getIndex(), captureConditionDTO.getStartScn().toString(), captureConditionDTO.getEndScn().toString());
        String parentDir = captureConditionDTO.getParentDir();
        String fileName = commonConfig.getIncrementDataFilePrefix() + captureConditionDTO.getIndex();
        try {
            DatasourceDTO datasourceDTO = captureConditionDTO.getDataSourceDTO();
            startScn = captureConditionDTO.getStartScn().longValue();
            endScn = captureConditionDTO.getEndScn().longValue();
            map.put("startScn", startScn);
            map.put("endScn", endScn);
            createDataFile(parentDir, fileName);
            captureDataFromOracleLog(datasourceDTO, Integer.parseInt(datasourceDTO.getVersion().substring(0, datasourceDTO.getVersion().indexOf("."))));
            TimeUnit.MILLISECONDS.sleep(300L);
            modifyFile(parentDir, fileName);
        } catch (Exception e) {
            log.error("任务Id:【{}】第【{}】个SCN片段[{}-{})的数据采集异常!", captureConditionDTO.getTaskId(), captureConditionDTO.getIndex(), captureConditionDTO.getStartScn().toString(), captureConditionDTO.getEndScn().toString(), e);
            throw new RuntimeException(MessageFormat.format("任务Id:【{0}】第【{1}】个SCN片段[{2}-{3})的数据采集异常:{4}", captureConditionDTO.getTaskId(), captureConditionDTO.getIndex(), captureConditionDTO.getStartScn().toString(), captureConditionDTO.getEndScn().toString(), e.getMessage()));
        }
        return generatorCaptureRecord();
    }

    abstract void analyseDataFromResultSet(ResultSet resultSet) throws SQLException;

    protected List<SQLStatement> getSQLStatementList(String sql) {
        return SQLUtils.parseStatements(sql, JdbcConstants.ORACLE.name());
    }

    protected OriginDataDTO generatorOriginDataDTO(String taskId, @NotNull CaptureTableDTO captureTableDTO, long lastScn, @NotNull Timestamp timestamp, OperationType operationType) {
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

    protected OriginDataDTO analyseUpdateDml(String taskId, SQLObject sqlObject, CaptureTableDTO captureTableDTO, long lastScn, Timestamp timestamp) {
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

    protected OriginDataDTO analyseDeleteDml(String taskId, SQLObject sqlObject, CaptureTableDTO captureTableDTO, long lastScn, Timestamp timestamp) {
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

    protected OriginDataDTO analyseInsertDml(String taskId, SQLObject sqlObject, CaptureTableDTO captureTableDTO, long lastScn, Timestamp timestamp) {
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

    protected String getSqlRedo(@NotNull ResultSet resultSet) throws SQLException {
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

    protected String getDataJson(Object object) {
        atomicReference.set((OriginDataDTO) object);
        if (Objects.nonNull(commonConfig.getSupportEncrypt()) && commonConfig.getSupportEncrypt()) {
            return EncryptUtil.encryptPassword(JacksonUtil.toJson(object));
        }
        return JacksonUtil.toJson(object);
    }

    protected String toJsonFromList(List<?> list) {
        if (CollectionUtils.isEmpty(list)) {
            return null;
        } else {
            StringBuilder builder = new StringBuilder();
            for (Iterator<?> iterator = list.iterator(); iterator.hasNext(); ) {
                builder.append(getDataJson(iterator.next()));
                if (iterator.hasNext()) {
                    builder.append('\n');
                }
            }
            return builder.toString();
        }
    }

    protected void sendCaptureMetrics() {
        if (Objects.nonNull(atomicReference.get())) {
            OriginDataDTO originDataDTO = atomicReference.get();
            DbIncrementMetricsDTO dbIncrementMetricsDTO = new DbIncrementMetricsDTO();
            dbIncrementMetricsDTO.setTaskId(originDataDTO.getTaskId());
            dbIncrementMetricsDTO.setTargetSchema(originDataDTO.getSchema());
            dbIncrementMetricsDTO.setTargetTable(originDataDTO.getTable());
            dbIncrementMetricsDTO.setCaptureTime(Timestamp.valueOf(LocalDateTime.now()));
            dbIncrementMetricsDTO.setCaptureDelay(originDataDTO.getCaptureTimestamp() - originDataDTO.getTimestamp());
            dataProducer.put(dbIncrementMetricsDTO);
        }
    }

    private void modifyFile(String parentDir, String fileName) throws IOException {
        String prefixName = fileName + '@';
        FileConvertHandler.stop(prefixName);
        ImmutableList<File> fileList = FileUtil.searchFiles(parentDir, prefixName).toSortedList(Comparator.comparing(File::getName));
        for (File file : fileList) {
            String dataFileName = file.getName();
            if (dataFileName.equals(prefixName + Constants.TEMP_SUFFIX)) {
                dataFileName = dataFileName.substring(0, dataFileName.indexOf(".")) + "last@" + fileList.size();
                File newFile = new File(parentDir + File.separator + dataFileName + Constants.DATA_SUFFIX);
                Files.move(file, newFile);
            } else {
                dataFileName = dataFileName.substring(0, dataFileName.indexOf("."));
                File newFile = new File(parentDir + File.separator + dataFileName + Constants.DATA_SUFFIX);
                Files.move(file, newFile);
            }
        }
    }

    private String generatorCaptureRecord() {
        DbCaptureRecordDTO dbCaptureRecordDTO = new DbCaptureRecordDTO();
        dbCaptureRecordDTO.setTaskId(captureConditionDTO.getTaskId());
        dbCaptureRecordDTO.setCaptureIndex(captureConditionDTO.getIndex());
        dbCaptureRecordDTO.setCaptureStartScn(captureConditionDTO.getStartScn().toString());
        dbCaptureRecordDTO.setCaptureEndScn(captureConditionDTO.getEndScn().toString());
        return dbCaptureRecordDTO.toString();
    }

    private void createDataFile(String parentDir, String fileName) {
        logger = FileConvertHandler.createOrGetHandler(parentDir, fileName + '@', Constants.TEMP_SUFFIX, commonConfig.getDataFileThreshold(), Boolean.FALSE);
        if (!new File(parentDir + File.separator + fileName + '@' + Constants.TEMP_SUFFIX).exists()) {
            createDataFile(parentDir, fileName);
        }
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
