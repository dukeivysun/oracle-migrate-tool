package org.dukejasun.migrate.service.capture.increment.capture.impl;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.google.common.collect.Lists;
import org.dukejasun.migrate.cache.CachedOperation;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.OperationType;
import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.OriginDataDTO;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author dukedpsun
 */
@Slf4j
public class CaptureDataByTransactionalCallable extends AbstractCaptureDataCallable {
    private final CachedOperation<String, Object> cachedOperation;

    public CaptureDataByTransactionalCallable(CaptureConditionDTO captureConditionDTO, CommonConfig commonConfig, Map<String, DataTypeConvert> dataTypeConvertMap, CachedOperation<String, Object> cachedOperation, DataProducer dataProducer) {
        super(captureConditionDTO, commonConfig, dataTypeConvertMap, dataProducer);
        this.cachedOperation = cachedOperation;
    }

    @Override
    public void analyseDataFromResultSet(@NotNull ResultSet resultSet) throws SQLException {
        long lastScn = 0;
        String taskId = getCaptureConditionDTO().getTaskId();
        while (resultSet.next()) {
            if (getAtomicInteger().intValue() > 0) {
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
                                if (valueObject instanceof List) {
                                    String value = toJsonFromList((List<?>) valueObject);
                                    if (StringUtils.isNotBlank(value)) {
                                        getLogger().info(value);
                                        sendCaptureMetrics();
                                    }
                                } else {
                                    getLogger().info(getDataJson(valueObject));
                                    sendCaptureMetrics();
                                }
                                cachedOperation.delete(xId);
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
        getMap().put("startScn", lastScn == 0L ? getEndScn() : lastScn);
    }
}
