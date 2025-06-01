package org.dukejasun.migrate.service.capture.total.impl;

import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureColumnDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.MigrateResultDTO;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.utils.IDTools;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.driver.OracleConnection;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 通过select * from方式查询
 *
 * @author dukedpsun
 */
@Slf4j
public class ReadAndWriteNoPage extends ReadAndWriteCommonService {
    private final String taskId;
    private final Long dataCount;
    private final Integer commitSize;
    private final String schemaName;
    private final String tableName;
    private final String destSql;
    private final String parentDir;
    private final CommonConfig commonConfig;
    private final JdbcTemplate jdbcTemplate;
    private final DataProducer dataProducer;
    private final OracleConnection oracleConnection;
    private final DatasourceDTO targetSourceConfig;
    private final CaptureTableDTO captureTableDTO;
    private final List<CaptureColumnDTO> captureColumnDTOList;
    private final List<String> columnNameList;
    private final DatabaseFeatures databaseFeatures;
    private final DatabaseFeatures targetDatabaseFeatures;

    public ReadAndWriteNoPage(String taskId, Long count, List<CaptureColumnDTO> captureColumnDTOList, String schemaName, String tableName, String destSql, List<String> columnNameList, String parentDir, @NotNull CommonConfig commonConfig, DatasourceDTO targetSourceConfig, JdbcTemplate jdbcTemplate, OracleConnection oracleConnection, CaptureTableDTO captureTableDTO, DatabaseFeatures databaseFeatures, DatabaseFeatures targetDatabaseFeatures, DataProducer dataProducer) {
        this.taskId = taskId;
        this.dataCount = count;
        this.captureColumnDTOList = captureColumnDTOList;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.destSql = destSql;
        this.columnNameList = columnNameList;
        this.parentDir = parentDir;
        this.commonConfig = commonConfig;
        this.targetSourceConfig = targetSourceConfig;
        this.jdbcTemplate = jdbcTemplate;
        this.oracleConnection = oracleConnection;
        this.captureTableDTO = captureTableDTO;
        this.databaseFeatures = databaseFeatures;
        this.targetDatabaseFeatures = targetDatabaseFeatures;
        this.commitSize = commonConfig.getCommitSize();
        this.dataProducer = dataProducer;
    }

    public void start() throws IOException {
        String querySql = MessageFormat.format(IConstants.QUERY_TABLE_BY_NO_PAGE, columnNameList.stream().map(databaseFeatures::getColumnName).collect(Collectors.joining(",")), schemaName, tableName);
        if (StringUtils.isNotBlank(captureTableDTO.getConditions())) {
            querySql = MessageFormat.format(IConstants.QUERY_TABLE_BY_NO_PAGE_WITH_CONDITIONS, columnNameList.stream().map(databaseFeatures::getColumnName).collect(Collectors.joining(",")), schemaName, tableName, captureTableDTO.getConditions());
        }
        String uuid = String.valueOf(IDTools.getUUID());
        String dataFileName = MessageFormat.format(Constants.SCHEMA_TABLE_KEY, captureTableDTO.getSchema(), captureTableDTO.getName());
        AtomicReference<File> originFile = createDataFile(parentDir, dataFileName, uuid, log);
        try {
            if (StringUtils.isNotBlank(querySql)) {
                AtomicLong rowCount = new AtomicLong();
                StringBuilder builder = new StringBuilder();
                String finalQuerySql = querySql;
                log.debug("{}.{}表的查询DQL:{}", captureTableDTO.getSchema(), captureTableDTO.getName(), querySql);
                jdbcTemplate.query(connection -> {
                    PreparedStatement preparedStatement = connection.prepareStatement(finalQuerySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    preparedStatement.setFetchSize(commonConfig.getFetchSize());
                    preparedStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
                    return preparedStatement;
                }, resultSet -> {
                    writeToTarget(taskId, schemaName, tableName, destSql, oracleConnection, commonConfig, captureTableDTO, parentDir, columnNameList, commitSize, resultSet, originFile, builder, rowCount, captureColumnDTOList, targetSourceConfig, targetDatabaseFeatures);
                });
                if (StringUtils.isBlank(parentDir)) {
                    writeDataToTarget(taskId, schemaName, tableName, builder, destSql, targetSourceConfig, targetDatabaseFeatures);
                }
                sendResult(taskId, dataCount, captureTableDTO, rowCount.get(), Objects.nonNull(parentDir), dataProducer);
            }
        } catch (Exception e) {
            MigrateResultDTO migrateResultDTO = new MigrateResultDTO();
            migrateResultDTO.setErrorReason(MessageFormat.format(Constants.TOTAL_ERROR_MESSAGE_3, e.getMessage()));
            throw new RuntimeException(migrateResultDTO.toString());
        } finally {
            if (Objects.nonNull(originFile)) {
                renameFile(originFile.get(), parentDir);
            }
            captureTableDTO.setEndTime(LocalDateTime.now());
            captureTableDTO.getLongAdder().increment();
        }
    }
}
