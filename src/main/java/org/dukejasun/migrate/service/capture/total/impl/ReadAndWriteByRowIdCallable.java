package org.dukejasun.migrate.service.capture.total.impl;

import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
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
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author dukedpsun
 */
@Slf4j
public class ReadAndWriteByRowIdCallable extends ReadAndWriteCommonService implements Callable<Object> {

    private final String taskId;
    private final Long dataCount;
    private final Integer commitSize;
    private final String schemaName;
    private final String tableName;
    private final String destSql;
    private final String querySql;
    private final String parentDir;
    private final CommonConfig commonConfig;
    private final JdbcTemplate jdbcTemplate;
    private final DataProducer dataProducer;
    private final OracleConnection oracleConnection;
    private final DatasourceDTO targetSourceConfig;
    private final CaptureTableDTO captureTableDTO;
    private final List<CaptureColumnDTO> captureColumnDTOList;
    private final List<String> columnNameList;
    private final CyclicBarrier barrier;
    private final DatabaseFeatures databaseFeatures;

    public ReadAndWriteByRowIdCallable(String taskId, Long dataCount, Integer commitSize, String schemaName, String tableName, String destSql, String querySql, String parentDir, CommonConfig commonConfig, DatasourceDTO targetSourceConfig, JdbcTemplate jdbcTemplate, OracleConnection oracleConnection, CaptureTableDTO captureTableDTO, List<CaptureColumnDTO> captureColumnDTOList, List<String> columnNameList, CyclicBarrier barrier, DatabaseFeatures databaseFeatures, DataProducer dataProducer) {
        this.taskId = taskId;
        this.dataCount = dataCount;
        this.commitSize = commitSize;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.destSql = destSql;
        this.querySql = querySql;
        this.parentDir = parentDir;
        this.commonConfig = commonConfig;
        this.jdbcTemplate = jdbcTemplate;
        this.oracleConnection = oracleConnection;
        this.targetSourceConfig = targetSourceConfig;
        this.captureTableDTO = captureTableDTO;
        this.captureColumnDTOList = captureColumnDTOList;
        this.columnNameList = columnNameList;
        this.barrier = barrier;
        this.databaseFeatures = databaseFeatures;
        this.dataProducer = dataProducer;
    }

    @Override
    public Object call() throws Exception {
        String uuid = String.valueOf(IDTools.getUUID());
        String dataFileName = MessageFormat.format(Constants.SCHEMA_TABLE_KEY, captureTableDTO.getSchema(), captureTableDTO.getName());
        AtomicReference<File> originFile = createDataFile(parentDir, dataFileName, uuid, log);
        try {
            if (StringUtils.isNotBlank(querySql)) {
                AtomicLong rowCount = new AtomicLong();
                StringBuilder builder = new StringBuilder();
                log.debug("{}.{}表的查询DQL:{}", captureTableDTO.getSchema(), captureTableDTO.getName(), querySql);
                jdbcTemplate.query(connection -> {
                    PreparedStatement preparedStatement = connection.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    preparedStatement.setFetchSize(commonConfig.getFetchSize());
                    preparedStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
                    return preparedStatement;
                }, resultSet -> {
                    writeToTarget(taskId, schemaName, tableName, destSql, oracleConnection, commonConfig, captureTableDTO, parentDir, columnNameList, commitSize, resultSet, originFile, builder, rowCount, captureColumnDTOList, targetSourceConfig, databaseFeatures);
                });
                if (StringUtils.isBlank(parentDir)) {
                    writeDataToTarget(taskId, schemaName, tableName, builder, destSql, targetSourceConfig, databaseFeatures);
                }
                log.info("任务Id:【{}】完成{}.{}表部分数据迁移!", taskId, schemaName, tableName);
                sendResult(taskId, dataCount, captureTableDTO, rowCount.get(), Objects.nonNull(parentDir), dataProducer);
            }
        } catch (Exception e) {
            MigrateResultDTO migrateResultDTO = new MigrateResultDTO();
            migrateResultDTO.setErrorReason(MessageFormat.format("{}.{}表部分数据迁移失败,异常:{}", schemaName, tableName, e.getMessage()));
            throw new RuntimeException(migrateResultDTO.toString());
        } finally {
            if (Objects.nonNull(originFile)) {
                renameFile(originFile.get(), parentDir);
            }
            barrier.await();
        }
        return "successfully";
    }
}
