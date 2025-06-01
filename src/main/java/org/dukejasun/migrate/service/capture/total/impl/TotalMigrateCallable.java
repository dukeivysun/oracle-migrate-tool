package org.dukejasun.migrate.service.capture.total.impl;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.handler.DbMigrateResultStatisticsHandler;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.DbMigrateResultStatisticsDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureColumnDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.MigrateResultDTO;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.utils.FileUtil;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.driver.OracleConnection;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
public class TotalMigrateCallable implements Callable<String> {
    private final Integer fetchSize;
    private final Integer commitSize;
    private final String taskId;
    private final String parentDir;
    private final CommonConfig commonConfig;
    private final DataProducer dataProducer;
    private final JdbcTemplate jdbcTemplate;
    private final OracleConnection oracleConnection;
    private final CaptureTableDTO captureTableDTO;
    private final DatasourceDTO targetSourceConfig;
    private final CountDownLatch countDownLatch;
    private final DatabaseFeatures databaseFeatures;
    private final DatabaseFeatures targetDatabaseFeatures;
    private final DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler;

    public TotalMigrateCallable(String taskId, JdbcTemplate jdbcTemplate, OracleConnection oracleConnection, @NotNull CommonConfig commonConfig, CaptureTableDTO captureTableDTO, DatasourceDTO targetSourceConfig, CountDownLatch countDownLatch, DatabaseFeatures databaseFeatures, DatabaseFeatures targetDatabaseFeatures, String parentDir, DataProducer dataProducer, DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler) {
        this.taskId = taskId;
        this.parentDir = parentDir;
        this.fetchSize = commonConfig.getFetchSize();
        this.commitSize = commonConfig.getCommitSize();
        this.commonConfig = commonConfig;
        this.dataProducer = dataProducer;
        this.jdbcTemplate = jdbcTemplate;
        this.oracleConnection = oracleConnection;
        this.captureTableDTO = captureTableDTO;
        this.targetSourceConfig = targetSourceConfig;
        this.countDownLatch = countDownLatch;
        this.databaseFeatures = databaseFeatures;
        this.targetDatabaseFeatures = targetDatabaseFeatures;
        this.dbMigrateResultStatisticsHandler = dbMigrateResultStatisticsHandler;
    }

    @Override
    public String call() {
        try {
            List<CaptureColumnDTO> captureColumnDTOList = captureTableDTO.getColumnMetaList();
            if (!CollectionUtils.isEmpty(captureColumnDTOList)) {
                String tableName = databaseFeatures.getTableName(captureTableDTO.getName());
                String schemaName = databaseFeatures.getSchema(captureTableDTO.getSchema());
                String targetTableName = targetDatabaseFeatures.getTableName(captureTableDTO.getTargetName());
                String targetSchemaName = targetDatabaseFeatures.getSchema(captureTableDTO.getTargetSchema());
                log.info("任务Id:【{}】开始{}.{}表全量迁移......", taskId, schemaName, tableName);
                truncateTargetTable();
                List<String> columnNameList = captureColumnDTOList.stream().map(CaptureColumnDTO::getName).collect(Collectors.toList());
                String destSql = null;
                if (Objects.isNull(commonConfig.getTotalDataToFile()) || !commonConfig.getTotalDataToFile()) {
                    destSql = generatorWriteTargetScript(columnNameList, targetSchemaName, targetTableName, commonConfig.insertModel());
                }
                String querySql = MessageFormat.format(IConstants.QUERY_TABLE_NUMBERS, schemaName, tableName);
                if (StringUtils.isNotBlank(captureTableDTO.getConditions())) {
                    querySql = MessageFormat.format(IConstants.QUERY_TABLE_NUMBERS_WITH_CONDITIONS, schemaName, tableName, captureTableDTO.getConditions());
                }
                Long count = jdbcTemplate.queryForObject(querySql, Long.class);
                if (Objects.nonNull(count) && count > 0L) {
                    if (StringUtils.isBlank(captureTableDTO.getFlashBack()) && CollectionUtils.isEmpty(captureTableDTO.getPartitionNameList())) {
                        if (Objects.nonNull(commonConfig.getTotalMigrateByPage()) && commonConfig.getTotalMigrateByPage()) {
                            transmissionByPageWithRowId(count, captureColumnDTOList, schemaName, tableName, destSql, columnNameList);
                        } else {
                            transmissionNoPage(count, captureColumnDTOList, schemaName, tableName, destSql, columnNameList);
                        }
                    } else {
                        if (!CollectionUtils.isEmpty(captureTableDTO.getPartitionNameList())) {
                            transmissionByPartition(count, captureColumnDTOList, captureTableDTO.getPartitionNameList(), schemaName, tableName, destSql, columnNameList);
                        } else {
                            transmissionByFlashBack(count, captureColumnDTOList, captureTableDTO.getFlashBack(), schemaName, tableName, destSql, columnNameList);
                        }
                    }
                } else {
                    sendResult(count, null);
                    captureTableDTO.setEndTime(LocalDateTime.now());
                    captureTableDTO.getLongAdder().increment();
                }
            } else {
                captureTableDTO.setEndTime(LocalDateTime.now());
                captureTableDTO.getLongAdder().increment();
            }
        } catch (Exception e) {
            captureTableDTO.setEndTime(LocalDateTime.now());
            captureTableDTO.getLongAdder().increment();
            throw new RuntimeException(e.getMessage());
        } finally {
            countDownLatch.countDown();
        }
        return "successfully";
    }

    private void transmissionNoPage(Long count, List<CaptureColumnDTO> captureColumnDTOList, String schemaName, String tableName, String destSql, List<String> columnNameList) throws IOException {
        new ReadAndWriteNoPage(taskId, count, captureColumnDTOList, schemaName, tableName, destSql, columnNameList, parentDir, commonConfig, targetSourceConfig, jdbcTemplate, oracleConnection, captureTableDTO, databaseFeatures, targetDatabaseFeatures, dataProducer).start();
    }

    private void transmissionByFlashBack(Long count, List<CaptureColumnDTO> captureColumnDTOList, String flashBack, String schemaName, String tableName, String destSql, List<String> columnNameList) throws IOException {
        new ReadAndWriteByFlashBack(taskId, count, captureColumnDTOList, flashBack, schemaName, tableName, destSql, columnNameList, parentDir, commonConfig, targetSourceConfig, jdbcTemplate, oracleConnection, captureTableDTO, databaseFeatures, targetDatabaseFeatures, dataProducer).start();
    }

    private void transmissionByPartition(Long count, List<CaptureColumnDTO> captureColumnDTOList, @NotNull List<String> partitionNameList, String schemaName, String tableName, String destSql, List<String> columnNameList) throws BrokenBarrierException, InterruptedException {
        int coreSize = partitionNameList.size();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(MessageFormat.format(Constants.TOTAL_SUB_THREAD_NAME, captureTableDTO.getSchema(), captureTableDTO.getName())).build();
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(coreSize, coreSize, 10, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        final CyclicBarrier barrier = new CyclicBarrier((coreSize + 1), () -> {
            log.info("任务Id:【{}】{}.{}表全量迁移完成!", taskId, captureTableDTO.getSchema(), captureTableDTO.getName());
            captureTableDTO.setEndTime(LocalDateTime.now());
            captureTableDTO.getLongAdder().increment();
        });
        ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
        for (String partitionName : partitionNameList) {
            ListenableFuture<Object> listenableFuture = listeningExecutorService.submit(new ReadAndWriteByPartitionCallable(taskId, count, commitSize, schemaName, tableName, partitionName, destSql, parentDir, commonConfig, targetSourceConfig, jdbcTemplate, oracleConnection, captureTableDTO, captureColumnDTOList, columnNameList, barrier, databaseFeatures, targetDatabaseFeatures, dataProducer));
            Futures.addCallback(listenableFuture, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object value) {
                }

                @Override
                public void onFailure(Throwable throwable) {
                    MigrateResultDTO migrateResultDTO = JacksonUtil.fromJson(throwable.getMessage(), MigrateResultDTO.class);
                    if (Objects.nonNull(migrateResultDTO)) {
                        sendResult(count, migrateResultDTO);
                        log.error("任务Id:【{}】{}.{}表{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), migrateResultDTO.getErrorReason());
                    }
                }
            }, executorService);
        }
        barrier.await();
    }

    private void transmissionByPageWithRowId(Long count, List<CaptureColumnDTO> captureColumnDTOList, String schemaName, String tableName, String destSql, List<String> columnNameList) throws BrokenBarrierException, InterruptedException {
        long coreSize = (long) Math.ceil(((double) count / fetchSize));
        long barrierSize = coreSize;
        int totalMigrateThreadNumber = Objects.isNull(commonConfig.getTotalMigrateThreadNumber()) ? 4 : commonConfig.getTotalMigrateThreadNumber();
        if (coreSize > totalMigrateThreadNumber) {
            coreSize = totalMigrateThreadNumber;
        }
        List<String> listQuery = Lists.newArrayList();
        String querySql = String.format(IConstants.QUERY_TABLE_BY_ROWID_PAGE_2, columnNameList.stream().map(databaseFeatures::getColumnName).collect(Collectors.joining(",")), MessageFormat.format(Constants.SCHEMA_TABLE_KEY, schemaName, tableName), coreSize, captureTableDTO.getName(), captureTableDTO.getSchema(), coreSize, coreSize, captureTableDTO.getName(), captureTableDTO.getSchema());
        jdbcTemplate.query(querySql, (resultSet) -> {
            do {
                listQuery.add(resultSet.getString(1));
            } while (resultSet.next());
        });
        if (!CollectionUtils.isEmpty(listQuery)) {
            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(MessageFormat.format(Constants.TOTAL_SUB_THREAD_NAME, captureTableDTO.getSchema(), captureTableDTO.getName())).build();
            ThreadPoolExecutor executorService = new ThreadPoolExecutor((int) coreSize, (int) barrierSize, 10, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
            final CyclicBarrier barrier = new CyclicBarrier((int) (barrierSize + 1), () -> {
                log.info("任务Id:【{}】{}.{}表全量迁移完成!", taskId, captureTableDTO.getSchema(), captureTableDTO.getName());
                captureTableDTO.setEndTime(LocalDateTime.now());
                captureTableDTO.getLongAdder().increment();
            });
            ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
            for (String query : listQuery) {
                ListenableFuture<Object> listenableFuture = listeningExecutorService.submit(new ReadAndWriteByRowIdCallable(taskId, count, commitSize, schemaName, tableName, destSql, query, parentDir, commonConfig, targetSourceConfig, jdbcTemplate, oracleConnection, captureTableDTO, captureColumnDTOList, columnNameList, barrier, targetDatabaseFeatures, dataProducer));
                Futures.addCallback(listenableFuture, new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object value) {
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        MigrateResultDTO migrateResultDTO = JacksonUtil.fromJson(throwable.getMessage(), MigrateResultDTO.class);
                        if (Objects.nonNull(migrateResultDTO)) {
                            sendResult(count, migrateResultDTO);
                            log.error("任务Id:【{}】{}.{}表{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), migrateResultDTO.getErrorReason());
                        } else {
                            log.error(throwable.getMessage());
                        }
                    }
                }, executorService);
            }
            barrier.await();
        }
    }

    private void transmissionByPage(Long count, List<CaptureColumnDTO> captureColumnDTOList, String schemaName, String tableName, String destSql, List<String> columnNameList) throws BrokenBarrierException, InterruptedException {
        int endIndex = fetchSize;
        long coreSize = (long) Math.ceil(((double) count / fetchSize));
        long barrierSize = coreSize;
        int totalMigrateThreadNumber = Objects.isNull(commonConfig.getTotalMigrateThreadNumber()) ? 4 : commonConfig.getTotalMigrateThreadNumber();
        if (coreSize > totalMigrateThreadNumber) {
            coreSize = totalMigrateThreadNumber;
        }
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(MessageFormat.format(Constants.TOTAL_SUB_THREAD_NAME, captureTableDTO.getSchema(), captureTableDTO.getName())).build();
        ThreadPoolExecutor executorService = new ThreadPoolExecutor((int) coreSize, (int) barrierSize, 10, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        final CyclicBarrier barrier = new CyclicBarrier((int) (barrierSize + 1), () -> {
            log.info("任务Id:【{}】{}.{}表全量迁移完成!", taskId, captureTableDTO.getSchema(), captureTableDTO.getName());
            captureTableDTO.setEndTime(LocalDateTime.now());
            captureTableDTO.getLongAdder().increment();
        });
        String columns = getColumns(captureTableDTO.getPrimaryKeyList(), schemaName, tableName);
        ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
        for (int startIndex = 0; startIndex < count; startIndex += fetchSize) {
            ListenableFuture<Object> listenableFuture = listeningExecutorService.submit(new ReadAndWriteByPageCallable(taskId, count, startIndex, endIndex, commitSize, schemaName, tableName, destSql, parentDir, commonConfig, targetSourceConfig, jdbcTemplate, oracleConnection, captureTableDTO, captureColumnDTOList, columnNameList, columns, barrier, databaseFeatures, targetDatabaseFeatures, dataProducer));
            Futures.addCallback(listenableFuture, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object value) {
                }

                @Override
                public void onFailure(Throwable throwable) {
                    MigrateResultDTO migrateResultDTO = JacksonUtil.fromJson(throwable.getMessage(), MigrateResultDTO.class);
                    if (Objects.nonNull(migrateResultDTO)) {
                        sendResult(count, migrateResultDTO);
                        log.error("任务Id:【{}】{}.{}表{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), migrateResultDTO.getErrorReason());
                    }
                }
            }, executorService);
            endIndex += fetchSize;
        }
        barrier.await();
    }

    private void sendResult(Long count, MigrateResultDTO migrateResultDTO) {
        DbMigrateResultStatisticsDTO dbMigrateResultStatisticsDTO = new DbMigrateResultStatisticsDTO();
        dbMigrateResultStatisticsDTO.setTaskId(taskId);
        dbMigrateResultStatisticsDTO.setTransmissionType(MigrateType.TOTAL.getCode());
        dbMigrateResultStatisticsDTO.setSourceSchema(captureTableDTO.getSchema());
        dbMigrateResultStatisticsDTO.setSourceTable(captureTableDTO.getName());
        dbMigrateResultStatisticsDTO.setTargetSchema(captureTableDTO.getTargetSchema());
        dbMigrateResultStatisticsDTO.setTargetTable(captureTableDTO.getTargetName());
        if (Objects.nonNull(migrateResultDTO)) {
            dbMigrateResultStatisticsDTO.setTransmissionResult(StatusEnum.ERROR.getCode());
            dbMigrateResultStatisticsDTO.setErrorReason(migrateResultDTO.getErrorReason());
        }
        dbMigrateResultStatisticsDTO.setSourceRecord(String.valueOf(count));
        dbMigrateResultStatisticsDTO.setTargetRecord("0");
        dbMigrateResultStatisticsDTO.setUpdateTime(LocalDateTime.now());
        dataProducer.put(dbMigrateResultStatisticsDTO);
    }

    private String generatorWriteTargetScript(List<String> columnNameList, String targetSchema, String targetTableName, @NotNull Boolean insertModel) {
        if (insertModel) {
            StringBuilder builder = new StringBuilder("INSERT INTO " + targetSchema + '.' + targetTableName + '(');
            for (String columnName : columnNameList) {
                builder.append(targetDatabaseFeatures.getColumnName(columnName)).append(',');
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(") VALUES (");
            for (int i = 0, n = columnNameList.size(); i < n; i++) {
                builder.append('?').append(',');
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(')');
            return builder.toString();
        }
        return targetDatabaseFeatures.destinationSql(columnNameList, targetSchema, targetTableName);
    }

    private void truncateTargetTable() throws IOException {
        if (Objects.nonNull(commonConfig.getTotalMigrateTruncateTargetTable()) && commonConfig.getTotalMigrateTruncateTargetTable()) {
            if (Objects.nonNull(commonConfig.getTotalDataToFile()) && commonConfig.getTotalDataToFile()) {
                log.info("清除{}.{}表的数据文件......", captureTableDTO.getSchema(), captureTableDTO.getName());
                FileUtil.deleteFile(commonConfig.getOutputPath() + File.separator + MigrateType.TOTAL.getCode().toLowerCase() + File.separator + taskId, MessageFormat.format(Constants.SCHEMA_TABLE_KEY, captureTableDTO.getSchema(), captureTableDTO.getName()));
            } else {
                DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(targetSourceConfig, commonConfig);
                if (Objects.nonNull(dataSource)) {
                    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                    log.info("清除目标{}.{}表的数据......", captureTableDTO.getTargetSchema(), captureTableDTO.getTargetName());
                    boolean result = targetDatabaseFeatures.truncateData(jdbcTemplate, captureTableDTO.getTargetSchema(), captureTableDTO.getTargetName());
                    if (!result) {
                        log.warn("目标数据库{}.{}表数据清除失败!", captureTableDTO.getTargetSchema(), captureTableDTO.getTargetName());
                    }
                }
            }
        }
        dbMigrateResultStatisticsHandler.deleteByTaskIdAndSourceSchemaAndSourceTable(taskId, captureTableDTO.getSchema(), captureTableDTO.getName());
    }

    private @NotNull String getColumns(@NotNull List<String> columnNameList, String schemaName, String tableName) {
        StringBuilder bufferColumn = new StringBuilder();
        for (Iterator<String> iterator = columnNameList.iterator(); iterator.hasNext(); ) {
            String pkName = iterator.next();
            if (StringUtils.isBlank(pkName)) {
                continue;
            }
            bufferColumn.append(schemaName).append('.').append(tableName).append('.').append(pkName);
            if (iterator.hasNext()) {
                bufferColumn.append(',');
            }
        }

        String value = bufferColumn.substring(bufferColumn.length() - 1);
        if (Constants.COMMA.equals(value)) {
            bufferColumn.deleteCharAt(bufferColumn.length() - 1);
        }
        return bufferColumn.toString();
    }
}
