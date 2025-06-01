package org.dukejasun.migrate.service.sink.increment;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.OperationType;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.handler.DbIncrementRecordHandler;
import org.dukejasun.migrate.handler.FileConvertHandler;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.event.DbIncrementMetricsDTO;
import org.dukejasun.migrate.model.dto.event.DbSinkRecordDTO;
import org.dukejasun.migrate.model.dto.event.SinkDataDTO;
import org.dukejasun.migrate.model.dto.output.ActualDataDTO;
import org.dukejasun.migrate.model.dto.output.ColumnDataDTO;
import org.dukejasun.migrate.model.entity.DbIncrementRecord;
import org.dukejasun.migrate.model.jdbc.LogPreparedStatement;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.model.EventObject;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Component
public class WriteDataToTarget {
    private final int keyConflictOperator;
    private final CommonConfig commonConfig;
    private final ExecutorService executorService;
    private final ListeningExecutorService listeningExecutorService;
    private final Map<String, DatabaseFeatures> databaseFeaturesMap;
    private final ApplicationContext applicationContext;
    private final DbIncrementRecordHandler dbIncrementRecordHandler;
    private final DataProducer dataProducer;

    @Autowired
    public WriteDataToTarget(@NotNull CommonConfig commonConfig, Map<String, DatabaseFeatures> databaseFeaturesMap, ApplicationContext applicationContext, DbIncrementRecordHandler dbIncrementRecordHandler, MigrateResultsStatisticsService migrateResultsStatisticsService) {
        this.commonConfig = commonConfig;
        this.databaseFeaturesMap = databaseFeaturesMap;
        this.applicationContext = applicationContext;
        this.dbIncrementRecordHandler = dbIncrementRecordHandler;

        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
        migrateWorkHandler.setMigrateResultsStatisticsService(migrateResultsStatisticsService);
        dataProducer = new DataProducer(disruptor, migrateWorkHandler);

        keyConflictOperator = Objects.nonNull(commonConfig.getPrimaryKeyConflictOperator()) ? commonConfig.getPrimaryKeyConflictOperator() : 1;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(Constants.SINK_THREAD_NAME).build();
        executorService = new ThreadPoolExecutor(commonConfig.getIncrementWriteThreadNumber(), commonConfig.getIncrementWriteThreadNumber() * 2, 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2048), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
    }

    public String sinkDataToTarget(EventObject eventObject) {
        SinkDataDTO sinkDataDTO = (SinkDataDTO) eventObject;
        List<ActualDataDTO> actualDataDTOList = sinkDataDTO.getActualDataDTOList();
        if (!CollectionUtils.isEmpty(actualDataDTOList)) {
            DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(sinkDataDTO.getDatasourceDTO(), commonConfig);
            Map<Boolean, List<ActualDataDTO>> keyValueMap = actualDataDTOList.stream().collect(Collectors.groupingBy(ActualDataDTO::getHasKey));
            DatabaseFeatures databaseFeatures = databaseFeaturesMap.get(sinkDataDTO.getDatasourceDTO().getType().name() + "_FEATURES");
            parallelExecute(sinkDataDTO.getDealIndex(), sinkDataDTO.getTaskId(), keyValueMap.get(true), sinkDataDTO, dataSource, databaseFeatures);
            singleExecute(sinkDataDTO.getDealIndex(), sinkDataDTO.getTaskId(), keyValueMap.get(false), sinkDataDTO, dataSource, databaseFeatures);
        }
        return sinkDataDTO.toString();
    }

    private void singleExecute(Integer index, String taskId, List<ActualDataDTO> actualDataDTOList, SinkDataDTO sinkDataDTO, DataSource dataSource, DatabaseFeatures databaseFeatures) {
        if (!CollectionUtils.isEmpty(actualDataDTOList)) {
            Map<OperationType, List<ActualDataDTO>> operationTypeListMap = actualDataDTOList.stream().collect(Collectors.groupingBy(ActualDataDTO::getOperation));
            log.info("任务Id:【{}】数据文件索引:{},数据处理!", sinkDataDTO.getTaskId(), sinkDataDTO.getDealIndex());
            operationTypeListMap.forEach((operationType, dataDTOList) -> {
                switch (operationType) {
                    case CREATE:
                    case DELETE:
                    case UPDATE:
                        try {
                            singleExecute(index, taskId, dataDTOList, dataSource, keyConflictOperator, databaseFeatures);
                        } catch (SQLException e) {
                            log.error("任务Id:【{}】数据同步异常,数据文件索引:{}.", taskId, index, e);
                            throw new RuntimeException(e.getMessage());
                        }
                        break;
                    case TRUNCATE:
                        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                        dataDTOList.forEach(actualDataDTO -> {
                            try {
                                jdbcTemplate.execute(actualDataDTO.getScript());
                            } catch (Exception e) {
                                log.error("SQL执行异常!", e);
                            }
                        });
                        break;
                    default:
                        break;
                }
            });
            log.info("任务Id:【{}】数据文件索引:{},完成数据处理!", sinkDataDTO.getTaskId(), sinkDataDTO.getDealIndex());
            sendSinkRecord(sinkDataDTO);
        }
    }

    private void parallelExecute(Integer index, String taskId, List<ActualDataDTO> actualDataDTOList, SinkDataDTO sinkDataDTO, DataSource dataSource, DatabaseFeatures databaseFeatures) {
        if (!CollectionUtils.isEmpty(actualDataDTOList)) {
            Map<OperationType, List<ActualDataDTO>> operationTypeListMap = actualDataDTOList.stream().collect(Collectors.groupingBy(ActualDataDTO::getOperation));
            AtomicInteger batchIndex = new AtomicInteger(1);
            operationTypeListMap.forEach((operationType, dataDTOList) -> {
                Map<String, List<ActualDataDTO>> stringListMap = dataDTOList.stream().collect(Collectors.groupingBy(ActualDataDTO::getSchemaAndTable));
                stringListMap.forEach((schemaAndName, actualDataList) -> {
                    List<List<ActualDataDTO>> subDataList = Lists.partition(actualDataList, commonConfig.getCommitSize());
                    for (List<ActualDataDTO> list : subDataList) {
                        ListenableFuture<String> listenableFuture = listeningExecutorService.submit(new WriteDataCallable(index, batchIndex.intValue(), keyConflictOperator, taskId, commonConfig.getOutputPath(), commonConfig.getDataFileThreshold(), dataSource, list, databaseFeatures, sinkDataDTO));
                        batchIndex.addAndGet(1);
                        Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                            @Override
                            public void onSuccess(String value) {
                                if (StringUtils.isNotBlank(value)) {
                                    SinkDataDTO sinkDataDTO = JacksonUtil.fromJson(value, SinkDataDTO.class);
                                    sendSinkRecord(sinkDataDTO);
                                    log.info("任务Id:【{}】数据文件索引:{},文件内部序号:{},第{}批次数据完成批量处理!", sinkDataDTO.getTaskId(), sinkDataDTO.getDealIndex(), sinkDataDTO.getSerialNumber(), batchIndex);
                                }
                            }

                            @Override
                            public void onFailure(Throwable throwable) {
                                DbIncrementRecord dbIncrementRecord = dbIncrementRecordHandler.findIncrementRecord(taskId);
                                if (Objects.nonNull(dbIncrementRecord)) {
                                    Integer sinkIndex = dbIncrementRecord.getSinkIndex();
                                    if (sinkIndex > index) {
                                        SinkDataDTO sinkDataDTO = new SinkDataDTO();
                                        sinkDataDTO.setTaskId(taskId);
                                        sinkDataDTO.setDealIndex(index);
                                        sendSinkRecord(sinkDataDTO);
                                    }
                                }
                                try {
                                    TimeUnit.MILLISECONDS.sleep(2000L);
                                } catch (InterruptedException ignored) {
                                }
                                log.error(throwable.getMessage());
                                System.exit(SpringApplication.exit(applicationContext, () -> 0));
                            }
                        }, executorService);
                    }
                });
            });
        }
    }

    private void singleExecute(Integer index, String taskId, List<ActualDataDTO> actualDataDTOList, DataSource dataSource, int keyConflictOperator, DatabaseFeatures databaseFeatures) throws SQLException {
        if (!CollectionUtils.isEmpty(actualDataDTOList)) {
            try (Connection connection = dataSource.getConnection()) {
                for (ActualDataDTO actualDataDTO : actualDataDTOList) {
                    if (Objects.isNull(actualDataDTO.getScript())) {
                        log.error("任务Id:【{}】sql内容为空:{},不能在目标写入!", taskId, actualDataDTO);
                        continue;
                    }
                    LogPreparedStatement preparedStatement = null;
                    try {
                        preparedStatement = new LogPreparedStatement(connection, actualDataDTO.getScript());
                        addPreparedStatementObject(preparedStatement, actualDataDTO);
                        preparedStatement.addBatch();
                        preparedStatement.executeBatch();
                        preparedStatement.clearBatch();
                        sendSinkMetrics(actualDataDTO);
                    } catch (SQLException | ParseException | IOException e) {
                        if (!StringUtils.containsIgnoreCase(e.getMessage(), "duplicate key")) {
                            assert preparedStatement != null;
                            log.error("任务Id:【{}】dml:{}执行失败!", taskId, preparedStatement, e);
                            throw new RuntimeException("dml执行失败!", e);
                        } else {
                            switch (keyConflictOperator) {
                                case 0:
                                    log.error("任务Id:【{}】dml:{}执行失败!", taskId, actualDataDTO.getScript(), e);
                                    throw new RuntimeException("dml执行失败!", e);
                                case 1:
                                    deleteWithPk(taskId, actualDataDTO, connection, databaseFeatures);
                                    try {
                                        preparedStatement = new LogPreparedStatement(connection, actualDataDTO.getScript());
                                        addPreparedStatementObject(preparedStatement, actualDataDTO);
                                        log.debug(preparedStatement.toString());
                                        preparedStatement.execute();
                                        sendSinkMetrics(actualDataDTO);
                                    } catch (Exception t) {
                                        log.error("任务Id:【{}】序号:{},删除主键重复数据后重启插入失败!{}", taskId, index, preparedStatement, t);
                                        throw new RuntimeException("dml执行失败!", e);
                                    } finally {
                                        if (Objects.nonNull(preparedStatement)) {
                                            preparedStatement.close();
                                        }
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                    } finally {
                        if (Objects.nonNull(preparedStatement)) {
                            preparedStatement.close();
                        }
                    }
                }
            }
        }
    }

    private void deleteWithPk(String taskId, @NotNull ActualDataDTO actualDataDTO, Connection connection, DatabaseFeatures databaseFeatures) {
        List<String> pkColumnNameList = actualDataDTO.getPkColumnNameList();
        if (!CollectionUtils.isEmpty(pkColumnNameList)) {
            String schemaTable = actualDataDTO.getSchemaAndTable();
            StringBuilder builder = new StringBuilder();
            builder.append("DELETE FROM ").append(schemaTable).append(" WHERE ");
            for (int i = 0, n = pkColumnNameList.size(); i < n; i++) {
                String pkName = databaseFeatures.getColumnName(pkColumnNameList.get(i));
                if (i == n - 1) {
                    builder.append(pkName).append("=?");
                    break;
                }
                builder.append(pkName).append(" =? AND ");
            }
            String deleteDml = builder.toString();
            builder.setLength(0);
            try (LogPreparedStatement preparedStatement = new LogPreparedStatement(connection, deleteDml)) {
                List<ColumnDataDTO> filterColumnDataList = actualDataDTO.getAfter().stream().filter(columnDataDTO -> isExists(pkColumnNameList, columnDataDTO.getName())).collect(Collectors.toList());
                for (int i = 0, n = pkColumnNameList.size(); i < n; i++) {
                    preparedStatement.setObject(i + 1, filterColumnDataList.get(i).getValue());
                }
                log.debug(preparedStatement.toString());
                preparedStatement.execute();
            } catch (Exception e) {
                log.error("任务Id:【{}】执行删除主键sql:{}失败!", taskId, deleteDml, e);
                throw new RuntimeException("执行删除主键sql失败!", e);
            }
        } else {
            log.error("任务Id:【{}】表{}不包含主键或者唯一键!", taskId, actualDataDTO.getSchemaAndTable());
        }
    }

    private boolean isExists(@NotNull List<String> pkColumnNameList, String columnName) {
        for (String pkName : pkColumnNameList) {
            if (StringUtils.equalsIgnoreCase(pkName, StringUtils.replace(columnName, "\"", ""))) {
                return true;
            }
        }
        return false;
    }

    private void addPreparedStatementObject(PreparedStatement preparedStatement, @NotNull ActualDataDTO actualDataDTO) throws SQLException, ParseException, IOException {
        List<ColumnDataDTO> before = actualDataDTO.getBefore();
        List<ColumnDataDTO> after = actualDataDTO.getAfter();
        OperationType operationType = actualDataDTO.getOperation();
        switch (operationType) {
            case INSERT:
                int index = 1;
                addValue(after, index, preparedStatement);
                break;
            case UPDATE:
                index = 1;
                index = addValue(after, index, preparedStatement);
                before.removeIf(entry -> Objects.isNull(entry.getValue()));
                addValue(before, index, preparedStatement);
                break;
            case DELETE:
                index = 1;
                before.removeIf(entry -> Objects.isNull(entry.getValue()));
                addValue(before, index, preparedStatement);
                break;
            default:
                break;
        }
    }

    private int addValue(@NotNull List<ColumnDataDTO> columnDataDTOList, int index, PreparedStatement preparedStatement) throws SQLException {
        for (ColumnDataDTO columnDataDTO : columnDataDTOList) {
            preparedStatement.setObject(index, columnDataDTO.getValue());
            index += 1;
        }
        return index;
    }

    class WriteDataCallable implements Callable<String> {
        private final Integer index;
        private final Integer batchIndex;
        private final Integer keyConflictOperator;
        private final String taskId;
        private final String output;
        private final String dataFileThreshold;
        private final DataSource dataSource;
        private final List<ActualDataDTO> actualDataList;
        private final DatabaseFeatures databaseFeatures;
        private final SinkDataDTO sinkDataDTO;
        private Logger logger;

        public WriteDataCallable(Integer index, int batchIndex, Integer keyConflictOperator, String taskId, String output, String dataFileThreshold, DataSource dataSource, List<ActualDataDTO> actualDataList, DatabaseFeatures databaseFeatures, SinkDataDTO sinkDataDTO) {
            this.index = index;
            this.batchIndex = batchIndex;
            this.keyConflictOperator = keyConflictOperator;
            this.taskId = taskId;
            this.output = output + File.separator + MigrateType.INCREMENT.getCode().toLowerCase();
            this.dataFileThreshold = dataFileThreshold;
            this.dataSource = dataSource;
            this.actualDataList = actualDataList;
            this.databaseFeatures = databaseFeatures;
            this.sinkDataDTO = sinkDataDTO;
        }

        @Override
        public String call() throws Exception {
            Connection connection = null;
            LogPreparedStatement preparedStatement = null;
            ActualDataDTO lastActualData = null;
            log.info("任务Id:【{}】数据文件索引:{},文件内部序号:{},第{}批次数据批量处理!", taskId, index, sinkDataDTO.getSerialNumber(), batchIndex);
            try {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = new LogPreparedStatement(connection, actualDataList.get(0).getScript());
                for (ActualDataDTO actualDataDTO : actualDataList) {
                    lastActualData = actualDataDTO;
                    addPreparedStatementObject(preparedStatement, actualDataDTO);
                    preparedStatement.addBatch();
                }
                log.debug(preparedStatement.toString());
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                connection.commit();
                sendSinkMetrics(lastActualData);
            } catch (SQLException e) {
                if (Objects.nonNull(connection)) {
                    connection.rollback();
                }
                if (!StringUtils.containsIgnoreCase(e.getMessage(), "duplicate key")) {
                    assert preparedStatement != null;
                    log.error("任务Id:【{}】DML:{}执行失败!", taskId, preparedStatement, e);
                    throw new RuntimeException("dml执行失败!", e);
                } else {
                    switch (keyConflictOperator) {
                        case 0:
                            throw new RuntimeException(MessageFormat.format("任务Id:【{0}】数据文件索引:{1},文件内部序号:{2},第{3}批次数据,批量数据处理异常!", taskId, index, sinkDataDTO.getSerialNumber(), batchIndex), e);
                        case 1:
                            log.warn("任务Id:【{}】数据文件索引:{},文件内部序号:{},第{}批次数据批量写入失败,主键冲突策略是:{},从新通过单条数据写入!", taskId, index, sinkDataDTO.getSerialNumber(), batchIndex, "先删除后插入");
                            singleExecute(batchIndex, taskId, actualDataList, dataSource, keyConflictOperator, databaseFeatures);
                            break;
                        case 3:
                            if (Objects.nonNull(preparedStatement)) {
                                String errorFileName = "sink_error_" + taskId + "_" + index + "_" + batchIndex;
                                log.warn("任务Id:【{}】数据文件索引:{},文件内部序号:{},第{}批次数据批量写入失败,主键冲突策略是:【{}】,失败的数据将写入到\"{}/{}{}\"文件中.", taskId, index, sinkDataDTO.getSerialNumber(), batchIndex, "整体忽略", output, errorFileName, Constants.ERROR_SUFFIX);
                                createDataFile(errorFileName);
                                logger.debug(preparedStatement.toString());
                            }
                            break;
                        default:
                            break;
                    }
                }
            } finally {
                if (Objects.nonNull(preparedStatement)) {
                    preparedStatement.close();
                }
                if (Objects.nonNull(connection)) {
                    connection.close();
                }
            }
            return sinkDataDTO.toString();
        }

        private void createDataFile(String fileName) {
            logger = FileConvertHandler.createOrGetHandler(output, fileName, Constants.ERROR_SUFFIX, dataFileThreshold, Boolean.FALSE);
            if (!new File(output + File.separator + fileName + Constants.ERROR_SUFFIX).exists()) {
                createDataFile(fileName);
            }
        }
    }

    private void sendSinkRecord(@NotNull SinkDataDTO sinkDataDTO) {
        DbSinkRecordDTO dbSinkRecordDTO = new DbSinkRecordDTO();
        dbSinkRecordDTO.setTaskId(sinkDataDTO.getTaskId());
        dbSinkRecordDTO.setSinkIndex(sinkDataDTO.getDealIndex());
        dbSinkRecordDTO.setSinkStartScn(String.valueOf(sinkDataDTO.getSerialNumber()));
        dbSinkRecordDTO.setUpdateTime(LocalDateTime.now());
        dataProducer.put(dbSinkRecordDTO);
    }

    private void sendSinkMetrics(ActualDataDTO actualDataDTO) {
        if (Objects.nonNull(actualDataDTO)) {
            DbIncrementMetricsDTO dbIncrementMetricsDTO = new DbIncrementMetricsDTO();
            dbIncrementMetricsDTO.setTaskId(actualDataDTO.getTaskId());
            dbIncrementMetricsDTO.setTargetSchema(actualDataDTO.getSchemaName());
            dbIncrementMetricsDTO.setTargetTable(actualDataDTO.getTableName());
            dbIncrementMetricsDTO.setSinkTime(Timestamp.valueOf(LocalDateTime.now()));
            dbIncrementMetricsDTO.setSinkDelay(dbIncrementMetricsDTO.getSinkTime().getTime() - actualDataDTO.getTransactionalTime());
            dataProducer.put(dbIncrementMetricsDTO);
        }
    }
}
