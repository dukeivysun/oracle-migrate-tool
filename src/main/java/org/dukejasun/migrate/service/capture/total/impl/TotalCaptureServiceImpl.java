package org.dukejasun.migrate.service.capture.total.impl;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.cache.local.TableMetadataCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.handler.DbMigrateResultStatisticsHandler;
import org.dukejasun.migrate.handler.DbMigrateTaskHandler;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.model.dto.event.DbMigrateResultStatisticsDTO;
import org.dukejasun.migrate.model.dto.input.ParameterDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.MigrateResultDTO;
import org.dukejasun.migrate.model.entity.DbMigrateTask;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.capture.total.TotalCaptureService;
import org.dukejasun.migrate.service.compare.total.TotalDataCompareImpl;
import org.dukejasun.migrate.service.sink.DataSinkService;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.dukejasun.migrate.utils.FileUtil;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.driver.OracleConnection;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("TOTAL_TRANSMISSION_SERVICE")
public class TotalCaptureServiceImpl implements TotalCaptureService {
    private final CommonConfig commonConfig;
    private final DataProducer dataProducer;
    private final DbMigrateTaskHandler dbMigrateTaskHandler;
    private final DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler;
    private final ExecutorService executorIncrementService;
    private final Map<String, DatabaseFeatures> databaseFeaturesMap;
    private final Map<String, DataSinkService> receiveAndSinkDataMap;

    @Autowired
    public TotalCaptureServiceImpl(@NotNull CommonConfig commonConfig, DbMigrateTaskHandler dbMigrateTaskHandler, DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler, MigrateResultsStatisticsService migrateResultsStatisticsService, Map<String, DatabaseFeatures> databaseFeaturesMap, Map<String, DataSinkService> receiveAndSinkDataMap) {
        this.commonConfig = commonConfig;
        this.dbMigrateTaskHandler = dbMigrateTaskHandler;
        this.dbMigrateResultStatisticsHandler = dbMigrateResultStatisticsHandler;
        this.databaseFeaturesMap = databaseFeaturesMap;
        this.receiveAndSinkDataMap = receiveAndSinkDataMap;

        Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        migrateWorkHandler.setMigrateResultsStatisticsService(migrateResultsStatisticsService);
        this.dataProducer = new DataProducer(disruptor, migrateWorkHandler);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(Constants.TOTAL_DATA_COMPARE_THREAD_NAME).build();
        executorIncrementService = new ThreadPoolExecutor(4, 4, 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536 * 2), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void startMigrateTask(String taskId, @NotNull ParameterDTO parameterDTO, JdbcTemplate jdbcTemplate) throws Exception {
        DatabaseFeatures databaseFeatures = databaseFeaturesMap.get(parameterDTO.getDataSourceDTO().getType().name() + "_FEATURES");
        DatabaseFeatures targetDatabaseFeatures = databaseFeaturesMap.get(parameterDTO.getTargetSourceDTO().getType().name() + "_FEATURES");
        if (commonConfig.getTotalDataToFile()) {
            switch (commonConfig.getReplicateType()) {
                case 0:
                    generatorConsumer(taskId, parameterDTO.getTargetSourceDTO(), targetDatabaseFeatures);
                    generatorProducer(taskId, parameterDTO, jdbcTemplate, databaseFeatures, targetDatabaseFeatures);
                    break;
                case 1:
                    generatorProducer(taskId, parameterDTO, jdbcTemplate, databaseFeatures, targetDatabaseFeatures);
                    break;
                case 2:
                    generatorConsumer(taskId, parameterDTO.getTargetSourceDTO(), targetDatabaseFeatures);
                    consumerCounter(taskId, databaseFeatures, targetDatabaseFeatures, jdbcTemplate, new JdbcTemplate(DataSourceFactory.INSTANCE.getDataSource(parameterDTO.getTargetSourceDTO(), commonConfig)));
                    break;
                default:
                    break;
            }
        } else {
            migrateTotalWithMemory(taskId, parameterDTO, jdbcTemplate, databaseFeatures, targetDatabaseFeatures);
        }
    }

    private void migrateTotalWithMemory(String taskId, ParameterDTO parameterDTO, JdbcTemplate jdbcTemplate, DatabaseFeatures databaseFeatures, DatabaseFeatures targetDatabaseFeatures) throws SQLException, InterruptedException {
        generatorProducer(taskId, parameterDTO, jdbcTemplate, databaseFeatures, targetDatabaseFeatures);
    }

    private void consumerCounter(String taskId, DatabaseFeatures databaseFeatures, DatabaseFeatures targetDatabaseFeatures, JdbcTemplate sourceJdbcTemplate, JdbcTemplate targetJdbcTemplate) {
        ExecutorService singleThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), new ThreadFactoryBuilder().setNameFormat(Constants.COMPARE_THREAD_NAME).build(), new ThreadPoolExecutor.AbortPolicy());
        singleThreadPool.execute(() -> CaptureTableCache.INSTANCE.get().forEach((schema, captureTableDTOList) -> captureTableDTOList.forEach(captureTableDTO -> {
            boolean flag = captureTableDTO.getCaptureLongAddr().longValue() != 0 && captureTableDTO.getSinkLongAddr().longValue() != 0 && captureTableDTO.getCaptureLongAddr().longValue() == captureTableDTO.getSinkLongAddr().longValue();
            while (!flag) {
                try {
                    TimeUnit.MILLISECONDS.sleep(300L);
                } catch (InterruptedException ignored) {
                }
                flag = captureTableDTO.getCaptureLongAddr().longValue() != 0 && captureTableDTO.getSinkLongAddr().longValue() != 0 && captureTableDTO.getCaptureLongAddr().longValue() == captureTableDTO.getSinkLongAddr().longValue();
            }
            if (Objects.nonNull(commonConfig.getSupportDataCompare()) && commonConfig.getSupportDataCompare()) {
                executorIncrementService.submit(new TotalDataCompareImpl(taskId, captureTableDTO, databaseFeatures, targetDatabaseFeatures, sourceJdbcTemplate, targetJdbcTemplate));
            } else {
                try {
                    TimeUnit.MILLISECONDS.sleep(300L);
                } catch (InterruptedException ignored) {
                }
                captureTableDTO.setEndTime(LocalDateTime.now());
                captureTableDTO.getLongAdder().increment();
            }
        })));
        singleThreadPool.shutdown();
    }

    private void generatorConsumer(String taskId, DatasourceDTO targetDatasourceDTO, DatabaseFeatures targetDatabaseFeatures) throws Exception {
        log.info("启动全量数据消费逻辑......");
        if (commonConfig.getTotalMigrateTruncateTargetTable()) {
            log.info("清除目标表原有数据......");
            targetDatabaseFeatures.truncateData(taskId, TableMetadataCache.INSTANCE.get(), targetDatasourceDTO, commonConfig, dbMigrateResultStatisticsHandler);
        }
        Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        migrateWorkHandler.setDataSinkService(receiveAndSinkDataMap.get(MigrateType.TOTAL.getCode() + "_RECEIVE_SINK_DATA"));
        DataProducer producer = new DataProducer(disruptor, migrateWorkHandler);
        String parentDir = commonConfig.getOutputPath() + File.separator + MigrateType.TOTAL.getCode().toLowerCase() + File.separator + taskId;
        File outputFile = new File(parentDir);
        commonConfig.setStartTransmissionTime(LocalDateTime.now());
        if (outputFile.exists()) {
            log.debug("在{}目录下获取采集文件列表......", outputFile.getAbsolutePath());
            collectionFileFromDirectory(taskId, outputFile, targetDatasourceDTO, producer);
        }
        monitorDirectory(taskId, parentDir, producer, targetDatasourceDTO);
    }

    private void monitorDirectory(String taskId, String outputPath, DataProducer producer, DatasourceDTO targetDatasourceDTO) throws Exception {
        FileAlterationObserver fileAlterationObserver = new FileAlterationObserver(new File(outputPath), FileFilterUtils.or(FileFilterUtils.and(FileFilterUtils.directoryFileFilter(), HiddenFileFilter.VISIBLE), FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter(Constants.CSV_SUFFIX))));
        fileAlterationObserver.addListener(new FileAlterationListenerAdaptor() {
            @Override
            public void onFileCreate(final File file) {
                generatorDataLogFileDTO(taskId, file, targetDatasourceDTO, producer);
            }
        });
        FileAlterationMonitor fileAlterationMonitor = new FileAlterationMonitor(1000L, fileAlterationObserver);
        fileAlterationMonitor.start();
    }

    private void collectionFileFromDirectory(String taskId, File outputFile, DatasourceDTO targetDatasourceDTO, DataProducer producer) {
        Files.fileTreeTraverser().preOrderTraversal(outputFile).filter(input -> StringUtils.equalsIgnoreCase("csv", Files.getFileExtension(input.getName()))).forEach(file -> generatorDataLogFileDTO(taskId, file, targetDatasourceDTO, producer));
    }

    private void generatorDataLogFileDTO(String taskId, @NotNull File file, DatasourceDTO targetDatasourceDTO, @NotNull DataProducer producer) {
        DataLogFileDTO dataLogFileDTO = new DataLogFileDTO();
        dataLogFileDTO.setTaskId(taskId);
        dataLogFileDTO.setIndex("0");
        dataLogFileDTO.setDataLogPath(file.getAbsolutePath());
        String fileName = file.getName();
        String key = fileName.substring(0, fileName.indexOf("@"));
        CaptureTableDTO captureTableDTO = CaptureTableCache.INSTANCE.get(key);
        if (Objects.nonNull(captureTableDTO)) {
            captureTableDTO.getCaptureLongAddr().increment();
        }
        dataLogFileDTO.setKey(key);
        dataLogFileDTO.setEmpty(file.length() == 0);
        dataLogFileDTO.setDatasourceDTO(targetDatasourceDTO);
        producer.put(dataLogFileDTO);
    }

    private void generatorProducer(String taskId, ParameterDTO parameterDTO, JdbcTemplate jdbcTemplate, DatabaseFeatures databaseFeatures, DatabaseFeatures targetDatabaseFeatures) throws SQLException, InterruptedException {
        DbMigrateTask dbMigrateTask = dbMigrateTaskHandler.findMigrateTaskByTaskId(taskId);
        if (Objects.nonNull(dbMigrateTask)) {
            if (commonConfig.getTotalDataToFile()) {
                log.info("启动全量数据迁移逻辑......");
            }
            if (commonConfig.insertModel()) {
                commonConfig.setDataDelimiter(",");
            }
            dbMigrateTask.setStatus(StatusEnum.RUNNING.getCode());
            saveCurrentScn(dbMigrateTask, jdbcTemplate, parameterDTO.getDataSourceDTO());
            List<CaptureTableDTO> captureTableDTOList = TableMetadataCache.INSTANCE.get();
            if (!CollectionUtils.isEmpty(captureTableDTOList)) {
                OracleConnection oracleConnection = null;
                if (Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection().isWrapperFor(OracleConnection.class)) {
                    oracleConnection = jdbcTemplate.getDataSource().getConnection().unwrap(OracleConnection.class);
                    oracleConnection.setRemarksReporting(true);
                }
                ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(Constants.TOTAL_MIGRATE_THREAD_NAME).build();
                CountDownLatch countDownLatch = new CountDownLatch(captureTableDTOList.size());
                DatasourceDTO targetSourceConfig = parameterDTO.getTargetSourceDTO();

                ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(commonConfig.getCorePoolSize(), commonConfig.getCorePoolSize() * 2, 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
                String parentDir = null;
                if (Objects.nonNull(commonConfig.getTotalDataToFile()) && commonConfig.getTotalDataToFile()) {
                    parentDir = commonConfig.getOutputPath() + File.separator + MigrateType.TOTAL.getCode().toLowerCase() + File.separator + taskId;
                    File file = new File(parentDir);
                    if (!file.exists()) {
                        FileUtil.createDirectory(Paths.get(parentDir));
                    }
                }
                ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
                commonConfig.setStartTransmissionTime(LocalDateTime.now());
                for (CaptureTableDTO captureTableDTO : captureTableDTOList) {
                    captureTableDTO.setStartTime(LocalDateTime.now());
                    ListenableFuture<String> listenableFuture = listeningExecutorService.submit(new TotalMigrateCallable(taskId, jdbcTemplate, oracleConnection, commonConfig, captureTableDTO, targetSourceConfig, countDownLatch, databaseFeatures, targetDatabaseFeatures, parentDir, dataProducer, dbMigrateResultStatisticsHandler));
                    Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                        @Override
                        public void onSuccess(String value) {
                            log.info("任务Id:【{}】完成{}.{}表的迁移!", taskId, captureTableDTO.getSchema(), captureTableDTO.getName());
                        }
                        @Override
                        public void onFailure(Throwable throwable) {
                            try {
                                MigrateResultDTO migrateResultDTO = JacksonUtil.fromJson(throwable.getMessage(), MigrateResultDTO.class);
                                if (Objects.nonNull(migrateResultDTO)) {
                                    log.error("任务Id:【{}】{}.{}表{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), migrateResultDTO.getErrorReason());
                                    sendErrorResult(taskId, captureTableDTO, migrateResultDTO);
                                } else {
                                    migrateResultDTO = new MigrateResultDTO();
                                    migrateResultDTO.setErrorReason(throwable.getMessage());
                                    log.error("任务Id:【{}】{}.{}表{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), throwable.getMessage());
                                    sendErrorResult(taskId, captureTableDTO, migrateResultDTO);
                                }
                            } catch (Exception e) {
                                MigrateResultDTO migrateResultDTO = new MigrateResultDTO();
                                migrateResultDTO.setErrorReason(throwable.getMessage());
                                log.error("任务Id:【{}】{}.{}表{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), throwable.getMessage());
                                sendErrorResult(taskId, captureTableDTO, migrateResultDTO);
                            }
                        }
                    }, threadPoolExecutor);
                }
                countDownLatch.await();
            }
        }
    }

    private void sendErrorResult(String taskId, @NotNull CaptureTableDTO captureTableDTO, @NotNull MigrateResultDTO migrateResultDTO) {
        DbMigrateResultStatisticsDTO dbMigrateResultStatisticsDTO = new DbMigrateResultStatisticsDTO();
        dbMigrateResultStatisticsDTO.setTaskId(taskId);
        dbMigrateResultStatisticsDTO.setTransmissionType(MigrateType.TOTAL.getCode());
        dbMigrateResultStatisticsDTO.setSourceSchema(captureTableDTO.getSchema());
        dbMigrateResultStatisticsDTO.setSourceTable(captureTableDTO.getName());
        dbMigrateResultStatisticsDTO.setTargetSchema(captureTableDTO.getTargetSchema());
        dbMigrateResultStatisticsDTO.setTargetTable(captureTableDTO.getTargetName());
        dbMigrateResultStatisticsDTO.setSourceRecord(migrateResultDTO.getSourceRecord());
        dbMigrateResultStatisticsDTO.setTargetRecord(migrateResultDTO.getTargetRecord());
        dbMigrateResultStatisticsDTO.setTransmissionResult(StatusEnum.ERROR.getCode());
        dbMigrateResultStatisticsDTO.setErrorReason(migrateResultDTO.getErrorReason());
        dataProducer.put(dbMigrateResultStatisticsDTO);
    }

    private void saveCurrentScn(@NotNull DbMigrateTask dbMigrateTask, JdbcTemplate jdbcTemplate, DatasourceDTO datasourceDTO) {
        String extra = dbMigrateTask.getExtra();
        Map<String, Object> map = null;
        if (StringUtils.isNotBlank(extra) && !Constants.NULL_VALUE.equalsIgnoreCase(extra)) {
            map = Objects.requireNonNull(JacksonUtil.fromJson(extra, Map.class));
        }
        AtomicLong currentScn = new AtomicLong();
        if (CollectionUtils.isEmpty(datasourceDTO.getClusterList())) {
            jdbcTemplate.query(IConstants.CURRENT_SCN, resultSet -> {
                do {
                    currentScn.set(Long.parseLong(resultSet.getString(1)));
                } while (resultSet.next());
            });
        } else {
            jdbcTemplate.query(Constants.OracleConstants.QUERY_CURRENT_SCN_WITH_RAC, resultSet -> {
                if (resultSet.next()) {
                    do {
                        currentScn.set(Long.parseLong(resultSet.getString(1)));
                    } while (resultSet.next());
                }
            });
        }
        if (currentScn.longValue() != 0) {
            if (CollectionUtils.isEmpty(map)) {
                map = Maps.newHashMap();
            }
            map.put("currentScn", String.valueOf(currentScn.longValue()));
        }
        if (!CollectionUtils.isEmpty(map)) {
            dbMigrateTask.setExtra(JacksonUtil.toJson(map));
        }
        dbMigrateTaskHandler.saveMigrateTask(dbMigrateTask);
    }

    @Override
    public void close() {
        if (Objects.nonNull(executorIncrementService)) {
            executorIncrementService.shutdown();
        }
    }
}
