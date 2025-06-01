package org.dukejasun.migrate.service.capture.structure.impl;

import com.google.common.util.concurrent.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.cache.local.DataTypeMappingCache;
import org.dukejasun.migrate.cache.local.TableMetadataCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.handler.DbMigrateTaskHandler;
import org.dukejasun.migrate.handler.FileConvertHandler;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.DbMigrateResultStatisticsDTO;
import org.dukejasun.migrate.model.dto.input.ParameterDTO;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.AnalyseResultDTO;
import org.dukejasun.migrate.model.dto.output.MigrateResultDTO;
import org.dukejasun.migrate.model.entity.DbMigrateTask;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.analyse.script.AnalyseDatabaseScriptService;
import org.dukejasun.migrate.service.analyse.script.impl.AnalyseOracleScriptService;
import org.dukejasun.migrate.service.analyse.script.impl.AnalyseMySQLScriptService;
import org.dukejasun.migrate.service.capture.structure.StructureCaptureService;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.dukejasun.migrate.utils.FileUtil;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("STRUCTURE_TRANSMISSION_SERVICE")
public class StructureCaptureServiceImpl implements StructureCaptureService {
    private Logger logger;
    private final CommonConfig commonConfig;
    private final MigrateResultsStatisticsService migrateResultsStatisticsService;
    private final DbMigrateTaskHandler dbMigrateTaskHandler;
    private final Map<String, DatabaseFeatures> databaseFeaturesMap;

    @Autowired
    public StructureCaptureServiceImpl(CommonConfig commonConfig, MigrateResultsStatisticsService migrateResultsStatisticsService, DbMigrateTaskHandler dbMigrateTaskHandler, Map<String, DatabaseFeatures> databaseFeaturesMap) {
        this.commonConfig = commonConfig;
        this.migrateResultsStatisticsService = migrateResultsStatisticsService;
        this.dbMigrateTaskHandler = dbMigrateTaskHandler;
        this.databaseFeaturesMap = databaseFeaturesMap;
    }
    @Override
    public void startMigrateTask(String taskId, ParameterDTO parameterDTO, JdbcTemplate jdbcTemplate) throws Exception {
        DbMigrateTask dbMigrateTask = dbMigrateTaskHandler.findMigrateTaskByTaskId(taskId);
        if (Objects.nonNull(dbMigrateTask)) {
            log.info("任务Id:【{}】启动结构迁移......", taskId);
            dbMigrateTask.setStatus(StatusEnum.RUNNING.getCode());
            dbMigrateTaskHandler.saveMigrateTask(dbMigrateTask);

            List<CaptureTableDTO> captureTableDTOList = TableMetadataCache.INSTANCE.get();
            if (!CollectionUtils.isEmpty(captureTableDTOList)) {
                ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(Constants.STRUCTURE_MIGRATE_THREAD_NAME).build();
                ExecutorService executorService = new ThreadPoolExecutor(commonConfig.getCorePoolSize(), commonConfig.getCorePoolSize() * 2, 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
                ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);

                CountDownLatch countDownLatch = new CountDownLatch(captureTableDTOList.size());
                DatasourceDTO dataSourceDTO = parameterDTO.getDataSourceDTO();

                DatasourceDTO targetSourceConfig = parameterDTO.getTargetSourceDTO();
                Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap = DataTypeMappingCache.INSTANCE.getDataTypeMapping(commonConfig.getDataTypeMappingKey());

                Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
                MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
                migrateWorkHandler.setMigrateResultsStatisticsService(migrateResultsStatisticsService);
                DataProducer dataProducer = new DataProducer(disruptor, migrateWorkHandler);
                DatabaseFeatures databaseFeatures = databaseFeaturesMap.get(targetSourceConfig.getType().name() + "_FEATURES");
                DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(targetSourceConfig, commonConfig);
                JdbcTemplate targetJdbcTemplate = new JdbcTemplate(dataSource);
                CaptureTableCache.INSTANCE.get().forEach((schema, value) -> targetJdbcTemplate.execute(MessageFormat.format(Constants.CREATE_SCHEMA_OR_DATABASE_SQL, databaseFeatures.getSchema(schema))));
                String parentDir = commonConfig.getOutputPath() + File.separator + MigrateType.STRUCTURE.getCode().toLowerCase() + File.separator + taskId;
                if (!new File(parentDir).exists()) {
                    FileUtil.createDirectory(Paths.get(parentDir));
                }
                createDataFile(parentDir, LocalDateTime.now());
                commonConfig.setStartTransmissionTime(LocalDateTime.now());
                for (CaptureTableDTO captureTableDTO : captureTableDTOList) {
                    ListenableFuture<String> listenableFuture = listeningExecutorService.submit(new StructureMigrateCallable(taskId, captureTableDTO, dataSourceDTO, targetJdbcTemplate, databaseFeatures, countDownLatch, dataTypeMappingBeanMap));
                    Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                        @Override
                        public void onSuccess(String value) {
                            try {
                                MigrateResultDTO migrateResultDTO = JacksonUtil.fromJson(value, MigrateResultDTO.class);
                                if (Objects.nonNull(migrateResultDTO)) {
                                    sendResult(taskId, migrateResultDTO, captureTableDTO, dataProducer, StatusEnum.FINISH.name());
                                    log.info("任务Id:【{}】{}.{}表结构迁移成功!", taskId, captureTableDTO.getSchema(), captureTableDTO.getName());
                                }
                            } catch (Exception ignored) {
                                MigrateResultDTO migrateResultDTO = new MigrateResultDTO();
                                sendResult(taskId, migrateResultDTO, captureTableDTO, dataProducer, StatusEnum.FINISH.name());
                            }
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            try {
                                MigrateResultDTO migrateResultDTO = JacksonUtil.fromJson(throwable.getMessage(), MigrateResultDTO.class);
                                if (Objects.nonNull(migrateResultDTO)) {
                                    sendResult(taskId, migrateResultDTO, captureTableDTO, dataProducer, StatusEnum.ERROR.name());
                                    log.error("任务Id:【{}】{}.{}表结构迁移失败!异常信息:{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), migrateResultDTO.getErrorReason());
                                }
                            } catch (Exception ignored) {
                                MigrateResultDTO migrateResultDTO = new MigrateResultDTO();
                                migrateResultDTO.setErrorReason(throwable.getMessage());
                                sendResult(taskId, migrateResultDTO, captureTableDTO, dataProducer, StatusEnum.ERROR.name());
                                log.error("任务Id:【{}】{}.{}表结构迁移失败!异常信息:{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), migrateResultDTO.getErrorReason());
                            }
                        }
                    }, executorService);
                }
                countDownLatch.await();
            }
        }
    }

    private void createDataFile(String parentDir, @NotNull LocalDateTime localDateTime) {
        logger = FileConvertHandler.createOrGetHandler(parentDir, commonConfig.getDataTypeMappingKey().toLowerCase() + '_' + localDateTime.format(Constants.DATE_TIME_FORMATTER) + '@', Constants.DATA_SUFFIX, commonConfig.getDataFileThreshold(), Boolean.FALSE);
        if (!new File(parentDir + File.separator + commonConfig.getDataTypeMappingKey().toLowerCase() + '_' + localDateTime.format(Constants.DATE_TIME_FORMATTER) + '@' + Constants.DATA_SUFFIX).exists()) {
            createDataFile(parentDir, localDateTime);
        }
    }

    private void sendResult(String taskId, @NotNull MigrateResultDTO migrateResultDTO, @NotNull CaptureTableDTO captureTableDTO, @NotNull DataProducer dataProducer, String transmissionResult) {
        DbMigrateResultStatisticsDTO dbMigrateResultStatisticsDTO = new DbMigrateResultStatisticsDTO();
        dbMigrateResultStatisticsDTO.setTaskId(taskId);
        dbMigrateResultStatisticsDTO.setTransmissionType(MigrateType.STRUCTURE.getCode());
        dbMigrateResultStatisticsDTO.setErrorReason(migrateResultDTO.getErrorReason());
        dbMigrateResultStatisticsDTO.setSourceSchema(captureTableDTO.getSchema());
        dbMigrateResultStatisticsDTO.setSourceTable(captureTableDTO.getName());
        dbMigrateResultStatisticsDTO.setTargetSchema(captureTableDTO.getTargetSchema());
        dbMigrateResultStatisticsDTO.setTargetTable(captureTableDTO.getTargetName());
        dbMigrateResultStatisticsDTO.setSourceRecord(migrateResultDTO.getSourceRecord());
        dbMigrateResultStatisticsDTO.setTargetRecord(migrateResultDTO.getTargetRecord());
        dbMigrateResultStatisticsDTO.setTransmissionResult(transmissionResult);
        logger.info(dbMigrateResultStatisticsDTO.toString());
        dataProducer.put(dbMigrateResultStatisticsDTO);
    }

    @Override
    public void close() {
    }

    static class StructureMigrateCallable implements Callable<String> {
        private final String taskId;
        private final CaptureTableDTO captureTableDTO;
        private final DatasourceDTO dataSourceDTO;
        private final JdbcTemplate jdbcTemplate;
        private final DatabaseFeatures databaseFeatures;
        private final CountDownLatch countDownLatch;
        private final Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap;

        public StructureMigrateCallable(String taskId, CaptureTableDTO captureTableDTO, DatasourceDTO dataSourceDTO, JdbcTemplate jdbcTemplate, DatabaseFeatures databaseFeatures, CountDownLatch countDownLatch, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) {
            this.taskId = taskId;
            this.captureTableDTO = captureTableDTO;
            this.dataSourceDTO = dataSourceDTO;
            this.jdbcTemplate = jdbcTemplate;
            this.databaseFeatures = databaseFeatures;
            this.countDownLatch = countDownLatch;
            this.dataTypeMappingBeanMap = dataTypeMappingBeanMap;
        }

        @Override
        public String call() {
            MigrateResultDTO migrateResultDTO = new MigrateResultDTO();
            try {
                if (!CollectionUtils.isEmpty(captureTableDTO.getColumnMetaList())) {
                    log.info("任务Id:【{}】将{}.{}表结构转换成{}数据库格式......", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), databaseFeatures.databaseType().name());
                    AnalyseResultDTO analyseResultDTO = analyseDatabaseScriptService(dataSourceDTO.getType()).analyseTableAndConvert(captureTableDTO, dataSourceDTO, databaseFeatures, dataTypeMappingBeanMap);
                    if (Objects.nonNull(analyseResultDTO)) {
                        migrateResultDTO.setSourceRecord(analyseResultDTO.getOriginScript());
                        migrateResultDTO.setTargetRecord(analyseResultDTO.toString());
                        String[] scripts = StringUtils.split(analyseResultDTO.getConvertScript(), ';');
                        for (String script : scripts) {
                            if (StringUtils.isNotBlank(script)) {
                                jdbcTemplate.execute(script);
                            }
                        }
                        if (StringUtils.isNotBlank(analyseResultDTO.getFkScript())) {
                            log.warn("任务Id:【{}】{}.{}表包含外键,迁移工具暂不支持!DDL:{}", taskId, captureTableDTO.getSchema(), captureTableDTO.getName(), analyseResultDTO.getFkScript());
                        }
                    }
                } else {
                    migrateResultDTO.setErrorReason(MessageFormat.format("任务Id:【{0}】表结构不完整,缺少列信息!", taskId));
                    throw new RuntimeException(migrateResultDTO.toString());
                }
            } catch (Exception e) {
                migrateResultDTO.setErrorReason(e.getMessage());
                throw new RuntimeException(migrateResultDTO.toString());
            } finally {
                captureTableDTO.setEndTime(LocalDateTime.now());
                captureTableDTO.getLongAdder().increment();
                countDownLatch.countDown();
            }
            return migrateResultDTO.toString();
        }

        private @NotNull AnalyseDatabaseScriptService analyseDatabaseScriptService(@NotNull DatabaseExpandType databaseType) {
            switch (databaseType) {
                case MYSQL:
                    return new AnalyseMySQLScriptService();
                case ORACLE:
                default:
                    return new AnalyseOracleScriptService();
            }
        }
    }
}
