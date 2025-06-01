package org.dukejasun.migrate.service.capture.increment.capture.impl;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.lmax.disruptor.dsl.Disruptor;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.LogType;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.handler.DbIncrementMetricsHandler;
import org.dukejasun.migrate.handler.DbMigrateTaskHandler;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.model.dto.event.DbIncrementMetricsDTO;
import org.dukejasun.migrate.model.dto.input.CurrentScnInfoDTO;
import org.dukejasun.migrate.model.dto.input.ParameterDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.entity.DbMigrateTask;
import org.dukejasun.migrate.model.vo.LogFile;
import org.dukejasun.migrate.model.vo.Scn;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.capture.increment.capture.CaptureIncrementData;
import org.dukejasun.migrate.service.capture.increment.capture.IncrementCaptureService;
import org.dukejasun.migrate.service.capture.increment.strategy.LogMiningStrategy;
import org.dukejasun.migrate.service.capture.increment.strategy.impl.RacNodesLogMiningStrategy;
import org.dukejasun.migrate.service.capture.increment.strategy.impl.SingleNodeLogMiningStrategy;
import org.dukejasun.migrate.service.sink.DataSinkService;
import org.dukejasun.migrate.utils.FileUtil;
import org.dukejasun.migrate.utils.OutputLogWithTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("INCREMENT_TRANSMISSION_SERVICE")
public class IncrementCaptureServiceImpl implements IncrementCaptureService {
    private final CommonConfig commonConfig;
    private final DbMigrateTaskHandler dbMigrateTaskHandler;
    private final DbIncrementMetricsHandler dbIncrementMetricsHandler;
    private final Map<String, CaptureIncrementData> captureIncrementDataMap;
    private final Map<String, DataSinkService> receiveAndSinkDataMap;
    private final AtomicReference<String> atomicReference = new AtomicReference<>();

    @Autowired
    public IncrementCaptureServiceImpl(CommonConfig commonConfig, Map<String, CaptureIncrementData> captureIncrementDataMap, Map<String, DataSinkService> receiveAndSinkDataMap, DbMigrateTaskHandler dbMigrateTaskHandler, DbIncrementMetricsHandler dbIncrementMetricsHandler) {
        this.commonConfig = commonConfig;
        this.captureIncrementDataMap = captureIncrementDataMap;
        this.receiveAndSinkDataMap = receiveAndSinkDataMap;
        this.dbMigrateTaskHandler = dbMigrateTaskHandler;
        this.dbIncrementMetricsHandler = dbIncrementMetricsHandler;
    }

    @Override
    public void startMigrateTask(String taskId, ParameterDTO parameterDTO, JdbcTemplate jdbcTemplate) throws Exception {
        DbMigrateTask dbMigrateTask = dbMigrateTaskHandler.findMigrateTaskByTaskId(taskId);
        if (Objects.nonNull(dbMigrateTask)) {
            dbMigrateTask.setStatus(StatusEnum.RUNNING.getCode());
            dbMigrateTaskHandler.saveMigrateTask(dbMigrateTask);
            DatasourceDTO dataSourceDTO = parameterDTO.getDataSourceDTO();
            try {
                StringBuilder builderSchema = new StringBuilder();
                StringBuilder builderTable = new StringBuilder();
                CaptureTableCache.INSTANCE.get().forEach((schema, captureTableList) -> {
                    builderSchema.append('\'').append(schema.toUpperCase()).append('\'');
                    for (Iterator<CaptureTableDTO> iterator = captureTableList.iterator(); iterator.hasNext(); ) {
                        CaptureTableDTO captureTableDTO = iterator.next();
                        builderTable.append('\'').append(captureTableDTO.getName().toUpperCase()).append('\'');
                        if (iterator.hasNext()) {
                            builderTable.append(',');
                        }
                    }
                    builderSchema.append(',');
                });
                if (builderSchema.length() > 0) {
                    builderSchema.deleteCharAt(builderSchema.length() - 1);
                }
                atomicReference.set(taskId);
                switch (commonConfig.getReplicateType()) {
                    case 0:
                        generatorConsumer(taskId, parameterDTO.getTargetSourceDTO());
                        generatorProducer(taskId, parameterDTO, jdbcTemplate, dataSourceDTO, builderSchema.toString(), builderTable.toString());
                        break;
                    case 1:
                        generatorProducer(taskId, parameterDTO, jdbcTemplate, dataSourceDTO, builderSchema.toString(), builderTable.toString());
                        break;
                    case 2:
                        generatorConsumer(taskId, parameterDTO.getTargetSourceDTO());
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private void generatorConsumer(String taskId, DatasourceDTO datasourceDTO) throws Exception {
        log.info("启动增量数据消费逻辑......");
        Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        migrateWorkHandler.setDataSinkService(receiveAndSinkDataMap.get(MigrateType.INCREMENT.getCode() + "_RECEIVE_SINK_DATA"));
        DataProducer producer = new DataProducer(disruptor, migrateWorkHandler);
        String parentDir = commonConfig.getOutputPath() + File.separator + MigrateType.INCREMENT.getCode().toLowerCase() + File.separator + taskId;
        File outputFile = new File(parentDir);
        commonConfig.setStartTransmissionTime(LocalDateTime.now());
        if (outputFile.exists()) {
            if (commonConfig.getReplicateType() == 2) {
                collectionFileFromDirectory(taskId, outputFile, datasourceDTO, producer);
            }
        }
        monitorDirectory(taskId, parentDir, producer, datasourceDTO);
    }

    private void collectionFileFromDirectory(String taskId, File outputFile, DatasourceDTO datasourceDTO, DataProducer producer) {
        FluentIterable<File> fileFluentIterable = Files.fileTreeTraverser().preOrderTraversal(outputFile).filter(input -> StringUtils.equalsIgnoreCase("txt", Files.getFileExtension(input.getName())));
        fileFluentIterable.forEach(file -> generatorDataLogFileDTO(taskId, file, datasourceDTO, producer));
    }

    private void generatorProducer(String taskId, ParameterDTO parameterDTO, JdbcTemplate jdbcTemplate, DatasourceDTO dataSourceDTO, String schemas, String tables) throws Exception {
        CurrentScnInfoDTO currentScnInfoDTO = getLogInfoDTO(jdbcTemplate);
        if (Objects.nonNull(currentScnInfoDTO.getMinScn()) && parameterDTO.getStartScn().compareTo(currentScnInfoDTO.getMinScn().longValue()) >= 0) {
            log.info("启动数据采集逻辑......");
            AtomicLong atomicLong = new AtomicLong();
            AtomicLong beginEndScn = new AtomicLong();
            AtomicReference<List<LogFile>> atomicReference = new AtomicReference<>();
            atomicReference.set(Lists.newArrayList());
            atomicLong.set(parameterDTO.getStartScn());
            Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
            MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
            migrateWorkHandler.setCaptureIncrementData(dataSourceDTO.isCluster() ? captureIncrementDataMap.get("CLUSTER_CAPTURE_INCREMENT_DATA") : captureIncrementDataMap.get("SINGLE_CAPTURE_INCREMENT_DATA"));
            try (DataProducer scnProducer = new DataProducer(disruptor, migrateWorkHandler)) {
                createDataDictionary(parameterDTO, parameterDTO.getDataSourceDTO());
                Map<String, Object> extra = parameterDTO.getExtra();
                String parentDir = commonConfig.getOutputPath() + File.separator + MigrateType.INCREMENT.getCode().toLowerCase() + File.separator + taskId;
                if (!new File(parentDir).exists()) {
                    FileUtil.createDirectory(Paths.get(parentDir));
                }
                int index = 0;
                if (!CollectionUtils.isEmpty(extra)) {
                    index = Objects.nonNull(extra.get("index")) ? (Integer) extra.get("index") : 0;
                    beginEndScn.set(Long.parseLong(StringUtils.isNotBlank((String) extra.get("captureEndScn")) ? (String) extra.get("captureEndScn") : "0"));
                }
                commonConfig.setIncrementCaptureNumber(index);
                deleteExistsFile(index, parentDir);
                boolean isEmpty = scnProducer.isEmpty();
                commonConfig.setStartTransmissionTime(LocalDateTime.now());
                while (isEmpty) {
                    BigInteger endScn = BigInteger.valueOf(atomicLong.longValue() + parameterDTO.getBatch());
                    if (beginEndScn.longValue() > 0) {
                        endScn = BigInteger.valueOf(beginEndScn.longValue());
                        beginEndScn.set(0L);
                    }
                    if (endScn.longValue() > currentScnInfoDTO.getCurrentScn().longValue()) {
                        TimeUnit.MILLISECONDS.sleep(1000L);
                    }
                    List<LogFile> logFileList = getLogFileList(jdbcTemplate, BigInteger.valueOf(atomicLong.longValue()).longValue(), endScn.longValue());
                    if (!CollectionUtils.isEmpty(logFileList)) {
                        List<LogFile> previousList = atomicReference.get();
                        if (!CollectionUtils.isEmpty(previousList) && previousList.size() > 1) {
                            previousList.forEach(redoLog -> logFileList.removeIf(logFile -> !logFile.isCurrent() && logFile.getSequence().equals(redoLog.getSequence())));
                        }
                        if (!CollectionUtils.isEmpty(logFileList)) {
                            CaptureConditionDTO captureConditionDTO = new CaptureConditionDTO();
                            captureConditionDTO.setTaskId(taskId);
                            captureConditionDTO.setIndex(index);
                            LogFile firstLogFile = logFileList.get(0);
                            LogFile lastLogFile = logFileList.get(logFileList.size() - 1);
                            if (index == 0) {
                                captureConditionDTO.setStartScn(BigInteger.valueOf(atomicLong.longValue()));
                            } else {
                                if (lastLogFile.getNextScn().longValue() < Constants.MAX_VALUE.longValue()) {
                                    if (firstLogFile.getFirstScn().longValue() - atomicLong.get() < 0) {
                                        captureConditionDTO.setStartScn(BigInteger.valueOf(atomicLong.longValue()));
                                    } else {
                                        captureConditionDTO.setStartScn(BigInteger.valueOf(firstLogFile.getFirstScn().longValue()));
                                    }
                                } else {
                                    captureConditionDTO.setStartScn(BigInteger.valueOf(atomicLong.longValue()));
                                }
                            }
                            if (endScn.longValue() < lastLogFile.getNextScn().longValue()) {
                                AtomicLong currentEndScn = new AtomicLong(lastLogFile.getNextScn().longValue());
                                if (lastLogFile.getNextScn().longValue() == Constants.MAX_VALUE.longValue()) {
                                    setCurrentScn(jdbcTemplate, currentEndScn, dataSourceDTO);
                                    while (currentEndScn.longValue() - captureConditionDTO.getStartScn().longValue() < 256L) {
                                        TimeUnit.MILLISECONDS.sleep(2500L);
                                        setCurrentScn(jdbcTemplate, currentEndScn, dataSourceDTO);
                                    }
                                }
                                captureConditionDTO.setEndScn(BigInteger.valueOf(currentEndScn.longValue()));
                            } else {
                                captureConditionDTO.setEndScn(endScn);
                            }
                            captureConditionDTO.setDictionaryPath(parameterDTO.getDictionaryPath());
                            captureConditionDTO.setDictionaryLocation(parameterDTO.getDictionaryLocation());
                            captureConditionDTO.setPdbName(parameterDTO.getPdbName());
                            captureConditionDTO.setDataSourceDTO(dataSourceDTO);
                            if (captureConditionDTO.getEndScn().longValue() < Constants.MAX_VALUE.longValue()) {
                                atomicLong.set(captureConditionDTO.getEndScn().longValue());
                            } else {
                                atomicLong.set(captureConditionDTO.getStartScn().longValue() + parameterDTO.getBatch());
                            }
                            captureConditionDTO.setLogFiles(logFileList);
                            atomicReference.set(logFileList);
                            captureConditionDTO.setSchemas(schemas);
                            captureConditionDTO.setTables(tables);
                            captureConditionDTO.setParentDir(parentDir);
                            if (dataSourceDTO.isCluster()) {
                                captureConditionDTO.setType(Constants.RAC_CONDITION_TYPE);
                            }
                            scnProducer.put(captureConditionDTO);
                            index += 1;
                        } else {
                            atomicLong.set(endScn.longValue());
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(300L);
                    isEmpty = !scnProducer.isFull();
                    try (LogMiningStrategy logMiningStrategy = resolveFlushStrategy(dataSourceDTO)) {
                        if (Objects.nonNull(logMiningStrategy)) {
                            logMiningStrategy.flush();
                        }
                    }
                }
            }
        } else {
            throw new RuntimeException(MessageFormat.format("当前最小scn:【{0}】大于输入scn:【{1}】,不能进行数据挖掘!", currentScnInfoDTO.getMinScn().toString(), String.valueOf(parameterDTO.getStartScn())));
        }
    }

    private void deleteExistsFile(int captureIndex, String parentDir) throws IOException {
        FluentIterable<File> fileList = Files.fileTreeTraverser().preOrderTraversal(new File(parentDir)).filter(input -> input.isFile() && Integer.parseInt(input.getName().substring(input.getName().lastIndexOf(commonConfig.getIncrementDataFilePrefix()) + commonConfig.getIncrementDataFilePrefix().length(), input.getName().indexOf('@'))) >= captureIndex);
        for (File file : fileList) {
            java.nio.file.Files.deleteIfExists(file.toPath());
        }
    }

    private void setCurrentScn(JdbcTemplate jdbcTemplate, AtomicLong currentEndScn, DatasourceDTO datasourceDTO) {
        try {
            if (!datasourceDTO.isCluster()) {
                jdbcTemplate.query(IConstants.CURRENT_SCN, resultSet -> {
                    do {
                        currentEndScn.set(Long.parseLong(resultSet.getString(1)));
                    } while (resultSet.next());
                });
            } else {
                jdbcTemplate.query(Constants.OracleConstants.QUERY_CURRENT_SCN_WITH_RAC, resultSet -> {
                    if (resultSet.next()) {
                        do {
                            currentEndScn.set(Long.parseLong(resultSet.getString(1)));
                        } while (resultSet.next());
                    }
                });
            }
        } catch (Exception e) {
            try {
                TimeUnit.MILLISECONDS.sleep(300L);
            } catch (InterruptedException ignored) {
            }
            setCurrentScn(jdbcTemplate, currentEndScn, datasourceDTO);
        }
    }

    private void createDataDictionary(ParameterDTO parameterDTO, DatasourceDTO datasourceDTO) {
        try (LogMiningStrategy logMiningStrategy = resolveFlushStrategy(datasourceDTO)) {
            if (Objects.nonNull(logMiningStrategy)) {
                logMiningStrategy.createDataDictionary(parameterDTO);
            }
        } catch (Exception e) {
            throw new RuntimeException("数据字典穿件失败!");
        }
    }

    private List<LogFile> getLogFileList(JdbcTemplate jdbcTemplate, long startScn, long endScn) {
        try {
            final LinkedList<LogFile> logFileList = Lists.newLinkedList();
            final LinkedList<LogFile> onlineLogFileList = Lists.newLinkedList();
            final LinkedList<LogFile> archivedLogFileList = Lists.newLinkedList();

            jdbcTemplate.query(IConstants.QUERY_DATA_LOGS, resultSet -> {
                do {
                    String fileName = resultSet.getString(1);
                    long firstScn = Long.parseLong(resultSet.getString(2));
                    long nextScn = getNextScn(resultSet.getString(3));
                    String status = resultSet.getString(5);
                    String type = resultSet.getString(6);
                    Long sequence = resultSet.getLong(7);
                    if (StringUtils.equalsIgnoreCase(IConstants.ARCHIVED, type)) {
                        if (nextScn >= startScn) {
                            archivedLogFileList.add(new LogFile(fileName, firstScn, nextScn, sequence, LogType.ARCHIVE));
                        }
                    } else if (StringUtils.equalsIgnoreCase(IConstants.ONLINE, type)) {
                        if (StringUtils.equalsIgnoreCase("CURRENT", status) && firstScn <= endScn) {
                            onlineLogFileList.add(new LogFile(fileName, Scn.valueOf(firstScn), Scn.valueOf(Long.MAX_VALUE), sequence, LogType.REDO, true));
                        }
                    }
                } while (resultSet.next());
            }, startScn, endScn);
            onlineLogFileList.forEach(redoLog -> archivedLogFileList.removeIf(archivedLogFile -> archivedLogFile.getSequence().equals(redoLog.getSequence())));
            if (!CollectionUtils.isEmpty(archivedLogFileList)) {
                logFileList.addAll(archivedLogFileList);
            }
            if (!CollectionUtils.isEmpty(onlineLogFileList)) {
                logFileList.addAll(onlineLogFileList);
            }
            return logFileList;
        } catch (Exception e) {
            try {
                TimeUnit.MILLISECONDS.sleep(300L);
            } catch (InterruptedException ignored) {
            }
            return getLogFileList(jdbcTemplate, startScn, endScn);
        }
    }

    private long getNextScn(String scn) {
        try {
            return Long.parseLong(scn);
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    private CurrentScnInfoDTO getLogInfoDTO(@NotNull JdbcTemplate jdbcTemplate) {
        CurrentScnInfoDTO logInfo = jdbcTemplate.query(IConstants.MIN_AND_MAX_SCN, resultSet -> {
            if (resultSet.next()) {
                do {
                    String minScn = resultSet.getString(1);
                    String archivedLogFileNumber = resultSet.getString(2);
                    String maxScn = resultSet.getString(3);
                    CurrentScnInfoDTO currentScnInfoDTO = new CurrentScnInfoDTO();
                    currentScnInfoDTO.setMinScn(new BigInteger(minScn));
                    currentScnInfoDTO.setMaxScn(new BigInteger(maxScn));
                    currentScnInfoDTO.setArchivedLogFileNumber(Long.valueOf(archivedLogFileNumber));
                    return currentScnInfoDTO;
                } while (resultSet.next());
            }
            return null;
        });
        if (Objects.nonNull(logInfo)) {
            currentScn(jdbcTemplate, logInfo);
        }
        return logInfo;
    }

    private void currentScn(@NotNull JdbcTemplate jdbcTemplate, CurrentScnInfoDTO currentScnInfoDTO) {
        jdbcTemplate.query(IConstants.CURRENT_SCN, resultSet -> {
            do {
                currentScnInfoDTO.setCurrentScn(new BigInteger(resultSet.getString(1)));
            } while (resultSet.next());
        });
    }

    private void monitorDirectory(String taskId, String outputPath, DataProducer producer, DatasourceDTO datasourceDTO) throws Exception {
        FileAlterationObserver fileAlterationObserver = new FileAlterationObserver(new File(outputPath), FileFilterUtils.or(FileFilterUtils.and(FileFilterUtils.directoryFileFilter(), HiddenFileFilter.VISIBLE), FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter(Constants.DATA_SUFFIX))));
        fileAlterationObserver.addListener(new FileAlterationListenerAdaptor() {
            @Override
            public void onFileCreate(final File file) {
                generatorDataLogFileDTO(taskId, file, datasourceDTO, producer);
            }
        });
        FileAlterationMonitor fileAlterationMonitor = new FileAlterationMonitor(1000L, fileAlterationObserver);
        fileAlterationMonitor.start();
    }

    private void generatorDataLogFileDTO(String taskId, @NotNull File file, DatasourceDTO datasourceDTO, DataProducer producer) {
        String fileName = file.getName();
        if (fileName.lastIndexOf(commonConfig.getIncrementDataFilePrefix()) != -1) {
            DataLogFileDTO dataLogFileDTO = new DataLogFileDTO();
            dataLogFileDTO.setTaskId(taskId);
            String firstNumber = fileName.substring(fileName.lastIndexOf(commonConfig.getIncrementDataFilePrefix()) + commonConfig.getIncrementDataFilePrefix().length(), fileName.indexOf('@'));
            String lastNumber = fileName.substring(fileName.lastIndexOf('@') + 1, fileName.indexOf('.'));
            dataLogFileDTO.setIndex(firstNumber + "." + lastNumber);
            dataLogFileDTO.setDataLogPath(file.getAbsolutePath());
            dataLogFileDTO.setEmpty(file.length() == 0);
            dataLogFileDTO.setDatasourceDTO(datasourceDTO);
            producer.put(dataLogFileDTO);
        }
    }

    private LogMiningStrategy resolveFlushStrategy(@NotNull DatasourceDTO datasourceDTO) {
        try {
            if (datasourceDTO.isCluster()) {
                return new RacNodesLogMiningStrategy(datasourceDTO);
            }
            return new SingleNodeLogMiningStrategy(datasourceDTO);
        } catch (SQLException e) {
            log.error("日志挖掘策略对象获取异常!", e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (!CollectionUtils.isEmpty(captureIncrementDataMap)) {
            captureIncrementDataMap.forEach((key, captureIncrement) -> {
                try {
                    captureIncrement.stop();
                } catch (IOException ignored) {
                }
            });
        }
    }

    @Async("metricsScheduleExecutor")
    @Scheduled(fixedDelayString = "200000")
    protected void outputIncrementLog() {
        if (StringUtils.isNotBlank(atomicReference.get())) {
            switch (commonConfig.getReplicateType()) {
                case 2:
                    DbIncrementMetricsDTO dbIncrementMetricsDTO = dbIncrementMetricsHandler.findMaxDeployMetrics(atomicReference.get(), false);
                    if (Objects.nonNull(dbIncrementMetricsDTO)) {
                        OutputLogWithTable outputLogWithTable = OutputLogWithTable.create("增量数据下沉统计指标如下:");
                        outputLogWithTable.setSbcMode(false);
                        outputLogWithTable.addHeader("任务Id", "目标Schema", "目标表名", "事务产生时间", "数据下沉时间", "数据下沉延迟(毫秒)", "数据下沉指标");
                        outputLogWithTable.addBody(dbIncrementMetricsDTO.getTaskId(), dbIncrementMetricsDTO.getTargetSchema(), dbIncrementMetricsDTO.getTargetTable(), String.valueOf(dbIncrementMetricsDTO.getTransactionTime()), String.valueOf(dbIncrementMetricsDTO.getSinkTime()), String.valueOf(dbIncrementMetricsDTO.getSinkDelay()), dbIncrementMetricsDTO.getSinkMetrics());
                        outputLogWithTable.printToConsole();
                    }
                    break;
                case 1:
                default:
                    dbIncrementMetricsDTO = dbIncrementMetricsHandler.findMaxDeployMetrics(atomicReference.get(), true);
                    if (Objects.nonNull(dbIncrementMetricsDTO)) {
                        OutputLogWithTable outputLogWithTable = OutputLogWithTable.create("增量数据采集统计指标如下:");
                        outputLogWithTable.setSbcMode(false);
                        outputLogWithTable.addHeader("任务Id", "Schema", "表名", "事务产生时间", "数据采集时间", "采集延迟(毫秒)", "采集日志指标");
                        outputLogWithTable.addBody(dbIncrementMetricsDTO.getTaskId(), dbIncrementMetricsDTO.getTargetSchema(), dbIncrementMetricsDTO.getTargetTable(), String.valueOf(dbIncrementMetricsDTO.getTransactionTime()), String.valueOf(dbIncrementMetricsDTO.getCaptureTime()), String.valueOf(dbIncrementMetricsDTO.getCaptureDelay()), dbIncrementMetricsDTO.getCaptureMetrics());
                        outputLogWithTable.printToConsole();
                    }
                    break;
            }
        }
    }
}
