package org.dukejasun.migrate.service.capture.increment.capture.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.lmax.disruptor.dsl.Disruptor;
import org.dukejasun.migrate.cache.CachedOperation;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.handler.FileConvertHandler;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;
import org.dukejasun.migrate.model.dto.event.DbCaptureRecordDTO;
import org.dukejasun.migrate.model.dto.event.DbIncrementMetricsDTO;
import org.dukejasun.migrate.model.dto.output.OriginDataDTO;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.capture.increment.capture.CaptureIncrementData;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.dukejasun.migrate.utils.EncryptUtil;
import org.dukejasun.migrate.utils.FileUtil;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("CLUSTER_CAPTURE_INCREMENT_DATA")
public class ClusterCaptureIncrementDataImpl implements CaptureIncrementData {
    private String parentDir;
    private String fileName;
    private Logger logger;
    private final List<Object> originDataList;
    private final CommonConfig commonConfig;
    private final DataProducer dataProducer;
    private final ApplicationContext applicationContext;
    private final CachedOperation<String, Object> cachedOperation;
    private final CaptureClusterDataByTransactional captureClusterDataByTransactional;
    private final AtomicReference<OriginDataDTO> atomicReference = new AtomicReference<>();

    @Autowired
    public ClusterCaptureIncrementDataImpl(@NotNull CommonConfig commonConfig, MigrateResultsStatisticsService migrateResultsStatisticsService, CachedOperation<String, Object> cachedOperation, ApplicationContext applicationContext, CaptureClusterDataByTransactional captureClusterDataByTransactional) {
        this.commonConfig = commonConfig;
        this.cachedOperation = cachedOperation;
        this.applicationContext = applicationContext;
        this.captureClusterDataByTransactional = captureClusterDataByTransactional;

        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
        migrateWorkHandler.setMigrateResultsStatisticsService(migrateResultsStatisticsService);
        dataProducer = new DataProducer(disruptor, migrateWorkHandler);

        originDataList = Lists.newArrayList();
        sendDataWithScheduled();
    }

    @Override
    public void startCapture(@NotNull CaptureConditionDTO captureConditionDTO) throws IOException {
        try {
            this.parentDir = captureConditionDTO.getParentDir();
            this.fileName = commonConfig.getIncrementDataFilePrefix() + captureConditionDTO.getIndex();
            createDataFile(parentDir, fileName);
            String value = captureClusterDataByTransactional.captureDataFromLog(captureConditionDTO, originDataList);
            if (StringUtils.isNotBlank(value)) {
                dataProducer.put(JacksonUtil.fromJson(value, DbCaptureRecordDTO.class));
            }
        } catch (Exception e) {
            log.error("日志挖掘失败!", e);
            try {
                TimeUnit.MILLISECONDS.sleep(2000L);
            } catch (InterruptedException ignored) {
            }
            stop();
            System.exit(SpringApplication.exit(applicationContext, () -> 0));
        } finally {
            modifyFile();
        }
    }

    protected void sendDataWithScheduled() {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4, new BasicThreadFactory.Builder().namingPattern("cluster_data_scheduled_pool_%d").daemon(true).build());
        executorService.scheduleAtFixedRate(() -> {
            synchronized (this) {
                if (!CollectionUtils.isEmpty(originDataList) && Objects.nonNull(logger)) {
                    originDataList.stream().map(obj -> (OriginDataDTO) obj).collect(Collectors.toList()).sort(Comparator.comparing(OriginDataDTO::getScn));
                    StringBuilder builder = new StringBuilder();
                    for (Iterator<?> iterator = originDataList.iterator(); iterator.hasNext(); ) {
                        builder.append(getDataJson(iterator.next()));
                        if (iterator.hasNext()) {
                            builder.append('\n');
                        }
                    }
                    logger.info(builder.toString());
                    builder.setLength(0);
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
                    originDataList.clear();
                }
            }
        }, 0, 5L, TimeUnit.SECONDS);
    }

    private void modifyFile() throws IOException {
        if (StringUtils.isNotBlank(fileName) && StringUtils.isNotBlank(parentDir)) {
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
    }

    protected String getDataJson(Object object) {
        atomicReference.set((OriginDataDTO) object);
        if (Objects.nonNull(commonConfig.getSupportEncrypt()) && commonConfig.getSupportEncrypt()) {
            return EncryptUtil.encryptPassword(JacksonUtil.toJson(object));
        }
        return JacksonUtil.toJson(object);
    }

    private void createDataFile(String parentDir, String fileName) {
        logger = FileConvertHandler.createOrGetHandler(parentDir, fileName + '@', Constants.TEMP_SUFFIX, commonConfig.getDataFileThreshold(), Boolean.FALSE);
        if (!new File(parentDir + File.separator + fileName + '@' + Constants.TEMP_SUFFIX).exists()) {
            createDataFile(parentDir, fileName);
        }
    }
    @Override
    public void stop() throws IOException {
        cachedOperation.close();
        if (!CollectionUtils.isEmpty(originDataList)) {
            originDataList.clear();
        }
    }
}
