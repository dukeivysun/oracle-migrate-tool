package org.dukejasun.migrate.service.sink.increment;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.model.dto.event.SinkDataDTO;
import org.dukejasun.migrate.model.dto.output.ActualDataDTO;
import org.dukejasun.migrate.model.dto.output.OriginDataDTO;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.sink.DataSinkService;
import org.dukejasun.migrate.utils.EncryptUtil;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("INCREMENT_RECEIVE_SINK_DATA")
public class IncrementDataSinkService implements DataSinkService {
    private final DataProducer dataProducer;
    private final CommonConfig commonConfig;
    private final ApplicationContext applicationContext;
    private final IncrementDataProcessingService incrementDataProcessingService;
    private final ThreadPoolExecutor executorIncrementService;
    private final Map<String, DatabaseFeatures> databaseFeaturesMap;
    private final AtomicInteger startNumber = new AtomicInteger(0);
    private final ConcurrentHashMap<String, DataLogFileDTO> map = new ConcurrentHashMap<>();

    @Autowired
    public IncrementDataSinkService(@NotNull CommonConfig commonConfig, ApplicationContext applicationContext, IncrementDataProcessingService incrementDataProcessingService, WriteDataToTarget writeDataToTarget, Map<String, DatabaseFeatures> databaseFeaturesMap) {
        this.commonConfig = commonConfig;
        this.applicationContext = applicationContext;
        this.incrementDataProcessingService = incrementDataProcessingService;
        this.databaseFeaturesMap = databaseFeaturesMap;

        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        migrateWorkHandler.setWriteDataToTarget(writeDataToTarget);
        dataProducer = new DataProducer(DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize()), migrateWorkHandler);
        executorIncrementService = new ThreadPoolExecutor(1, 1, 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536), new ThreadFactoryBuilder().setNameFormat(Constants.READ_DATA_FILE_THREAD_NAME).build(), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public String receiveDataLog(DataLogFileDTO dataLogFileDTO) {
        map.put(String.valueOf(dataLogFileDTO.getIndex()), dataLogFileDTO);
        executorIncrementService.submit(this::readAndSend);
        return null;
    }

    private void readAndSend() {
        synchronized (this) {
            AtomicInteger atomicInteger = new AtomicInteger(1);
            startNumber.set(commonConfig.getIncrementStartNumber());
            while (!CollectionUtils.isEmpty(map)) {
                boolean flag = isExists(map, startNumber.get());
                while (!flag) {
                    flag = isExists(map, startNumber.get());
                }
                while (isExists(map, startNumber.get())) {
                    DataLogFileDTO dataLogFileDTO = map.get(startNumber.get() + "." + atomicInteger.get());
                    DatabaseFeatures databaseFeatures = databaseFeaturesMap.get(dataLogFileDTO.getDatasourceDTO().getType().name() + "_FEATURES");

                    if (!dataLogFileDTO.getEmpty()) {
                        try {
                            Files.asCharSource(new File(dataLogFileDTO.getDataLogPath()), Charset.defaultCharset()).readLines(new LineProcessor<String>() {
                                final List<OriginDataDTO> originDataDTOList = Lists.newArrayList();
                                final Boolean supportEncrypt = Objects.nonNull(commonConfig.getSupportEncrypt()) && commonConfig.getSupportEncrypt();

                                @Override
                                public boolean processLine(@NotNull String line) {
                                    if (StringUtils.isNotBlank(line)) {
                                        OriginDataDTO originDataDTO;
                                        if (supportEncrypt) {
                                            originDataDTO = JacksonUtil.fromJson(EncryptUtil.decodePassWord(line), OriginDataDTO.class);
                                        } else {
                                            originDataDTO = JacksonUtil.fromJson(line, OriginDataDTO.class);
                                        }
                                        if (Objects.nonNull(originDataDTO)) {
                                            switch (originDataDTO.getType()) {
                                                case COMMIT:
                                                    return true;
                                                case ROLLBACK:
                                                    String xId = originDataDTO.getXId();
                                                    if (StringUtils.isNotBlank(xId)) {
                                                        originDataDTOList.removeIf(originDataDTO1 -> originDataDTO1.getXId().equals(xId));
                                                    } else {
                                                        int size = originDataDTOList.size() - 1;
                                                        if (size >= 0) {
                                                            OriginDataDTO originDataLast = originDataDTOList.get(size);
                                                            if (originDataDTO.getRowId().equals(originDataLast.getRowId())) {
                                                                originDataDTOList.remove(size);
                                                            }
                                                        }
                                                    }
                                                    return true;
                                                default:
                                                    originDataDTOList.add(originDataDTO);
                                                    break;
                                            }
                                        }
                                    }
                                    return true;
                                }

                                @Override
                                public String getResult() {
                                    if (!CollectionUtils.isEmpty(originDataDTOList)) {
                                        try {
                                            List<ActualDataDTO> actualDataDTOList = incrementDataProcessingService.generatorTargetScript(databaseFeatures, originDataDTOList);
                                            if (!CollectionUtils.isEmpty(actualDataDTOList)) {
                                                SinkDataDTO sinkDataDTO = new SinkDataDTO();
                                                sinkDataDTO.setDealIndex(startNumber.get());
                                                sinkDataDTO.setSerialNumber(atomicInteger.longValue());
                                                sinkDataDTO.setTaskId(dataLogFileDTO.getTaskId());
                                                sinkDataDTO.setDatasourceDTO(dataLogFileDTO.getDatasourceDTO());
                                                sinkDataDTO.setActualDataDTOList(actualDataDTOList);
                                                dataProducer.put(sinkDataDTO);
                                            }
                                        } catch (Exception e) {
                                            log.error("任务Id:【{}】数据文件索引:{},数据处理失败!", dataLogFileDTO.getTaskId(), startNumber.get(), e);
                                            System.exit(SpringApplication.exit(applicationContext, () -> 0));
                                        }
                                    }
                                    return "successfully";
                                }
                            });
                        } catch (IOException e) {
                            log.error("任务Id:【{}】读取数据文件失败!:{}", dataLogFileDTO.getTaskId(), dataLogFileDTO, e);
                            System.exit(SpringApplication.exit(applicationContext, () -> 0));
                        }
                    } else {
                        log.info("任务Id:【{}】数据文件索引:{},文件内容为空!", dataLogFileDTO.getTaskId(), startNumber.get());
                    }
                    map.remove(startNumber.get() + "." + atomicInteger.get());
                    atomicInteger.incrementAndGet();

                    try {
                        TimeUnit.MILLISECONDS.sleep(50L);
                    } catch (InterruptedException ignored) {
                    }
                }
                startNumber.incrementAndGet();
                commonConfig.setIncrementStartNumber(startNumber.get());
                atomicInteger.set(1);
            }
        }
    }

    private boolean isExists(ConcurrentHashMap<String, DataLogFileDTO> map, int startNumber) {
        if (CollectionUtils.isEmpty(map)) {
            return false;
        }
        Set<String> setKey = map.keySet();
        for (String key : setKey) {
            if (Integer.parseInt(key.substring(0, key.indexOf("."))) == startNumber) {
                return true;
            }
        }
        return false;
    }
}
