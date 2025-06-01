package org.dukejasun.migrate.service.capture.increment.capture.impl;

import com.google.common.util.concurrent.*;
import com.lmax.disruptor.dsl.Disruptor;
import org.dukejasun.migrate.cache.CachedOperation;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;
import org.dukejasun.migrate.model.dto.event.DbCaptureRecordDTO;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import org.dukejasun.migrate.service.capture.increment.capture.CaptureIncrementData;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("SINGLE_CAPTURE_INCREMENT_DATA")
public class CaptureIncrementDataImpl implements CaptureIncrementData {
    private final CommonConfig commonConfig;
    private final Map<String, DataTypeConvert> dataTypeConvertMap;
    private final ExecutorService executorIncrementService;
    private final ListeningExecutorService listeningExecutorService;
    private final DataProducer dataProducer;
    private final ApplicationContext applicationContext;
    private final CachedOperation<String, Object> cachedOperation;

    @Autowired
    public CaptureIncrementDataImpl(@NotNull CommonConfig commonConfig, Map<String, DataTypeConvert> dataTypeConvertMap, MigrateResultsStatisticsService migrateResultsStatisticsService, CachedOperation<String, Object> cachedOperation, ApplicationContext applicationContext) {
        this.commonConfig = commonConfig;
        this.dataTypeConvertMap = dataTypeConvertMap;
        this.cachedOperation = cachedOperation;
        this.applicationContext = applicationContext;

        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(commonConfig.getBufferSize());
        migrateWorkHandler.setMigrateResultsStatisticsService(migrateResultsStatisticsService);
        dataProducer = new DataProducer(disruptor, migrateWorkHandler);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(Constants.INCREMENT_CAPTURE_THREAD_NAME).build();
        executorIncrementService = new ThreadPoolExecutor(commonConfig.getIncrementCaptureThreadNumber(), commonConfig.getIncrementCaptureThreadNumber(), 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536 * 2), namedThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        listeningExecutorService = MoreExecutors.listeningDecorator(executorIncrementService);
    }

    @Override
    public void startCapture(CaptureConditionDTO captureConditionDTO) {
        AbstractCaptureDataCallable abstractCaptureDataCallable;
        if (Objects.nonNull(commonConfig.getIncrementCaptureType()) && commonConfig.getIncrementCaptureType()) {
            abstractCaptureDataCallable = new CaptureDataByTransactionalCallable(captureConditionDTO, commonConfig, dataTypeConvertMap, cachedOperation, dataProducer);
        } else {
            abstractCaptureDataCallable = new CaptureDataByRowCallable(captureConditionDTO, commonConfig, dataTypeConvertMap, dataProducer);
        }
        ListenableFuture<String> listenableFuture = listeningExecutorService.submit(abstractCaptureDataCallable);
        Futures.addCallback(listenableFuture, new FutureCallback<String>() {
            @Override
            public void onSuccess(String value) {
                if (StringUtils.isNotBlank(value)) {
                    dataProducer.put(JacksonUtil.fromJson(value, DbCaptureRecordDTO.class));
                }
            }
            @Override
            public void onFailure(Throwable throwable) {
//                 解决oracle无法从套接字读取更多数据的问题
//                 https://www.qycn.com/xzx/article/13023.html#:~:text=%E2%80%9D%E6%96%87%E7%AB%A0%E8%83%BD%E5%B8%AE%E5%8A%A9%E5%A4%A7%E5%AE%B6%E8%A7%A3%E5%86%B3%E9%97%AE%E9%A2%98%E3%80%82%20%E5%9C%A8oracle%E4%B8%AD%EF%BC%8C%E5%8F%AF%E4%BB%A5%E5%88%A9%E7%94%A8%E2%80%9Calter%20system%20set,%22_optimizer_connect_by_cost_based%22%20%3D%20false%20scope%3Dboth%3B%E2%80%9D%E8%AF%AD%E5%8F%A5%E8%A7%A3%E5%86%B3%E6%97%A0%E6%B3%95%E4%BB%8E%E5%A5%97%E6%8E%A5%E5%AD%97%E8%AF%BB%E5%8F%96%E6%9B%B4%E5%A4%9A%E7%9A%84%E6%95%B0%E6%8D%AE%E7%9A%84%E5%BC%82%E5%B8%B8%EF%BC%8C%E8%AF%A5%E8%AF%AD%E5%8F%A5%E5%B0%86%E2%80%9C_optimizer_connect_by_cost_based%E2%80%9D%E7%9A%84%E5%80%BC%E8%AE%BE%E7%BD%AE%E4%B8%BAboth%EF%BC%8C%E4%BF%AE%E6%94%B9%E5%90%8E%E5%BD%93%E5%89%8D%E8%B5%B7%E4%BD%9C%E7%94%A8%EF%BC%8C%E4%B8%8B%E6%AC%A1%E9%87%8D%E5%90%AF%E6%95%B0%E6%8D%AE%E5%BA%93%E4%B9%9F%E8%B5%B7%E4%BD%9C%E7%94%A8%E3%80%82
                try {
                    TimeUnit.MILLISECONDS.sleep(2000L);
                } catch (InterruptedException ignored) {
                }
                System.exit(SpringApplication.exit(applicationContext, () -> 0));
            }
        }, executorIncrementService);
    }

    @Override
    public void stop() throws IOException {
        if (Objects.nonNull(listeningExecutorService)) {
            listeningExecutorService.shutdown();
        }
        if (Objects.nonNull(executorIncrementService)) {
            executorIncrementService.shutdown();
        }
        cachedOperation.close();
    }
}
