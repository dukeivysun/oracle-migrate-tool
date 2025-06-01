package org.dukejasun.migrate.service.sink.total;

import com.google.common.util.concurrent.*;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.service.sink.DataSinkService;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("TOTAL_RECEIVE_SINK_DATA")
public class TotalDataSinkService implements DataSinkService {
    private final ThreadPoolExecutor executorIncrementService;
    private final ListeningExecutorService listeningExecutorService;
    private final Map<String, DatabaseFeatures> databaseFeaturesMap;
    private final ApplicationContext applicationContext;

    @Autowired
    public TotalDataSinkService(@NotNull CommonConfig commonConfig, Map<String, DatabaseFeatures> databaseFeaturesMap, ApplicationContext applicationContext) {
        this.databaseFeaturesMap = databaseFeaturesMap;
        this.applicationContext = applicationContext;
        executorIncrementService = new ThreadPoolExecutor(commonConfig.getTotalMigrateThreadNumber(), commonConfig.getTotalMigrateThreadNumber(), 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536), new ThreadFactoryBuilder().setNameFormat(Constants.SINK_THREAD_NAME).build(), new ThreadPoolExecutor.CallerRunsPolicy());
        listeningExecutorService = MoreExecutors.listeningDecorator(executorIncrementService);
    }

    @Override
    public String receiveDataLog(@NotNull DataLogFileDTO dataLogFileDTO) {
        if (!dataLogFileDTO.getEmpty()) {
            ListenableFuture<String> listenableFuture = listeningExecutorService.submit(new WriteToTargetCallable(databaseFeaturesMap, dataLogFileDTO));
            Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                @Override
                public void onSuccess(String value) {
                }

                @Override
                public void onFailure(Throwable throwable) {
                    log.error(throwable.getMessage());
                    System.exit(SpringApplication.exit(applicationContext, () -> 0));
                }
            }, executorIncrementService);
        }
        return null;
    }

    static class WriteToTargetCallable implements Callable<String> {
        private final Map<String, DatabaseFeatures> databaseFeaturesMap;
        private final DataLogFileDTO dataLogFileDTO;

        public WriteToTargetCallable(Map<String, DatabaseFeatures> databaseFeaturesMap, DataLogFileDTO dataLogFileDTO) {
            this.databaseFeaturesMap = databaseFeaturesMap;
            this.dataLogFileDTO = dataLogFileDTO;
        }

        @Override
        public String call() {
            try {
                databaseFeaturesMap.get(dataLogFileDTO.getDatasourceDTO().getType().name() + "_FEATURES").sinkToTarget(dataLogFileDTO);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
            return "successfully";
        }
    }
}
