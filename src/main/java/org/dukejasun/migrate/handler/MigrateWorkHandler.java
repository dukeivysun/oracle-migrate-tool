package org.dukejasun.migrate.handler;

import com.lmax.disruptor.WorkHandler;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.service.capture.increment.capture.CaptureIncrementData;
import org.dukejasun.migrate.service.sink.DataSinkService;
import org.dukejasun.migrate.service.sink.increment.WriteDataToTarget;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * @author dukedpsun
 */
@Setter
@Slf4j
@NoArgsConstructor
public class MigrateWorkHandler implements WorkHandler<ReplicatorEvent> {

    private CaptureIncrementData captureIncrementData;
    private DataSinkService dataSinkService;
    private MigrateResultsStatisticsService migrateResultsStatisticsService;
    private WriteDataToTarget writeDataToTarget;


    @Override
    public void onEvent(@NotNull ReplicatorEvent event) throws Exception {
        String type = event.getEventObject().getType();
        switch (type) {
            case Constants.CONDITION_TYPE:
            case Constants.RAC_CONDITION_TYPE:
                if (Objects.nonNull(captureIncrementData)) {
                    captureIncrementData.startCapture((CaptureConditionDTO) event.getEventObject());
                }
                break;
            case Constants.DATA_LOG_TYPE:
                if (Objects.nonNull(dataSinkService)) {
                    dataSinkService.receiveDataLog((DataLogFileDTO) event.getEventObject());
                }
                break;
            case Constants.CALLBACK_TASK_TYPE:
            case Constants.CALLBACK_RECORD_TYPE:
            case Constants.INCREMENT_REPLICATOR_METRICS:
            case Constants.CALLBACK_CAPTURE_RECORD_TYPE:
                if (Objects.nonNull(migrateResultsStatisticsService)) {
                    migrateResultsStatisticsService.statistics(event.getEventObject());
                }
                break;
            case Constants.WRITE_DATA_TYPE:
                if (Objects.nonNull(writeDataToTarget)) {
                    writeDataToTarget.sinkDataToTarget(event.getEventObject());
                }
                break;
            default:
                break;
        }
    }
}
