package org.dukejasun.migrate.service.capture;

import org.dukejasun.migrate.model.dto.input.ParameterDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author dukedpsun
 */
public interface MigrateCaptureService extends Closeable {

    /**
     * 开始结构、全量、增量数据迁移
     *
     * @param taskId
     * @param parameterDTO
     * @param jdbcTemplate
     * @throws Exception
     */
    void startMigrateTask(String taskId, ParameterDTO parameterDTO, JdbcTemplate jdbcTemplate) throws Exception;

    /**
     * 等待迁移任务是否完成
     *
     * @param captureTableList
     * @throws InterruptedException
     */
    default void waiteUntilFinish(@NotNull List<CaptureTableDTO> captureTableList) throws InterruptedException {
        long size = captureTableList.stream().filter(captureTableDTO -> captureTableDTO.getLongAdder().intValue() == 0).count();
        while (size > 0) {
            TimeUnit.MILLISECONDS.sleep(1000L);
            size = captureTableList.stream().filter(captureTableDTO -> captureTableDTO.getLongAdder().intValue() == 0).count();
        }
    }
}
