package org.dukejasun.migrate.service.capture.increment.capture;

import org.dukejasun.migrate.model.dto.event.CaptureConditionDTO;

import java.io.IOException;

/**
 * @author dukedpsun
 */
public interface CaptureIncrementData {

    /**
     * 采集增量数据
     *
     * @param captureConditionDTO
     * @throws Exception
     */
    void startCapture(CaptureConditionDTO captureConditionDTO) throws Exception;

    /**
     * 结束采集
     */
    void stop() throws IOException;
}
