package org.dukejasun.migrate.service.sink;

import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;

/**
 * @author dukedpsun
 */
public interface DataSinkService {

    /**
     * 接收数据文件
     *
     * @param dataLogFileDTO
     * @return
     * @throws Exception
     */
    String receiveDataLog(DataLogFileDTO dataLogFileDTO) throws Exception;
}
