package org.dukejasun.migrate.service.capture.increment.strategy;

import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.input.ParameterDTO;

import java.sql.SQLException;

/**
 * @author dukedpsun
 */
public interface LogMiningStrategy extends AutoCloseable {

    String getHost();

    void flush() throws InterruptedException, SQLException;

    void createDataDictionary(ParameterDTO parameterDTO);
}
