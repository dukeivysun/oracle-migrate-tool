package org.dukejasun.migrate.service.analyse.script.impl;

import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.AnalyseResultDTO;
import org.dukejasun.migrate.service.analyse.script.AnalyseDatabaseScriptService;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Map;

/**
 * @author dukedpsun
 */
@Slf4j
public class AnalyseMySQLScriptService implements AnalyseDatabaseScriptService {
    public AnalyseMySQLScriptService() {

    }

    @Override
    public AnalyseResultDTO analyseTableAndConvert(CaptureTableDTO captureTableDTO, DatasourceDTO dataSourceDTO, DatabaseFeatures databaseFeatures, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) throws SQLException {
        return null;
    }
}
