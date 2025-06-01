package org.dukejasun.migrate.service.analyse.script;

import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.AnalyseResultDTO;

import java.sql.SQLException;
import java.util.Map;

/**
 * @author dukedpsun
 */
public interface AnalyseDatabaseScriptService {

    /**
     * 分析表结构
     *
     * @param captureTableDTO
     * @param dataSourceDTO
     * @param databaseFeatures
     * @param dataTypeMappingBeanMap
     * @return
     */
    AnalyseResultDTO analyseTableAndConvert(CaptureTableDTO captureTableDTO, DatasourceDTO dataSourceDTO, DatabaseFeatures databaseFeatures, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) throws SQLException;

}
