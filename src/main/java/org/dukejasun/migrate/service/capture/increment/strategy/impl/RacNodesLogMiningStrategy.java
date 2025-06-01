package org.dukejasun.migrate.service.capture.increment.strategy.impl;

import com.google.common.collect.Lists;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.input.ParameterDTO;
import org.dukejasun.migrate.service.capture.increment.strategy.LogMiningStrategy;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author dukedpsun
 */
@Slf4j
public class RacNodesLogMiningStrategy implements LogMiningStrategy {
    private final DatasourceDTO datasourceDTO;
    private final List<SingleNodeLogMiningStrategy> logMiningStrategies = Lists.newArrayList();

    public RacNodesLogMiningStrategy(@NotNull DatasourceDTO datasourceDTO) throws SQLException {
        this.datasourceDTO = datasourceDTO;
        List<DatasourceDTO> clusterList = datasourceDTO.getClusterList();
        if (!CollectionUtils.isEmpty(clusterList)) {
            clusterList.forEach(datasourceNode -> {
                datasourceNode.setVersion(datasourceDTO.getVersion());
                try {
                    logMiningStrategies.add(new SingleNodeLogMiningStrategy(datasourceNode));
                } catch (SQLException ignored) {
                }
            });
        }
    }

    @Override
    public String getHost() {
        return datasourceDTO.getHost();
    }

    @Override
    public void flush() {
        logMiningStrategies.forEach(singleNodeLogMiningStrategy -> {
            boolean recreateConnections = false;
            try {
                singleNodeLogMiningStrategy.flush();
            } catch (InterruptedException | SQLException e) {
                log.error(e.getMessage());
                recreateConnections = true;
            }
            if (recreateConnections) {
                try {
                    TimeUnit.MILLISECONDS.sleep(3000L);
                    singleNodeLogMiningStrategy.flush();
                } catch (InterruptedException | SQLException e) {
                    log.error(e.getMessage());
                }
            }
        });
    }

    @Override
    public void createDataDictionary(ParameterDTO parameterDTO) {
        logMiningStrategies.forEach(singleNodeLogMiningStrategy -> singleNodeLogMiningStrategy.createDataDictionary(parameterDTO));
    }

    @Override
    public void close() throws Exception {
        logMiningStrategies.forEach(singleNodeLogMiningStrategy -> {
            try {
                singleNodeLogMiningStrategy.close();
            } catch (Exception ignored) {
            }
        });
    }
}
