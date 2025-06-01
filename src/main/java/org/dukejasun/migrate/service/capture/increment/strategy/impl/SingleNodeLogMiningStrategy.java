package org.dukejasun.migrate.service.capture.increment.strategy.impl;

import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.model.connection.OracleConnection;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.input.ParameterDTO;
import org.dukejasun.migrate.model.vo.Scn;
import org.dukejasun.migrate.service.capture.increment.strategy.LogMiningStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Objects;

/**
 * @author dukedpsun
 */
@Slf4j
public class SingleNodeLogMiningStrategy implements LogMiningStrategy {

    private final DatasourceDTO datasourceDTO;
    private final OracleConnection oracleConnection;

    public SingleNodeLogMiningStrategy(@NotNull DatasourceDTO datasourceDTO) throws SQLException {
        this.datasourceDTO = datasourceDTO;
        String url = MessageFormat.format(DatabaseExpandType.ORACLE.getUrlTemplate(), datasourceDTO.getHost(), String.valueOf(datasourceDTO.getPort()), datasourceDTO.getDatabaseName());
        this.oracleConnection = new OracleConnection(url, datasourceDTO.getUsername(), datasourceDTO.getPassword());
        createFlushTableIfNotExists();
    }

    @Override
    public String getHost() {
        return datasourceDTO.getHost();
    }

    @Override
    public void flush() throws InterruptedException, SQLException {
        Scn currentScn = getCurrentScn();
        if (Objects.nonNull(currentScn)) {
            try {
                oracleConnection.executeWithoutCommitting(Constants.OracleConstants.UPDATE_FLUSH_TABLE + currentScn);
            } catch (SQLException e) {
                throw new RuntimeException(getHost() + "归档Oracle在线日志失败.", e);
            }
        }
    }

    @Override
    public void createDataDictionary(ParameterDTO parameterDTO) {
        try {
            int version = Integer.parseInt(datasourceDTO.getVersion().substring(0, datasourceDTO.getVersion().indexOf(".")));
            if (version < 12) {
                if (StringUtils.isNotBlank(parameterDTO.getDictionaryPath())) {
                    oracleConnection.executeWithoutCommitting(MessageFormat.format(IConstants.BUILD_FLAT_DATA_DICTIONARY, "dictionary_" + StringUtils.replace(datasourceDTO.getHost(), ".", "") + IConstants.SUFFIX, parameterDTO.getDictionaryPath()));
                }
            } else {
                if (version == 12) {
                    if (StringUtils.isNotBlank(parameterDTO.getDictionaryLocation())) {
                        oracleConnection.executeWithoutCommitting(MessageFormat.format(IConstants.BUILD_FLAT_DATA_DICTIONARY, "dictionary_" + StringUtils.replace(datasourceDTO.getHost(), ".", "") + IConstants.SUFFIX, parameterDTO.getDictionaryLocation()));
                    } else {
                        oracleConnection.executeWithoutCommitting(IConstants.BUILD_REDO_DATA_DICTIONARY);
                    }
                } else {
                    oracleConnection.executeWithoutCommitting(IConstants.BUILD_REDO_DATA_DICTIONARY);
                }
            }
        } catch (Exception e) {
            log.error("创建数据字典失败!", e);
            throw new RuntimeException("创建数据字典失败!" + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (Objects.nonNull(oracleConnection)) {
            oracleConnection.close();
        }
    }

    private void createFlushTableIfNotExists() throws SQLException {
        if (!isTableExists()) {
            oracleConnection.executeWithoutCommitting(Constants.OracleConstants.CREATE_FLUSH_TABLE);
        }
        if (isTableEmpty()) {
            oracleConnection.executeWithoutCommitting(Constants.OracleConstants.INSERT_FLUSH_TABLE);
        }
    }

    private boolean isTableExists() throws SQLException {
        return oracleConnection.queryAndMap(Constants.OracleConstants.EXISTS_TABLE, resultSet -> resultSet.next() && resultSet.getLong(1) > 0);
    }

    private boolean isTableEmpty() throws SQLException {
        return oracleConnection.queryAndMap(Constants.OracleConstants.QUERY_TABLE_COUNT, resultSet -> {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
            return 0L;
        }) == 0L;
    }

    public Scn getCurrentScn() throws SQLException {
        return oracleConnection.queryAndMap(Constants.OracleConstants.QUERY_CURRENT_SCN, (resultSet) -> {
            if (resultSet.next()) {
                return Scn.valueOf(resultSet.getString(1));
            }
            return null;
        });
    }
}
