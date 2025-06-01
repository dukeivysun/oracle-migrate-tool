package org.dukejasun.migrate.enums;

import org.dukejasun.migrate.cache.LocalCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

/**
 * 获取数据库连接
 *
 * @author dukedpsun
 */
@Slf4j
public enum DataSourceFactory {
    /**
     * 实例
     */
    INSTANCE;

    private final Map<DatasourceDTO, DataSource> dataSourceMap = new LocalCache<>(32);

    /**
     * 创建获取获取数据源
     *
     * @param dataSourceDTO
     * @param commonConfig
     * @return
     */
    public DataSource getDataSource(DatasourceDTO dataSourceDTO, CommonConfig commonConfig) {
        DataSource dataSource = dataSourceMap.get(dataSourceDTO);
        if (Objects.isNull(dataSource)) {
            dataSource = createDataSource(dataSourceDTO, commonConfig.getMinimumIdle(), commonConfig.getMaximumPoolSize(), dataSourceDTO.getParameterMap(), commonConfig);
            dataSourceMap.put(dataSourceDTO, dataSource);
        }
        return dataSource;
    }

    private @NotNull DataSource createDataSource(@NotNull DatasourceDTO dbConfig, Integer minimumIdle, Integer maximumPoolSize, Map<String, Object> extra, @NotNull CommonConfig commonConfig) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername(dbConfig.getUsername());
        dataSource.setPassword(dbConfig.getPassword());
        dataSource.setMinimumIdle(minimumIdle);
        dataSource.setMaximumPoolSize(maximumPoolSize);
        if (Objects.nonNull(commonConfig.getMaxLifetimeMs())) {
            dataSource.setMaxLifetime(commonConfig.getMaxLifetimeMs());
        }
        dataSource.setDriverClassName(dbConfig.getType().getDriver());
        if (Objects.nonNull(commonConfig.getIdleTimeoutMs())) {
            dataSource.setIdleTimeout(commonConfig.getIdleTimeoutMs());
        }
        if (Objects.nonNull(commonConfig.getConnectionTimeoutMs())) {
            dataSource.setConnectionTimeout(commonConfig.getConnectionTimeoutMs());
        }
        if (Objects.nonNull(commonConfig.getValidationTimeoutMs())) {
            dataSource.setValidationTimeout(commonConfig.getValidationTimeoutMs());
        }
        dataSource.setAutoCommit(true);
        DatabaseExpandType databaseType = dbConfig.getType();
        dataSource.setPoolName(databaseType.name() + "_HikariPool");
        dataSource.setJdbcUrl(MessageFormat.format(databaseType.getUrlTemplate(), dbConfig.getHost(), String.valueOf(dbConfig.getPort()), dbConfig.getDatabaseName()));
        if (StringUtils.isNotBlank(databaseType.getTestQuery())) {
            dataSource.setConnectionTestQuery(databaseType.getTestQuery());
        }
        if (!CollectionUtils.isEmpty(extra)) {
            extra.forEach(dataSource::addDataSourceProperty);
        } else {
            switch (databaseType) {
                case ORACLE:
                    dataSource.addDataSourceProperty("useUnicode", "true");
                    dataSource.addDataSourceProperty("characterEncoding", StringUtils.isNotBlank(commonConfig.getOracleCharacterEncoding()) ? commonConfig.getOracleCharacterEncoding() : "UTF-8");
                    dataSource.addDataSourceProperty("remarksReporting", "true");
                    dataSource.addDataSourceProperty("v$session.program", "migrate-task-oracle-connection");
                    break;
                   case MYSQL:
                    dataSource.addDataSourceProperty("cachePrepStmts", "true");
                    dataSource.addDataSourceProperty("prepStmtCacheSize", "250");
                    dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
                    dataSource.addDataSourceProperty("useServerPrepStmts", "true");
                    dataSource.addDataSourceProperty("useLocalSessionState", "true");
                    dataSource.addDataSourceProperty("useLocalTransactionState", "true");
                    dataSource.addDataSourceProperty("rewriteBatchedStatements", "true");
                    dataSource.addDataSourceProperty("cacheResultSetMetadata", "true");
                    dataSource.addDataSourceProperty("cacheServerConfiguration", "true");
                    dataSource.addDataSourceProperty("elideSetAutoCommits", "true");
                    dataSource.addDataSourceProperty("maintainTimeStats", "false");
                    dataSource.addDataSourceProperty("useSSL", "false");
                    dataSource.addDataSourceProperty("allowLoadLocalInfile", "true");
                    dataSource.addDataSourceProperty("allowMultiQueries", "true");
                    dataSource.addDataSourceProperty("readOnlyPropagatesToServer", "false");
                    dataSource.addDataSourceProperty("zeroDateTimeBehavior", "convertToNull");
                    dataSource.addDataSourceProperty("connectionTimeZone", ZoneId.systemDefault().toString());
                    dataSource.addDataSourceProperty("useUnicode", "true");
                    dataSource.addDataSourceProperty("characterEncoding", StringUtils.isNotBlank(commonConfig.getCharacterEncoding()) ? commonConfig.getCharacterEncoding() : "UTF-8");
                    dataSource.addDataSourceProperty("tinyInt1isBit", "false");
                    dataSource.addDataSourceProperty("noDatetimeStringSync", "true");
                    dataSource.addDataSourceProperty("netTimeoutForStreamingResults", "3600");
                    dataSource.addDataSourceProperty("autoReconnect", "true");
                    dataSource.addDataSourceProperty("failOverReadOnly", "false");
                    dataSource.addDataSourceProperty("autoClosePStmtStreams", "false");
                    dataSource.addDataSourceProperty("useCursorFetch", "true");
                    dataSource.addDataSourceProperty("yearIsDateType", "true");
                    dataSource.addDataSourceProperty("connectionAttributes", "program_name:migrate-task-mysql-connection");
                    break;
                default:
                    break;
            }
        }
        return dataSource;
    }
}
