package org.dukejasun.migrate.features.impl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLPartition;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureColumnDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.dukejasun.migrate.utils.StringTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("MySQL_FEATURES")
public class MySQLFeatures implements DatabaseFeatures {
    private final CommonConfig commonConfig;
    private final DataProducer dataProducer;

    @Autowired
    public MySQLFeatures(@NotNull CommonConfig commonConfig, MigrateResultsStatisticsService migrateResultsStatisticsService) {
        this.commonConfig = commonConfig;
        this.dataProducer = generatorDataProducer(commonConfig.getBufferSize(), migrateResultsStatisticsService);
    }

    @Override
    public String getDatabaseVersion(JdbcTemplate jdbcTemplate) {
        return null;
    }

    @Override
    public String getCharacterEncoding() {
        return commonConfig.getCharacterEncoding();
    }

    public String convert(String name) {
        if (StringUtils.containsIgnoreCase(commonConfig.getDataTypeMappingKey(), "mysql2")) {
            return '`' + name + '`';
        } else {
            int nameResolution = Objects.nonNull(commonConfig.getNameResolution()) ? commonConfig.getNameResolution() : 0;
            switch (nameResolution) {
                case 1:
                    return '`' + name.toUpperCase() + '`';
                case 2:
                    return '`' + name.toLowerCase() + '`';
                default:
                    return '`' + name + '`';
            }
        }
    }

    @Override
    public void sinkToTarget(@NotNull DataLogFileDTO dataLogFileDTO) throws IOException {
        log.info("任务Id:【{}】开始将{}表csv文件:{}的数据写入目标数据库.", dataLogFileDTO.getTaskId(), dataLogFileDTO.getKey(), dataLogFileDTO.getDataLogPath());
        int dataSize;
        CaptureTableDTO captureTableDTO = CaptureTableCache.INSTANCE.get(dataLogFileDTO.getKey());
        if (Objects.nonNull(captureTableDTO)) {
            DatasourceDTO datasourceDTO = dataLogFileDTO.getDatasourceDTO();
            final List<String> columnNameList = captureTableDTO.getColumnMetaList().stream().map(CaptureColumnDTO::getName).collect(Collectors.toList());
            String columnNames = getColumnNames(columnNameList);
            if (commonConfig.insertModel()) {
                final String destSql = MessageFormat.format(Constants.INSERT_SQL, getSchema(captureTableDTO.getTargetSchema()), getTableName(captureTableDTO.getTargetName()), columnNames);
                sinkToTarget(dataLogFileDTO, destSql, captureTableDTO, datasourceDTO, commonConfig, dataProducer);
                return;
            }
            String sql = MessageFormat.format(Constants.MySQLConstants.LOAD_DATA_INFILE, dataLogFileDTO.getDataLogPath(), commonConfig.getPrimaryKeyConflictOperator() == 1 ? "REPLACE" : "IGNORE", getSchema(captureTableDTO.getTargetSchema()), getTableName(captureTableDTO.getTargetName()), columnNames);
            log.info(sql);
            try (Connection connection = DataSourceFactory.INSTANCE.getDataSource(datasourceDTO, commonConfig).getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                connection.setAutoCommit(false);
                dataSize = preparedStatement.executeUpdate();
                connection.commit();
                log.info("任务Id:【{}】完成将{}表csv文件:{}的数据写入目标数据库.", dataLogFileDTO.getTaskId(), dataLogFileDTO.getKey(), dataLogFileDTO.getDataLogPath());
                if (dataSize != 0) {
                    sendResult(dataLogFileDTO.getTaskId(), captureTableDTO, (long) dataSize, null, dataProducer);
                }
                captureTableDTO.getSinkLongAddr().increment();
            } catch (SQLException e) {
                sendResult(dataLogFileDTO.getTaskId(), captureTableDTO, 0L, e.getMessage(), dataProducer);
                throw new RuntimeException(MessageFormat.format("任务Id:【{0}】{1}表数据文件:{2},写入目标数据库异常!{3}", dataLogFileDTO.getTaskId(), dataLogFileDTO.getKey(), dataLogFileDTO.getDataLogPath(), e.getMessage()));
            }
        }
    }

    @Override
    public void sinkToTarget(String taskId, String schemaName, String tableName, StringBuilder builder, String destSql, DatasourceDTO datasourceDTO) {
        log.info("任务Id:【{}】开始将{}.{}表的数据写入目标数据库.", taskId, schemaName, tableName);
        try (Connection connection = DataSourceFactory.INSTANCE.getDataSource(datasourceDTO, commonConfig).getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(destSql)) {
            connection.setAutoCommit(false);
            connection.commit();
            log.info("任务Id:【{}】完成将{}.{}表的数据写入目标数据库.", taskId, schemaName, tableName);
        } catch (SQLException e) {
            log.error("任务Id:【{}】将{}.{}表的数据写入目标数据库异常!", taskId, schemaName, tableName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String destinationSql(List<String> columnNameList, String schemaName, String tableName) {
        return MessageFormat.format(Constants.MySQLConstants.LOAD_DATA_MEMORY, commonConfig.getPrimaryKeyConflictOperator() == 1 ? "REPLACE" : "IGNORE", schemaName, tableName, getColumnNames(columnNameList));
    }

    @Override
    public List<CaptureTableDTO> searchAllCaptureTablesNoColumns(JdbcTemplate jdbcTemplate, String captureSchemaName, Map<String, CaptureTableDTO> includeTableMap, Map<String, CaptureTableDTO> excludeTableMap, String version) throws SQLException {
        return null;
    }

    @Override
    public void generatorCaptureTableAllColumns(String taskId, JdbcTemplate jdbcTemplate, CaptureTableDTO originCaptureTable, String migrateType, String version) throws SQLException {

    }

    @Override
    public Boolean supportSequence() {
        return false;
    }

    @Override
    public String autoIncrementScript(String tableName, String columnName) {
        return " AUTO_INCREMENT ";
    }

    @Override
    public String generatorTableComment(@NotNull CaptureTableDTO captureTableDTO) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotBlank(captureTableDTO.getComments())) {
            builder.append("alter table").append('\t').append(getSchema(captureTableDTO.getSchema())).append('.').append(getTableName(captureTableDTO.getName())).append('\t').append("comment").append('\t').append('\'').append(captureTableDTO.getComments()).append('\'').append(';');
        }
        return builder.toString();
    }

    @Override
    public String createTableScriptByMetadata(@NotNull CaptureTableDTO captureTableDTO, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) {
        StringBuilder builder = new StringBuilder();
        builder.append(Constants.DDL_CREATE_TABLE_KEY).append(' ');
        builder.append("IF NOT EXISTS").append(' ');
        String schemaAndTable = MessageFormat.format(Constants.SCHEMA_TABLE_KEY, getSchema(captureTableDTO.getSchema()), getTableName(captureTableDTO.getName()));
        builder.append(schemaAndTable).append('(').append('\n');
        List<String> pkList = captureTableDTO.getPrimaryKeyList();
        List<CaptureColumnDTO> columnMetadataList = captureTableDTO.getColumnMetaList();
        for (int i = 0, n = columnMetadataList.size(); i < n; i++) {
            CaptureColumnDTO columnMetadata = columnMetadataList.get(i);
            String name = columnMetadata.getName();
            String dataType = columnMetadata.getDataType().toUpperCase();
            if (!CollectionUtils.isEmpty(dataTypeMappingBeanMap)) {
                dataType = StringUtils.isBlank(Objects.nonNull(dataTypeMappingBeanMap.get(dataType)) ? dataTypeMappingBeanMap.get(dataType).getTargetName() : "") ? dataType : dataTypeMappingBeanMap.get(dataType).getTargetName();
            }
            builder.append(getColumnName(name)).append(' ');
            if (columnMetadata.getLength().compareTo(0) > 0) {
                if (StringUtils.equalsIgnoreCase("BYTEA", dataType)) {
                    builder.append(dataType);
                } else {
                    if (columnMetadata.getScale().compareTo(0) > 0) {
                        builder.append(dataType).append('(').append(columnMetadata.getLength()).append(',').append(columnMetadata.getScale()).append(')');
                    } else {
                        if (StringTools.equalsIgnoreCase(Constants.DECIMAL_TYPE_LIST, dataType)) {
                            builder.append(dataType).append('(').append(columnMetadata.getLength()).append(',').append(columnMetadata.getScale()).append(')');
                        } else {
                            if (!StringUtils.equalsIgnoreCase(columnMetadata.getDataTypeMapping(), "TIMESTAMP(0)")) {
                                builder.append(dataType).append('(').append(columnMetadata.getLength()).append(')');
                            } else {
                                builder.append(dataType);
                            }
                        }
                    }
                }
            } else {
                builder.append(dataType);
            }
            if (!columnMetadata.getNullable()) {
                builder.append(" NOT NULL ");
            }
            if (Objects.nonNull(columnMetadata.getIncrement()) && columnMetadata.getIncrement()) {
                builder.append(autoIncrementScript(captureTableDTO.getTargetName(), name));
            } else if (StringUtils.isNotBlank(columnMetadata.getDefaultValue())) {
                builder.append(" DEFAULT ").append(columnMetadata.getDefaultValue());
            }
            String columnComment = columnMetadata.getComment();
            if (StringUtils.isNotBlank(columnComment)) {
                builder.append(" COMMENT ").append('\'').append(columnComment).append('\'');
            }
            if (i == n - 1) {
                if (!CollectionUtils.isEmpty(pkList)) {
                    builder.append(',').append('\n');
                }
            } else {
                builder.append(',').append('\n');
            }
        }
        if (!CollectionUtils.isEmpty(pkList)) {
            String pkNames = getColumnNames(pkList);
            if (pkNames.length() > 0) {
                builder.append("PRIMARY KEY (").append(pkNames).append(')').append(',');
            }
        }
        if (builder.charAt(builder.length() - 1) == ',') {
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append(')');
        String tableComment = captureTableDTO.getComments();
        if (StringUtils.isNotBlank(tableComment)) {
            builder.append(" COMMENT ").append('\'').append(tableComment).append('\'');
        }
        builder.append(';');
        return builder.toString();
    }

    @Override
    public String generatorPartitionParameters(@NotNull List<SQLExpr> sqlExprList, List<SQLPartition> partitionList, String partitionType, String schemaAndTable) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, n = sqlExprList.size(); i < n; i++) {
            SQLExpr sqlExpr = sqlExprList.get(i);
            if (i == 0) {
                builder.append('(');
            }
            if (sqlExpr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
                builder.append(getColumnName(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", "")));
            } else if (sqlExpr instanceof SQLCharExpr) {
                SQLCharExpr sqlCharExpr = (SQLCharExpr) sqlExpr;
                builder.append('\'').append(getColumnName(StringUtils.replace(sqlCharExpr.getText(), "\"", ""))).append('\'');
            }
            if (i != n - 1) {
                builder.append(',');
            }
            if (i == n - 1) {
                builder.append(')');
            }
        }
        builder.append('\n');
        if (!CollectionUtils.isEmpty(partitionList)) {
            switch (partitionType) {
                case "HASH":
                    //TODO HASH类型分区表
                    break;
                case "LIST":
                    builder.append('(').append('\n');
                    for (int i = 0, n = partitionList.size(); i < n; i++) {
                        SQLPartition sqlPartition = partitionList.get(i);
                        builder.append('\t').append("PARTITION").append('\t').append(StringUtils.replace(partitionList.get(i).getName().getSimpleName(), "\"", "")).append('\t').append(StringUtils.replace(sqlPartition.getValues().toString(), "VALUES", "VALUES IN"));
                        if (i != n - 1) {
                            builder.append(',').append('\n');
                        }
                    }
                    builder.append('\n').append(')');
                    break;
                case "RANGE":
                    builder.append('(').append('\n');
                    for (int i = 0, n = partitionList.size(); i < n; i++) {
                        SQLPartition sqlPartition = partitionList.get(i);
                        List<SQLExpr> argumentList = ((SQLMethodInvokeExpr) sqlPartition.getValues().getItems().get(0)).getArguments();
                        if (!CollectionUtils.isEmpty(argumentList)) {
                            SQLExpr parameterExpr = argumentList.get(0);
                            if (parameterExpr instanceof SQLCharExpr) {
                                SQLCharExpr sqlCharExpr = (SQLCharExpr) parameterExpr;
                                builder.append('\t').append("PARTITION").append('\t').append(StringUtils.replace(sqlPartition.getName().getSimpleName(), "\"", "")).append('\t').append("values less than (").append('\'').append(sqlCharExpr.getText().trim()).append('\'').append(')');
                            }
                        }
                        if (i != n - 1) {
                            builder.append(',').append('\n');
                        }
                    }
                    builder.append('\n').append(')');
                    break;
                default:
                    break;
            }
        }
        return builder.toString();
    }

    @Override
    public DatabaseExpandType databaseType() {
        return DatabaseExpandType.MYSQL;
    }
}
