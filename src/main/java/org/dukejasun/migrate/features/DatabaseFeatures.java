package org.dukejasun.migrate.features;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLPartition;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.lmax.disruptor.dsl.Disruptor;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.handler.DbMigrateResultStatisticsHandler;
import org.dukejasun.migrate.handler.MigrateWorkHandler;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.model.dto.event.DbMigrateResultStatisticsDTO;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.entity.DbMigrateResultStatistics;
import org.dukejasun.migrate.queue.event.ReplicatorEvent;
import org.dukejasun.migrate.queue.factory.DisruptorFactory;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author dukedpsun
 */
public interface DatabaseFeatures {

    /**
     * 模式名称转换
     *
     * @param schema
     * @return
     */
    default String getSchema(String schema) {
        return convert(schema);
    }

    /**
     * 表名称转换
     *
     * @param tableName
     * @return
     */
    default String getTableName(String tableName) {
        return convert(tableName);
    }

    /**
     * 列名
     *
     * @param columnName
     * @return
     */
    default String getColumnName(String columnName) {
        return convert(columnName);
    }

    default String getColumnNames(List<String> columnNameList) {
        StringBuilder columnValues = new StringBuilder();
        for (Iterator<String> iterator = columnNameList.iterator(); iterator.hasNext(); ) {
            String columnName = iterator.next();
            columnValues.append(getColumnName(columnName));
            if (iterator.hasNext()) {
                columnValues.append(',');
            }
        }
        return columnValues.toString();
    }

    /**
     * @param databaseObject
     * @return
     */
    default String getOtherObjectName(String databaseObject) {
        return convert(databaseObject);
    }

    /**
     * 获取数据库的版本
     *
     * @param jdbcTemplate
     * @return
     */
    String getDatabaseVersion(JdbcTemplate jdbcTemplate);

    String getCharacterEncoding();

    /**
     * 转换方式
     *
     * @param name
     * @return
     */
    String convert(String name);

    /**
     * csv写数据到目标
     *
     * @param dataLogFileDTO
     * @throws IOException
     */
    void sinkToTarget(DataLogFileDTO dataLogFileDTO) throws IOException;

    /**
     * 内存方式写入目标
     *
     * @param taskId
     * @param schemaName
     * @param tableName
     * @param builder
     * @param destSql
     * @param targetSourceConfig
     * @throws SQLException
     */
    void sinkToTarget(String taskId, String schemaName, String tableName, StringBuilder builder, String destSql, DatasourceDTO targetSourceConfig) throws SQLException;

    /**
     * 目标SQL
     *
     * @param columnNameList
     * @param schemaName
     * @param tableName
     * @return
     */
    String destinationSql(List<String> columnNameList, String schemaName, String tableName);

    List<CaptureTableDTO> searchAllCaptureTablesNoColumns(JdbcTemplate jdbcTemplate, String captureSchemaName, Map<String, CaptureTableDTO> includeTableMap, Map<String, CaptureTableDTO> excludeTableMap, String version) throws SQLException;

    void generatorCaptureTableAllColumns(String taskId, JdbcTemplate jdbcTemplate, CaptureTableDTO originCaptureTable, String migrateType, String version) throws SQLException;

    Boolean supportSequence();

    String autoIncrementScript(String tableName, String columnName);

    String generatorTableComment(CaptureTableDTO captureTableDTO);

    String createTableScriptByMetadata(CaptureTableDTO captureTableDTO, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap);

    String generatorPartitionParameters(@NotNull List<SQLExpr> sqlExprList, List<SQLPartition> partitionList, String partitionType, String schemaAndTable);

    DatabaseExpandType databaseType();

    default DataProducer generatorDataProducer(int bufferSize, MigrateResultsStatisticsService migrateResultsStatisticsService) {
        Disruptor<ReplicatorEvent> disruptor = DisruptorFactory.INSTANCE.getDisruptor(bufferSize);
        MigrateWorkHandler migrateWorkHandler = new MigrateWorkHandler();
        migrateWorkHandler.setMigrateResultsStatisticsService(migrateResultsStatisticsService);
        return new DataProducer(disruptor, migrateWorkHandler);
    }

    default void sendResult(String taskId, @NotNull CaptureTableDTO captureTableDTO, Long dataSize, String errorMessage, @NotNull DataProducer dataProducer) {
        DbMigrateResultStatisticsDTO dbMigrateResultStatisticsDTO = new DbMigrateResultStatisticsDTO();
        dbMigrateResultStatisticsDTO.setTaskId(taskId);
        dbMigrateResultStatisticsDTO.setTransmissionType(MigrateType.TOTAL.getCode());
        dbMigrateResultStatisticsDTO.setSourceSchema(captureTableDTO.getSchema());
        dbMigrateResultStatisticsDTO.setSourceTable(captureTableDTO.getName());
        dbMigrateResultStatisticsDTO.setTargetSchema(captureTableDTO.getTargetSchema());
        dbMigrateResultStatisticsDTO.setTargetTable(captureTableDTO.getTargetName());
        dbMigrateResultStatisticsDTO.setCreateTime(LocalDateTime.now());
        dbMigrateResultStatisticsDTO.setTargetRecord(String.valueOf(dataSize));
        if (Objects.nonNull(errorMessage)) {
            dbMigrateResultStatisticsDTO.setTransmissionResult(StatusEnum.ERROR.getCode());
            dbMigrateResultStatisticsDTO.setErrorReason(errorMessage);
        } else {
            dbMigrateResultStatisticsDTO.setTransmissionResult(StatusEnum.FINISH.getCode());
            dbMigrateResultStatisticsDTO.setErrorReason("");
        }
        dbMigrateResultStatisticsDTO.setUpdateTime(LocalDateTime.now());
        dataProducer.put(dbMigrateResultStatisticsDTO);
    }

    /**
     * truncate 目标表
     *
     * @param taskId
     * @param captureTableDTOList
     * @param datasourceDTO
     * @param commonConfig
     * @param dbMigrateResultStatisticsHandler
     * @return
     */
    default Boolean truncateData(String taskId, List<CaptureTableDTO> captureTableDTOList, DatasourceDTO datasourceDTO, CommonConfig commonConfig, @NotNull DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler) {
        DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(datasourceDTO, commonConfig);
        if (Objects.nonNull(dataSource)) {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            for (CaptureTableDTO captureTableDTO : captureTableDTOList) {
                truncateData(jdbcTemplate, captureTableDTO.getTargetSchema(), captureTableDTO.getTargetName());
            }
            List<DbMigrateResultStatistics> dbMigrateResultStatisticsList = dbMigrateResultStatisticsHandler.findAllByTaskIdAndTransmissionType(taskId, MigrateType.TOTAL.getCode());
            if (!CollectionUtils.isEmpty(dbMigrateResultStatisticsList)) {
                dbMigrateResultStatisticsList.forEach(dbMigrateResultStatistics -> dbMigrateResultStatistics.setTargetRecord("0"));
                dbMigrateResultStatisticsHandler.batchUpdateStatistics(dbMigrateResultStatisticsList);
            }
        }
        return true;
    }

    /**
     * 清除目标表
     *
     * @param jdbcTemplate
     * @param schema
     * @param tableName
     * @return
     */
    default Boolean truncateData(JdbcTemplate jdbcTemplate, String schema, String tableName) {
        try {
            jdbcTemplate.execute(MessageFormat.format(Constants.TRUNCATE_TABLE, getSchema(schema), getTableName(tableName)));
        } catch (Exception ignored) {
            return false;
        }
        return true;
    }

    default CaptureTableDTO getIgnoreCaptureTable(String schema, String name, Map<String, CaptureTableDTO> captureTableMap) {
        return !CollectionUtils.isEmpty(captureTableMap) ? captureTableMap.get(MessageFormat.format(Constants.SCHEMA_TABLE_KEY, schema, name)) : null;
    }

    default String generatorTruncateScript(String schema, String tableName) {
        return MessageFormat.format(Constants.TRUNCATE_TABLE, schema, tableName);
    }

    default void sinkToTarget(@NotNull DataLogFileDTO dataLogFileDTO, @NotNull final String destSql, CaptureTableDTO captureTableDTO, DatasourceDTO datasourceDTO, CommonConfig commonConfig, DataProducer dataProducer) throws IOException {
        String dataPath = dataLogFileDTO.getDataLogPath();
        if (new File(dataPath).exists()) {
            DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(datasourceDTO, commonConfig);
            final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            Files.asCharSource(new File(dataPath), Charset.defaultCharset()).readLines(new LineProcessor<String>() {
                final List<String> list = Lists.newLinkedList();

                @Override
                public boolean processLine(@NotNull String line) {
                    if (StringUtils.isNotBlank(line) && line.trim().getBytes()[0] != '#' && line.trim().getBytes()[0] != '\\') {
                        list.add('(' + line + ')');
                    }
                    return true;
                }

                @Override
                public String getResult() {
                    if (!CollectionUtils.isEmpty(list)) {
                        StringBuilder builder = new StringBuilder();
                        Lists.partition(list, commonConfig.getCommitSize()).forEach(valueList -> {
                            builder.append(destSql).append(String.join(",", valueList));
                            jdbcTemplate.execute(builder.toString());
                            builder.setLength(0);
                        });
                        sendResult(dataLogFileDTO.getTaskId(), captureTableDTO, (long) list.size(), null, dataProducer);
                        captureTableDTO.getSinkLongAddr().increment();
                    }
                    return null;
                }
            });
        }
    }
}
