package org.dukejasun.migrate.features.impl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLPartition;
import com.google.common.collect.Lists;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.DataLogFileDTO;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureColumnDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.utils.StringTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("ORACLE_FEATURES")
public class OracleFeatures implements DatabaseFeatures {
    private final CommonConfig commonConfig;

    @Autowired
    public OracleFeatures(CommonConfig commonConfig) {
        this.commonConfig = commonConfig;
    }

    @Override
    public String getDatabaseVersion(@NotNull JdbcTemplate jdbcTemplate) {
        return jdbcTemplate.query(IConstants.QUERY_VERSION, resultSet -> {
            if (resultSet.next()) {
                do {
                    return resultSet.getString(1);
                } while (resultSet.next());
            }
            return "1.0";
        });
    }

    @Override
    public String getCharacterEncoding() {
        return commonConfig.getOracleCharacterEncoding();
    }

    public String convert(String name) {
        if (StringUtils.containsIgnoreCase(commonConfig.getDataTypeMappingKey(), "oracle2")) {
            return '\"' + name + '\"';
        } else {
            int nameResolution = Objects.nonNull(commonConfig.getNameResolution()) ? commonConfig.getNameResolution() : 0;
            switch (nameResolution) {
                case 1:
                    return '\"' + name.toUpperCase() + '\"';
                case 2:
                    return '\"' + name.toLowerCase() + '\"';
                default:
                    return '\"' + name + '\"';
            }
        }
    }

    @Override
    public void sinkToTarget(DataLogFileDTO dataLogFileDTO) {
    }

    @Override
    public void sinkToTarget(String taskId, String schemaName, String tableName, StringBuilder builder, String destSql, DatasourceDTO targetSourceConfig) throws SQLException {
    }

    @Override
    public String destinationSql(List<String> columnNameList, String schemaName, String tableName) {
        return null;
    }

    @Override
    public List<CaptureTableDTO> searchAllCaptureTablesNoColumns(@NotNull JdbcTemplate jdbcTemplate, String captureSchemaName, Map<String, CaptureTableDTO> includeTableMap, Map<String, CaptureTableDTO> excludeTableMap, String version) {
        List<CaptureTableDTO> captureTableDTOList = Lists.newArrayList();
        jdbcTemplate.query(MessageFormat.format(Constants.OracleConstants.ALL_TABLE_METADATA, captureSchemaName), resultSet -> {
            do {
                String schema = resultSet.getString(1);
                String name = resultSet.getString(2);
                if (!CollectionUtils.isEmpty(includeTableMap)) {
                    CaptureTableDTO includeTableDTO = getIgnoreCaptureTable(schema, name, includeTableMap);
                    if (Objects.nonNull(includeTableDTO)) {
                        CaptureTableDTO captureTableDTO = new CaptureTableDTO();
                        captureTableDTO.setSchema(schema);
                        captureTableDTO.setName(name);
                        captureTableDTO.setComments(resultSet.getString(3));
                        captureTableDTO.setTablespaceName(resultSet.getString(4));
                        captureTableDTO.setLongAdder(new LongAdder());
                        captureTableDTO.setCaptureLongAddr(new LongAdder());
                        captureTableDTO.setSinkLongAddr(new LongAdder());
                        captureTableDTO.setTargetSchema(StringUtils.isNotBlank(includeTableDTO.getTargetSchema()) ? includeTableDTO.getTargetSchema() : schema);
                        captureTableDTO.setTargetName(StringUtils.isNotBlank(includeTableDTO.getTargetName()) ? includeTableDTO.getTargetName() : name);
                        captureTableDTO.setFlashBack(includeTableDTO.getFlashBack());
                        captureTableDTO.setConditions(includeTableDTO.getConditions());
                        captureTableDTOList.add(captureTableDTO);
                    }
                } else {
                    CaptureTableDTO excludeTableDTO = getIgnoreCaptureTable(schema, name, excludeTableMap);
                    if (Objects.isNull(excludeTableDTO)) {
                        CaptureTableDTO captureTableDTO = new CaptureTableDTO();
                        captureTableDTO.setSchema(schema);
                        captureTableDTO.setName(name);
                        captureTableDTO.setComments(resultSet.getString(3));
                        captureTableDTO.setTablespaceName(resultSet.getString(4));
                        captureTableDTO.setLongAdder(new LongAdder());
                        captureTableDTO.setCaptureLongAddr(new LongAdder());
                        captureTableDTO.setSinkLongAddr(new LongAdder());
                        captureTableDTO.setTargetSchema(schema);
                        captureTableDTO.setTargetName(name);
                        captureTableDTOList.add(captureTableDTO);
                    }
                }
            } while (resultSet.next());
        });
        return captureTableDTOList;
    }

    @Override
    public void generatorCaptureTableAllColumns(String taskId, JdbcTemplate jdbcTemplate, CaptureTableDTO originCaptureTable, String migrateTypeCode, String version) {
        try {
            List<CaptureColumnDTO> captureColumnDTOList = Lists.newArrayList();
            String query = Constants.OracleConstants.ALL_COLUMN_METADATA_ORACLE;
            if (StringUtils.isNotBlank(version.substring(0, version.indexOf(".")))) {
                version = version.substring(0, version.indexOf("."));
                if (Integer.parseInt(version) < 12) {
                    query = Constants.OracleConstants.ALL_COLUMN_METADATA_ORACLE_2;
                }
            }
            jdbcTemplate.query(query, resultSet -> {
                do {
                    CaptureColumnDTO captureColumnDTO = new CaptureColumnDTO();
                    String schemaName = resultSet.getString(2);
                    String tableName = resultSet.getString(3);
                    String dataType = resultSet.getString(5);
                    captureColumnDTO.setColumnNo(resultSet.getInt(1))
                            .setTableSchema(schemaName)
                            .setTableName(tableName)
                            .setName(resultSet.getString(4))
                            .setDataType(dataType)
                            .setLength(resultSet.getInt(6))
                            .setScale(resultSet.getInt(7))
                            .setPrimaryKey(resultSet.getInt(8) != 0)
                            .setUniqueKey(resultSet.getInt(9) != 0)
                            .setNullable(resultSet.getInt(10) == 1)
                            .setDefaultValue(resultSet.getString(11))
                            .setVirtualColumn(!"NO".equalsIgnoreCase(resultSet.getString(12)))
                            .setComment(resultSet.getString(13))
                            .setDataTypeMapping(StringTools.replaceAll(dataType, " ", " ", commonConfig.getDataTypeMappingKey()));
                    captureColumnDTOList.add(captureColumnDTO);
                } while (resultSet.next());
            }, originCaptureTable.getSchema(), originCaptureTable.getName());
            originCaptureTable.setColumnMetaList(captureColumnDTOList);
            List<String> pkList = captureColumnDTOList.stream().map(columnMetadata -> columnMetadata.getPrimaryKey() ? columnMetadata.getName() : null).collect(Collectors.toList()).stream().filter(Objects::nonNull).collect(Collectors.toList());
            originCaptureTable.setPrimaryKeyList(pkList);
            originCaptureTable.setDataTypeList(captureColumnDTOList.stream().map(CaptureColumnDTO::getDataTypeMapping).collect(Collectors.toList()));
            MigrateType migrateType = MigrateType.getMigrateType(migrateTypeCode);
            switch (migrateType) {
                case STRUCTURE:
                    //索引
                    if (commonConfig.getStructureContainsIndex()) {
                        jdbcTemplate.query(Constants.OracleConstants.QUERY_INDEX_NAMES, resultSet -> {
                            do {
                                originCaptureTable.addIndexNameList(resultSet.getString(1));
                            } while (resultSet.next());
                        }, originCaptureTable.getName(), originCaptureTable.getSchema(), originCaptureTable.getSchema());
                    }
                    break;
                case TOTAL:
                    //分区
                    jdbcTemplate.query(Constants.OracleConstants.QUERY_PARTITION_NAMES, resultSet -> {
                        do {
                            originCaptureTable.addPartitionNameList(resultSet.getString(1));
                        } while (resultSet.next());
                    }, originCaptureTable.getName(), originCaptureTable.getSchema());
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("获取{}.{}表的元数据失败!{}", originCaptureTable.getSchema(), originCaptureTable.getName(), e.getMessage());
        }
    }

    @Override
    public Boolean supportSequence() {
        return true;
    }

    @Override
    public String autoIncrementScript(String tableName, String columnName) {
        return " DEFAULT " + "seq_" + tableName + ".nextval ";
    }

    @Override
    public String generatorTableComment(CaptureTableDTO captureTableDTO) {
        return null;
    }

    @Override
    public String createTableScriptByMetadata(CaptureTableDTO captureTableDTO, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) {
        return null;
    }

    @Override
    public String generatorPartitionParameters(@NotNull List<SQLExpr> sqlExprList, List<SQLPartition> partitionList, String partitionType, String schemaAndTable) {
        return null;
    }

    @Override
    public DatabaseExpandType databaseType() {
        return DatabaseExpandType.ORACLE;
    }
}
