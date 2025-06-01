package org.dukejasun.migrate.service.sink.increment;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.OperationType;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.output.ActualDataDTO;
import org.dukejasun.migrate.model.dto.output.ColumnDataDTO;
import org.dukejasun.migrate.model.dto.output.OriginDataDTO;
import org.dukejasun.migrate.model.dto.output.TableDataDTO;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Component
public class IncrementDataProcessingService {

    /**
     * 处理增量数据生成目标可以执行脚本对象
     *
     * @param databaseFeatures  数据库特性
     * @param originDataDTOList 原始数据集合
     * @return 实际数据集合
     */
    public List<ActualDataDTO> generatorTargetScript(DatabaseFeatures databaseFeatures, @NotNull List<OriginDataDTO> originDataDTOList) throws Exception {
        List<ActualDataDTO> actualDataDTOList = Lists.newLinkedList();
        Map<TableDataDTO, ActualDataDTO> actualDataMap = Maps.newLinkedHashMap();
        List<OriginDataDTO> originDataListWithUniqueKey = originDataDTOList.stream().filter(originDataDTO -> !CollectionUtils.isEmpty(originDataDTO.getPkColumnName())).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(originDataListWithUniqueKey)) {
            for (OriginDataDTO originDataDTO : originDataListWithUniqueKey) {
                mergeByOperationType(originDataDTO, actualDataMap, databaseFeatures);
            }
            if (!CollectionUtils.isEmpty(actualDataMap)) {
                actualDataDTOList.addAll(Lists.newLinkedList(actualDataMap.values()));
            }
        }
        List<OriginDataDTO> originDataListWithoutUniqueKey = originDataDTOList.stream().filter(originDataDTO -> CollectionUtils.isEmpty(originDataDTO.getPkColumnName())).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(originDataListWithoutUniqueKey)) {
            for (OriginDataDTO originDataDTO : originDataListWithoutUniqueKey) {
                actualDataDTOList.add(generatorActualData(originDataDTO, databaseFeatures));
            }
        }
        return actualDataDTOList;
    }

    private @NotNull ActualDataDTO generatorActualData(@NotNull OriginDataDTO originDataDTO, DatabaseFeatures databaseFeatures) throws Exception {
        ActualDataDTO actualDataDTO = new ActualDataDTO();
        actualDataDTO.setTaskId(originDataDTO.getTaskId());
        actualDataDTO.setScn(originDataDTO.getScn().toString());

        List<String> pkNames = originDataDTO.getPkColumnName();
        actualDataDTO.setPkColumnNameList(pkNames);
        actualDataDTO.setHasKey(!CollectionUtils.isEmpty(pkNames));

        List<String> typeList = originDataDTO.getTypeList();
        LinkedHashMap<String, Object> after = originDataDTO.getAfter();
        LinkedHashMap<String, Object> before = originDataDTO.getBefore();
        Map<String, Integer> columnIndexMap = !CollectionUtils.isEmpty(after) ? convertMap(after) : !CollectionUtils.isEmpty(before) ? convertMap(before) : Maps.newHashMapWithExpectedSize(16);
        actualDataDTO.setAfter(getColumnDataList(after, typeList, columnIndexMap, databaseFeatures));
        actualDataDTO.setBefore(getBeforeColumnDataList(before, typeList, pkNames, columnIndexMap, databaseFeatures));
        actualDataDTO.setLinkedMap(before);
        actualDataDTO.setSchemaName(originDataDTO.getSchema());
        actualDataDTO.setTableName(originDataDTO.getTable());
        actualDataDTO.setTransactionalTime(originDataDTO.getTimestamp());

        String tableName = databaseFeatures.getTableName(originDataDTO.getTable());
        String schemaName = databaseFeatures.getSchema(originDataDTO.getSchema());
        String schemaAndName = MessageFormat.format(Constants.SCHEMA_TABLE_KEY, schemaName, tableName);
        actualDataDTO.setSchemaAndTable(schemaAndName);

        OperationType operation = originDataDTO.getType();
        actualDataDTO.setOperation(operation);
        switch (operation) {
            case INSERT:
                actualDataDTO.setScript(generatorInsertScript(after, schemaAndName, databaseFeatures));
                break;
            case DELETE:
                actualDataDTO.setScript(generatorDeleteScript(schemaAndName, before, pkNames, databaseFeatures));
                break;
            case UPDATE:
                actualDataDTO.setScript(generatorUpdateScript(schemaAndName, after, before, pkNames, databaseFeatures));
                break;
            case TRUNCATE:
                actualDataDTO.setHasKey(false);
                actualDataDTO.setScript(databaseFeatures.generatorTruncateScript(schemaName, tableName));
                break;
            default:
                break;
        }
        return actualDataDTO;
    }

    private @NotNull List<ColumnDataDTO> getBeforeColumnDataList(LinkedHashMap<String, Object> map, List<String> typeList, List<String> pkNames, Map<String, Integer> columnIndexMap, DatabaseFeatures databaseFeatures) throws Exception {
        if (CollectionUtils.isEmpty(map)) {
            return Lists.newArrayList();
        }
        List<ColumnDataDTO> columnDataDTOList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(pkNames)) {
            for (String key : map.keySet()) {
                columnDataDTOList.add(getColumnDataDTO(key, typeList, columnIndexMap, map, databaseFeatures));
            }
        } else {
            for (String key : pkNames) {
                columnDataDTOList.add(getColumnDataDTO(key, typeList, columnIndexMap, map, databaseFeatures));
            }
        }
        return columnDataDTOList;
    }


    private @NotNull ColumnDataDTO getColumnDataDTO(String key, List<String> typeList, @NotNull Map<String, Integer> columnIndexMap, LinkedHashMap<String, Object> map, DatabaseFeatures databaseFeatures) throws Exception {
        Integer index = columnIndexMap.get(key);
        if (Objects.isNull(index)) {
            throw new RuntimeException(MessageFormat.format("没有找到{0}列对应的索引!", key));
        }
        ColumnDataDTO columnDataDTO = new ColumnDataDTO();
        columnDataDTO.setName(databaseFeatures.getColumnName(key));
        columnDataDTO.setValue(map.get(key));
        columnDataDTO.setType(String.valueOf(typeList.get(index)));
        return columnDataDTO;
    }

    private @NotNull Map<String, Integer> convertMap(@NotNull LinkedHashMap<String, Object> linkedHashMap) {
        Map<String, Integer> columnIndexMap = Maps.newHashMapWithExpectedSize(16);
        Set<String> keySet = linkedHashMap.keySet();
        int index = 0;
        for (String key : keySet) {
            columnIndexMap.put(key, index);
            index += 1;
        }
        return columnIndexMap;
    }

    private @NotNull List<ColumnDataDTO> getColumnDataList(LinkedHashMap<String, Object> map, List<String> typeList, Map<String, Integer> columnIndexMap, DatabaseFeatures databaseFeatures) throws Exception {
        if (CollectionUtils.isEmpty(map)) {
            return Lists.newArrayList();
        }
        List<ColumnDataDTO> columnDataDTOList = Lists.newArrayList();
        for (String key : map.keySet()) {
            columnDataDTOList.add(getColumnDataDTO(key, typeList, columnIndexMap, map, databaseFeatures));
        }
        return columnDataDTOList;
    }

    private @Nullable String generatorInsertScript(LinkedHashMap<String, Object> after, String schemaAndName, DatabaseFeatures databaseFeatures) {
        if (CollectionUtils.isEmpty(after)) {
            return null;
        }
        StringBuffer columnNames = new StringBuffer();
        StringBuilder builder = new StringBuilder();
        int n = after.keySet().size();
        int index = 0;
        for (String key : after.keySet()) {
            key = databaseFeatures.getColumnName(key);
            if (index != n - 1) {
                builder.append('?').append(',');
                columnNames.append(key).append(',');
            } else {
                builder.append('?');
                columnNames.append(key);
            }
            index++;
        }
        String insertDml = MessageFormat.format(Constants.INSERT_DML, schemaAndName, columnNames, builder.toString());
        builder.setLength(0);
        return insertDml;
    }

    private @Nullable String generatorDeleteScript(String schemaAndName, LinkedHashMap<String, Object> before, List<String> pkNames, DatabaseFeatures databaseFeatures) {
        if (CollectionUtils.isEmpty(before)) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        builder.append("DELETE FROM ").append(schemaAndName);
        generatorWhereCondition(before, pkNames, builder, databaseFeatures);
        String deleteDml = builder.toString();
        builder.setLength(0);
        return deleteDml;
    }

    private @Nullable String generatorUpdateScript(String schemaAndName, LinkedHashMap<String, Object> after, LinkedHashMap<String, Object> before, List<String> pkNames, DatabaseFeatures databaseFeatures) {
        if (CollectionUtils.isEmpty(before) || CollectionUtils.isEmpty(after)) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ").append(schemaAndName).append(" SET ");
        int index = 0;
        int n = after.keySet().size();
        for (String key : after.keySet()) {
            key = databaseFeatures.getColumnName(key);
            if (index != n - 1) {
                builder.append(key).append('=').append('?').append(',');
            } else {
                builder.append(key).append('=').append('?');
            }
            index++;
        }
        generatorWhereCondition(before, pkNames, builder, databaseFeatures);
        String updateDml = builder.toString();
        builder.setLength(0);
        return updateDml;
    }

    private void generatorWhereCondition(LinkedHashMap<String, Object> before, List<String> pkNames, StringBuilder builder, DatabaseFeatures databaseFeatures) {
        if (CollectionUtils.isEmpty(before) && CollectionUtils.isEmpty(pkNames)) {
            return;
        }
        int index = 0;
        builder.append(" WHERE ");
        if (!CollectionUtils.isEmpty(pkNames)) {
            for (int i = 0, n = pkNames.size(); i < n; i++) {
                String pkName = databaseFeatures.getColumnName(pkNames.get(i));
                if (i == n - 1) {
                    builder.append(pkName).append("=?");
                    break;
                }
                builder.append(pkName).append(" =? AND ");
            }
        } else {
            int n = before.keySet().size();
            for (String key : before.keySet()) {
                if (index != n - 1) {
                    if (Objects.isNull(before.get(key))) {
                        builder.append(databaseFeatures.getColumnName(key)).append(" is null").append(" AND ");
                    } else {
                        builder.append(databaseFeatures.getColumnName(key)).append('=').append('?').append(" AND ");
                    }
                } else {
                    if (Objects.isNull(before.get(key))) {
                        builder.append(databaseFeatures.getColumnName(key)).append(" is null");
                    } else {
                        builder.append(databaseFeatures.getColumnName(key)).append('=').append('?');
                    }
                }
                index++;
            }
        }
    }

    /**
     * 按照数据类型进行merge
     *
     * @param originDataDTO
     * @param actualDataMap
     * @param databaseFeatures
     * @throws Exception
     */
    private void mergeByOperationType(@NotNull OriginDataDTO originDataDTO, Map<TableDataDTO, ActualDataDTO> actualDataMap, DatabaseFeatures databaseFeatures) throws Exception {
        OperationType type = originDataDTO.getType();
        switch (type) {
            case INSERT:
                TableDataDTO tableDataDTO = new TableDataDTO();
                tableDataDTO.setSchemaName(originDataDTO.getSchema()).setTableName(originDataDTO.getTable()).setColumnDataList(getColumnValueByKey(originDataDTO.getAfter(), originDataDTO.getPkColumnName()));
                ActualDataDTO actualDataDTO = actualDataMap.get(tableDataDTO);
                if (Objects.isNull(actualDataDTO)) {
                    actualDataMap.put(tableDataDTO, generatorActualData(originDataDTO, databaseFeatures));
                } else {
                    switch (actualDataDTO.getOperation()) {
                        case DELETE:
                            originDataDTO.setType(OperationType.INSERT);
                            break;
                        case UPDATE:
                            originDataDTO.setType(OperationType.UPDATE);
                            break;
                        case INSERT:
                        default:
                            break;
                    }
                    actualDataMap.put(tableDataDTO, generatorActualData(originDataDTO, databaseFeatures));
                }
                break;
            case DELETE:
                tableDataDTO = new TableDataDTO();
                tableDataDTO.setSchemaName(originDataDTO.getSchema()).setTableName(originDataDTO.getTable()).setColumnDataList(getColumnValueByKey(originDataDTO.getBefore(), originDataDTO.getPkColumnName()));
                actualDataDTO = actualDataMap.get(tableDataDTO);
                if (Objects.isNull(actualDataDTO)) {
                    actualDataMap.put(tableDataDTO, generatorActualData(originDataDTO, databaseFeatures));
                } else {
                    LinkedHashMap<String, Object> linkedMap = actualDataDTO.getLinkedMap();
                    actualDataMap.remove(tableDataDTO);
                    if (originDataDTO.getType().equals(OperationType.UPDATE)) {
                        originDataDTO.setBefore(linkedMap);
                        TableDataDTO tableBeforeData = new TableDataDTO(originDataDTO.getSchema(), originDataDTO.getTable(), getColumnValueByKey(originDataDTO.getBefore(), originDataDTO.getPkColumnName()));
                        actualDataMap.put(tableBeforeData, generatorActualData(originDataDTO, databaseFeatures));
                    }
                }
                break;
            case UPDATE:
                TableDataDTO tableAfterData = new TableDataDTO(originDataDTO.getSchema(), originDataDTO.getTable(), getColumnValueByKey(originDataDTO.getAfter(), originDataDTO.getPkColumnName()));
                TableDataDTO tableBeforeData = new TableDataDTO(originDataDTO.getSchema(), originDataDTO.getTable(), getColumnValueByKey(originDataDTO.getBefore(), originDataDTO.getPkColumnName()));
                ActualDataDTO beforeActualData = actualDataMap.get(tableBeforeData);
                if (Objects.isNull(beforeActualData)) {
                    actualDataMap.put(tableAfterData, generatorActualData(originDataDTO, databaseFeatures));
                } else {
                    OperationType operationType = beforeActualData.getOperation();
                    switch (operationType) {
                        case INSERT:
                            originDataDTO.setType(OperationType.INSERT);
                            actualDataMap.remove(tableBeforeData);
                            actualDataMap.put(tableAfterData, generatorActualData(originDataDTO, databaseFeatures));
                            break;
                        case DELETE:
                            actualDataMap.put(tableAfterData, generatorActualData(originDataDTO, databaseFeatures));
                            break;
                        case UPDATE:
                            LinkedHashMap<String, Object> linkedMap = beforeActualData.getLinkedMap();
                            actualDataMap.remove(tableBeforeData);
                            originDataDTO.setBefore(linkedMap);
                            actualDataMap.put(tableAfterData, generatorActualData(originDataDTO, databaseFeatures));
                            break;
                        default:
                            break;
                    }
                }
                break;
            case TRUNCATE:
                removeDataBySameKey(actualDataMap, originDataDTO.getSchema(), originDataDTO.getTable());
                tableDataDTO = new TableDataDTO(originDataDTO.getSchema(), originDataDTO.getTable(), null);
                actualDataMap.put(tableDataDTO, generatorActualData(originDataDTO, databaseFeatures));
                break;
            default:
                break;
        }
    }

    private void removeDataBySameKey(@NotNull Map<TableDataDTO, ActualDataDTO> actualDataMap, String schemaName, String tableName) {
        actualDataMap.keySet().removeIf(tableDataDTO -> tableDataDTO.getSchemaName().equals(schemaName) && tableDataDTO.getTableName().equals(tableName));
    }

    private @NotNull List<ColumnDataDTO> getColumnValueByKey(@NotNull LinkedHashMap<String, Object> linkedHashMap, List<String> pkNames) {
        List<ColumnDataDTO> columnValueList = Lists.newArrayList();
        linkedHashMap.forEach((key, value) -> {
            if (pkNames.contains(key)) {
                ColumnDataDTO columnValue = new ColumnDataDTO();
                columnValue.setName(key);
                columnValue.setValue(value);
                columnValueList.add(columnValue);
            }
        });
        return columnValueList;
    }
}
