package org.dukejasun.migrate.service.capture.total.impl;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.event.DbMigrateResultStatisticsDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureColumnDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.jdbc.LogPreparedStatement;
import org.dukejasun.migrate.queue.producer.DataProducer;
import org.dukejasun.migrate.utils.FileUtil;
import org.dukejasun.migrate.utils.HexConverter;
import org.dukejasun.migrate.utils.IDTools;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.driver.OracleConnection;
import oracle.sql.TIMESTAMPLTZ;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;
import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author dukedpsun
 */
@Slf4j
public class ReadAndWriteCommonService {

    protected void generatorData(List<String> columnNameList, List<CaptureColumnDTO> captureColumnDTOList, OracleConnection oracleConnection, AtomicLong rowCount, @NotNull ResultSetMetaData metaData, ResultSet resultSet, StringBuilder builder, String dataDelimiter, String encoding) throws SQLException, IOException {
        for (int i = 1, n = metaData.getColumnCount(); i <= n; i++) {
            if (columnNameList.contains(metaData.getColumnName(i))) {
                Object columnValue;
                int type = metaData.getColumnType(i);
                if (StringUtils.isNotBlank(encoding)) {
                    columnValue = getValueFromResultSet(resultSet, type, i, encoding);
                    if (Objects.nonNull(columnValue)) {
                        switch (type) {
                            case Types.BOOLEAN:
                            case Types.NUMERIC:
                            case Types.FLOAT:
                            case Types.REAL:
                            case Types.DOUBLE:
                            case Types.DECIMAL:
                            case Types.BIGINT:
                            case Types.INTEGER:
                            case Types.SMALLINT:
                            case Types.TINYINT:
                                break;
                            default:
                                columnValue = "'" + columnValue + "'";
                                break;
                        }
                    } else {
                        columnValue = "null";
                    }
                } else {
                    columnValue = getValueFromResultSet(resultSet, type, i);
                }
                if (i - 1 > 0) {
                    builder.append(dataDelimiter);
                }
                if (Objects.nonNull(columnValue)) {
                    String columnType = captureColumnDTOList.get(i - 1).getDataType();
                    if (columnValue instanceof byte[]) {
                        if (StringUtils.equalsIgnoreCase("BLOB", columnType)) {
                            builder.append(HexConverter.convertToHexString((byte[]) columnValue));
                        } else {
                            builder.append(columnValue);
                        }
                    } else if (columnValue instanceof TIMESTAMPLTZ) {
                        if (Objects.nonNull(oracleConnection)) {
                            builder.append(TIMESTAMPLTZ.toTimestamp(oracleConnection, ((TIMESTAMPLTZ) columnValue).toBytes()));
                        }
                    } else {
                        builder.append(columnValue);
                    }
                }
            }
        }
        rowCount.incrementAndGet();
    }

    private @Nullable Object getValueFromResultSet(ResultSet resultSet, int type, int index, String encoding) throws SQLException, IOException {
        switch (type) {
            case Types.BOOLEAN:
                return resultSet.getBoolean(index);
            case Types.NUMERIC:
            case Types.FLOAT:
            case Types.REAL:
                return resultSet.getDouble(index);
            case Types.DOUBLE:
                try {
                    return resultSet.getDouble(index);
                } catch (SQLException e) {
                    return new BigDecimal(resultSet.getString(index));
                }
            case Types.DECIMAL:
                Object object = resultSet.getBigDecimal(index);
                if (Objects.isNull(object)) {
                    return null;
                }
                return object;
            case Types.BIGINT:
                object = resultSet.getObject(index);
                if (object instanceof BigDecimal) {
                    return object;
                }
                return resultSet.getLong(index);
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return resultSet.getInt(index);
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
            case Types.BINARY:
                return resultSet.getBytes(index);
            case Types.BLOB:
                Blob blob = resultSet.getBlob(index);
                if (Objects.nonNull(blob)) {
                    return CharStreams.toString(new InputStreamReader(blob.getBinaryStream(), encoding));
                }
                return null;
            case Types.DATE:
                return resultSet.getDate(index);
            case Types.TIME:
                return resultSet.getTime(index);
            case Types.BIT:
                try {
                    object = resultSet.getObject(index);
                    if (object instanceof Boolean) {
                        return resultSet.getString(index);
                    }
                    return resultSet.getObject(index);
                } catch (SQLException e) {
                    object = resultSet.getString(index);
                    return object;
                }
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return resultSet.getTimestamp(index);
            case Types.CLOB:
            case Types.NCLOB:
                Clob clob = resultSet.getClob(index);
                if (Objects.nonNull(clob)) {
                    Reader reader = clob.getCharacterStream();
                    try (BufferedReader bufferedReader = new BufferedReader(reader)) {
                        StringBuilder buffer = new StringBuilder();
                        String temp;
                        while ((temp = bufferedReader.readLine()) != null) {
                            buffer.append(temp);
                        }
                        return buffer.toString();
                    }
                }
                return null;
            default:
                String value = resultSet.getString(index);
                if (StringUtils.isNotBlank(value)) {
                    if (StringUtils.isNotBlank(encoding)) {
                        return StringUtils.replace(new String(value.getBytes(encoding), encoding), "\n", " ");
                    }
                }
                return value;
        }
    }

    protected Object getValueFromResultSet(ResultSet resultSet, int type, int index) throws SQLException, IOException {
        switch (type) {
            case Types.BOOLEAN:
                return resultSet.getBoolean(index);
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return resultSet.getDouble(index);
            case Types.DECIMAL:
            case Types.NUMERIC:
                Object object = resultSet.getBigDecimal(index);
                if (Objects.isNull(object)) {
                    return BigDecimal.ZERO;
                }
                return object;
            case Types.BIGINT:
                object = resultSet.getObject(index);
                if (Objects.isNull(object)) {
                    return null;
                }
                if (object instanceof BigDecimal) {
                    return object;
                }
                return resultSet.getLong(index);
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return resultSet.getInt(index);
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
            case Types.BINARY:
                return resultSet.getBytes(index);
            case Types.BLOB:
                object = resultSet.getObject(index);
                if (Objects.isNull(object)) {
                    return null;
                }
                if (object instanceof String) {
                    SerialBlob serialBlob = new SerialBlob(((String) object).getBytes(Charsets.UTF_8));
                    return HexConverter.convertToHexString(HexConverter.blobToBytes(serialBlob));
                }
                return HexConverter.convertToHexString(HexConverter.blobToBytes((Blob) object));
            case Types.DATE:
                return resultSet.getDate(index);
            case Types.TIME:
                return resultSet.getTime(index);
            case Types.BIT:
                try {
                    object = resultSet.getObject(index);
                    if (Objects.isNull(object)) {
                        return null;
                    }
                    if (object instanceof Boolean) {
                        return resultSet.getString(index);
                    }
                    return resultSet.getObject(index);
                } catch (SQLException e) {
                    object = resultSet.getString(index);
                    return object;
                }
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return resultSet.getTimestamp(index);
            case Types.CLOB:
            case Types.NCLOB:
                Clob clob = resultSet.getClob(index);
                if (Objects.nonNull(clob)) {
                    return generatorClobData(clob);
                }
                return null;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            default:
                object = resultSet.getObject(index);
                if (Objects.isNull(object)) {
                    return null;
                }
                if (object instanceof String) {
                    String value = (String) object;
                    if (StringUtils.isNotBlank(value)) {
                        return new String(('\"' + StringUtils.replace(StringUtils.replace(StringUtils.replace(StringUtils.replace(value, "\"", "\'"), "\u0000", ""), "\\", "\\\\"), "\n", " ") + '\"').getBytes(Charsets.UTF_8), Charsets.UTF_8);
                    }
                } else if (object instanceof Blob) {
                    return HexConverter.convertToHexString(HexConverter.blobToBytes((Blob) object));
                } else if (object instanceof Clob) {
                    return generatorClobData((Clob) object);
                }
                return object;
        }
    }

    @Contract("_ -> new")
    private @NotNull Object generatorClobData(@NotNull Clob clob) throws SQLException, IOException {
        Reader reader = clob.getCharacterStream();
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
            StringBuilder builder = new StringBuilder();
            String temp;
            while ((temp = bufferedReader.readLine()) != null) {
                builder.append(temp).append('\n');
            }
            if (builder.length() > 0) {
                builder.deleteCharAt(builder.length() - 1);
            }
            return new String(('\"' + StringUtils.replace(StringUtils.replace(StringUtils.replace(StringUtils.replace(builder.toString(), "\"", "/\""), "\u0000", ""), "\\", "\\\\"), "\n", " ") + '\"').getBytes(Charsets.UTF_8), Charsets.UTF_8);
        }
    }

    protected void sendResult(String taskId, Long dataCount, @NotNull CaptureTableDTO captureTableDTO, Long transmissionCount, Boolean writeToFile, DataProducer dataProducer) {
        DbMigrateResultStatisticsDTO dbMigrateResultStatisticsDTO = new DbMigrateResultStatisticsDTO();
        dbMigrateResultStatisticsDTO.setTaskId(taskId);
        dbMigrateResultStatisticsDTO.setTransmissionType(MigrateType.TOTAL.getCode());
        dbMigrateResultStatisticsDTO.setSourceSchema(captureTableDTO.getSchema());
        dbMigrateResultStatisticsDTO.setSourceTable(captureTableDTO.getName());
        dbMigrateResultStatisticsDTO.setTargetSchema(captureTableDTO.getTargetSchema());
        dbMigrateResultStatisticsDTO.setTargetTable(captureTableDTO.getTargetName());
        dbMigrateResultStatisticsDTO.setSourceRecord(String.valueOf(dataCount));
        dbMigrateResultStatisticsDTO.setTransmissionResult(StatusEnum.FINISH.getCode());
        if (!writeToFile) {
            dbMigrateResultStatisticsDTO.setTargetRecord(String.valueOf(transmissionCount));
        }
        dbMigrateResultStatisticsDTO.setErrorReason(null);
        dbMigrateResultStatisticsDTO.setUpdateTime(LocalDateTime.now());
        dataProducer.put(dbMigrateResultStatisticsDTO);
    }

    protected void writeDataToTarget(String taskId, String schemaName, String tableName, @NotNull StringBuilder builder, String destSql, DatasourceDTO targetSourceConfig, @NotNull DatabaseFeatures databaseFeatures) throws SQLException, IOException {
        if (StringUtils.isNotBlank(builder.toString())) {
            builder.deleteCharAt(builder.length() - 1);
            databaseFeatures.sinkToTarget(taskId, schemaName, tableName, builder, destSql, targetSourceConfig);
        }
    }

    protected AtomicReference<File> createDataFile(String parentDir, String dataFileName, String uuid, Logger log) throws IOException {
        AtomicReference<File> originFile = null;
        if (Objects.nonNull(parentDir)) {
            originFile = new AtomicReference<>(createNewFile(parentDir, dataFileName + '@' + uuid, log));
        }
        return originFile;
    }

    protected File createNewFile(String parentDir, String schema, String tableName, Long totalFileThreshold, File originFile, Logger log) throws IOException {
        if (Objects.nonNull(originFile) && originFile.exists()) {
            if (FileUtils.sizeOf(originFile) >= totalFileThreshold) {
                renameFile(originFile, parentDir);
                originFile = createNewFile(parentDir, MessageFormat.format(Constants.SCHEMA_TABLE_KEY2, schema, tableName, String.valueOf(IDTools.getUUID())), log);
            }
        } else {
            originFile = createNewFile(parentDir, MessageFormat.format(Constants.SCHEMA_TABLE_KEY2, schema, tableName, String.valueOf(IDTools.getUUID())), log);
        }
        return originFile;
    }

    protected File createNewFile(String parentDir, String dataFileName, Logger log) throws IOException {
        File originFile = new File(parentDir + File.separator + dataFileName + Constants.TEMP_SUFFIX);
        if (!originFile.exists()) {
            originFile.createNewFile();
            log.info("生成{}表的新数据文件:{}", dataFileName.substring(0, dataFileName.lastIndexOf('@')), originFile.getAbsolutePath());
        }
        return originFile;
    }

    protected synchronized void renameFile(File originFile, String parentDir) {
        if (Objects.nonNull(originFile) && originFile.exists()) {
            File newFile = new File(parentDir + File.separator + originFile.getName().substring(0, originFile.getName().lastIndexOf(".")) + Constants.CSV_SUFFIX);
            try {
                Files.move(originFile, newFile);
            } catch (IOException e) {
                try {
                    TimeUnit.MILLISECONDS.sleep(300L);
                } catch (InterruptedException ignored) {
                }
                renameFile(originFile, parentDir);
            }
        } else {
            log.warn("数据文件不存在!");
        }
    }

    protected synchronized File writeDataToFile(@NotNull StringBuilder builder, File originFile, String parentDir, String schema, String tableName, String dataFileThreshold, Logger log) throws IOException {
        originFile = createNewFile(parentDir, schema, tableName, FileUtil.parse(dataFileThreshold), originFile, log);
        Files.append(builder.append('\n').toString(), originFile, Charsets.UTF_8);
        return originFile;
    }

    protected void writeToTarget(String taskId, String schemaName, String tableName, String destSql, OracleConnection oracleConnection, @NotNull CommonConfig commonConfig, CaptureTableDTO captureTableDTO, String parentDir, List<String> columnNameList, Integer commitSize, @NotNull ResultSet resultSet, AtomicReference<File> originFile, StringBuilder builder, AtomicLong rowCount, List<CaptureColumnDTO> captureColumnDTOList, DatasourceDTO targetSourceConfig, DatabaseFeatures databaseFeatures) {
        try {
            if (commonConfig.insertModel()) {
                if (Objects.nonNull(parentDir) && Objects.nonNull(originFile)) {
                    do {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        generatorData(columnNameList, captureColumnDTOList, oracleConnection, rowCount, metaData, resultSet, builder, commonConfig.getDataDelimiter(), databaseFeatures.getCharacterEncoding());
                        originFile.set(writeDataToFile(builder, originFile.get(), parentDir, captureTableDTO.getSchema(), captureTableDTO.getName(), commonConfig.getDataFileThreshold(), log));
                        builder.setLength(0);
                    } while (resultSet.next());
                } else {
                    DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(targetSourceConfig, commonConfig);
                    String encoding = databaseFeatures.getCharacterEncoding();
                    Connection connection = dataSource.getConnection();
                    LogPreparedStatement preparedStatement = new LogPreparedStatement(connection, destSql);
                    connection.setAutoCommit(false);
                    try {
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        do {
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                preparedStatement.setObject(i, getValueFromResultSet(resultSet, resultSetMetaData.getColumnType(i), i, encoding));
                            }
                            preparedStatement.addBatch();
                            log.debug(preparedStatement.toString());
                            if (rowCount.incrementAndGet() % commitSize == 0) {
                                int[] number = preparedStatement.executeBatch();
                                log.info("任务Id:【{}】{}.{}：提交{}条数据", taskId, schemaName, tableName, number.length);
                                preparedStatement.clearBatch();
                                connection.commit();
                            }
                        } while (resultSet.next());
                        int[] number = preparedStatement.executeBatch();
                        if (number.length > 0) {
                            log.info("任务Id:【{}】{}.{}：提交{}条数据", taskId, schemaName, tableName, number.length);
                        }
                        connection.commit();
                    } catch (SQLException | IOException e) {
                        log.error("任务Id:【{}】{}.{}插入数据异常内容:{}", taskId, schemaName, tableName, e.getMessage());
                        throw new RuntimeException(databaseFeatures.databaseType().getType() + "插入数据异常内容:", e);
                    } finally {
                        preparedStatement.close();
                        connection.close();
                    }
                }
            } else {
                do {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    if (Objects.nonNull(parentDir) && Objects.nonNull(originFile)) {
                        generatorData(columnNameList, captureColumnDTOList, oracleConnection, rowCount, metaData, resultSet, builder, commonConfig.getDataDelimiter(), null);
                        originFile.set(writeDataToFile(builder, originFile.get(), parentDir, captureTableDTO.getSchema(), captureTableDTO.getName(), commonConfig.getDataFileThreshold(), log));
                        builder.setLength(0);
                    } else {
                        generatorData(columnNameList, captureColumnDTOList, oracleConnection, rowCount, metaData, resultSet, builder, commonConfig.getDataDelimiter(), null);
                        builder.append('\n');
                        if (rowCount.get() % commitSize == 0) {
                            writeDataToTarget(taskId, schemaName, tableName, builder, destSql, targetSourceConfig, databaseFeatures);
                        }
                    }
                } while (resultSet.next());
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
