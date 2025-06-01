package org.dukejasun.migrate.service.analyse.script.impl;

import com.alibaba.druid.sql.SQLTransformUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.ast.OracleDataTypeIntervalDay;
import com.alibaba.druid.sql.dialect.oracle.ast.expr.OracleSysdateExpr;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleCheck;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleCreateIndexStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleForeignKey;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.util.JdbcConstants;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.connection.OracleConnection;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.AnalyseResultDTO;
import org.dukejasun.migrate.service.analyse.script.AnalyseDatabaseScriptService;
import org.dukejasun.migrate.utils.IDTools;
import org.dukejasun.migrate.utils.StringTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.util.CollectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * @author dukedpsun
 */
@Slf4j
public class AnalyseOracleScriptService implements AnalyseDatabaseScriptService {

    public AnalyseOracleScriptService() {

    }

    @Override
    public AnalyseResultDTO analyseTableAndConvert(@NotNull CaptureTableDTO captureTableDTO, @NotNull DatasourceDTO dataSourceDTO, @NotNull DatabaseFeatures databaseFeatures, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) throws SQLException {
        AnalyseResultDTO analyseResultDTO = new AnalyseResultDTO();
        String url = MessageFormat.format(DatabaseExpandType.ORACLE.getUrlTemplate(), dataSourceDTO.getHost(), String.valueOf(dataSourceDTO.getPort()), dataSourceDTO.getDatabaseName());
        try (OracleConnection oracleConnection = new OracleConnection(url, dataSourceDTO.getUsername(), dataSourceDTO.getPassword())) {
            oracleConnection.executeWithoutCommitting(IConstants.TABLE_META_CONTAINS_CONSTRAINTS);
            oracleConnection.executeWithoutCommitting(IConstants.TABLE_META_CONTAINS_CONSTRAINTS_AS_ALTER);
            oracleConnection.executeWithoutCommitting(IConstants.TABLE_META_CONTAINS_REF_CONSTRAINTS);
            oracleConnection.executeWithoutCommitting(IConstants.TABLE_META_CONTAINS_SEGMENT_ATTRIBUTES);
            oracleConnection.executeWithoutCommitting(IConstants.TABLE_META_CONTAINS_SQLTERMINATOR);
            oracleConnection.executeWithoutCommitting(IConstants.TABLE_META_CONTAINS_STORAGE);
            oracleConnection.executeWithoutCommitting(IConstants.TABLE_META_CONTAINS_TABLESPACE);
            StringBuffer buffer = new StringBuffer();
            StringBuilder builderFk = new StringBuilder();
            try {
                String tableDDL = getTableScript(oracleConnection, captureTableDTO);
                analyseResultDTO.setOriginScript(tableDDL);
                List<SQLStatement> sqlStatementList = SQLUtils.parseStatements(tableDDL, JdbcConstants.ORACLE.name());
                for (SQLStatement sqlStatement : sqlStatementList) {
                    if (sqlStatement instanceof SQLCreateTableStatement) {
                        generatorCreateTableObject(sqlStatement, databaseFeatures, captureTableDTO, dataTypeMappingBeanMap, buffer);
                    } else if (sqlStatement instanceof SQLAlterTableStatement) {
                        generatorAlterTableObject(sqlStatement, buffer, databaseFeatures, captureTableDTO, dataTypeMappingBeanMap, builderFk);
                    }
                }
                generatorIndexObject(oracleConnection, captureTableDTO, databaseFeatures, buffer);
                buffer.append('\n').append(databaseFeatures.generatorTableComment(captureTableDTO));
                analyseResultDTO.setConvertScript(buffer.toString());
                analyseResultDTO.setFkScript(builderFk.toString());
            } catch (ParserException e) {
                buffer.append(databaseFeatures.createTableScriptByMetadata(captureTableDTO, dataTypeMappingBeanMap));
                generatorIndexObject(oracleConnection, captureTableDTO, databaseFeatures, buffer);
                analyseResultDTO.setConvertScript(buffer.toString());
            }
        }
        return analyseResultDTO;
    }

    private void generatorIndexObject(OracleConnection oracleConnection, @NotNull CaptureTableDTO captureTableDTO, DatabaseFeatures databaseFeatures, StringBuffer buffer) {
        List<String> indexNameList = captureTableDTO.getIndexNameList();
        if (!CollectionUtils.isEmpty(indexNameList)) {
            for (String indexName : indexNameList) {
                try (PreparedStatement statement = oracleConnection.connection().prepareStatement(IConstants.QUERY_INDEX_META, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
                    statement.setString(1, indexName);
                    statement.setString(2, captureTableDTO.getSchema());
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            do {
                                Clob clob = resultSet.getClob(1);
                                Reader reader = clob.getCharacterStream();
                                try (BufferedReader bufferedReader = new BufferedReader(reader)) {
                                    StringBuilder indexBuffer = new StringBuilder();
                                    String temp;
                                    while ((temp = bufferedReader.readLine()) != null) {
                                        indexBuffer.append(temp);
                                    }
                                    List<SQLStatement> sqlStatementList = SQLUtils.parseStatements(indexBuffer.toString(), JdbcConstants.ORACLE.name());
                                    for (SQLStatement sqlStatement : sqlStatementList) {
                                        if (sqlStatement instanceof OracleCreateIndexStatement) {
                                            OracleCreateIndexStatement oracleCreateIndexStatement = (OracleCreateIndexStatement) sqlStatement;
                                            SQLIndexDefinition sqlIndexDefinition = oracleCreateIndexStatement.getIndexDefinition();
                                            List<SQLSelectOrderByItem> sqlSelectOrderByItemList = sqlIndexDefinition.getColumns();
                                            if (!CollectionUtils.isEmpty(sqlSelectOrderByItemList)) {
                                                buffer.append('\n').append(Constants.DDL_CREATE_INDEX_KEY).append(' ').append(databaseFeatures.getOtherObjectName(indexName)).append(' ');
                                                buffer.append("ON").append(' ').append(databaseFeatures.getSchema(captureTableDTO.getTargetSchema())).append('.').append(databaseFeatures.getTableName(captureTableDTO.getTargetName())).append(' ');
                                                generatorParameter(sqlSelectOrderByItemList, databaseFeatures, buffer);
                                                buffer.append(';').append('\n');
                                            }
                                        }
                                    }
                                }
                            } while (resultSet.next());
                        }
                    }
                } catch (SQLException | IOException e) {
                    throw new RuntimeException(MessageFormat.format("{0}.{1}解析索引数据异常!{2}", captureTableDTO.getSchema(), indexName, e.getMessage()));
                }
            }
        }
    }

    private void generatorAlterTableObject(SQLStatement sqlStatement, StringBuffer buffer, DatabaseFeatures databaseFeatures, CaptureTableDTO captureTableDTO, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap, StringBuilder builderFk) {
        SQLAlterTableStatement sqlAlterTableStatement = (SQLAlterTableStatement) sqlStatement;
        List<SQLAlterTableItem> sqlAlterTableItemList = sqlAlterTableStatement.getItems();
        sqlAlterTableItemList.forEach(sqlAlterTableItem -> {
            if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                buffer.append('\n').append(Constants.DDL_ALTER_TABLE_KEY).append(' ').append(databaseFeatures.getSchema(captureTableDTO.getSchema())).append('.').append(databaseFeatures.getTableName(captureTableDTO.getName())).append(' ');
                settingAddColumnSql(buffer, captureTableDTO, sqlAlterTableItem, databaseFeatures, dataTypeMappingBeanMap);
                buffer.append(';').append('\n');
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddConstraint) {
                SQLAlterTableAddConstraint sqlAlterTableAddConstraint = (SQLAlterTableAddConstraint) sqlAlterTableItem;
                SQLConstraint sqlConstraint = sqlAlterTableAddConstraint.getConstraint();
                if (sqlConstraint instanceof SQLPrimaryKeyImpl) {
                    buffer.append('\n').append(Constants.DDL_ALTER_TABLE_KEY).append(' ').append(databaseFeatures.getSchema(captureTableDTO.getSchema())).append('.').append(databaseFeatures.getTableName(captureTableDTO.getName())).append(' ');
                    SQLPrimaryKeyImpl sqlPrimaryKey = (SQLPrimaryKeyImpl) sqlConstraint;
                    generatorParameter(sqlPrimaryKey.getColumns(), databaseFeatures, buffer.append("ADD ").append(' ').append("PRIMARY KEY").append(' '));
                    buffer.append(';').append('\n');
                } else if (sqlConstraint instanceof SQLUnique) {
                    buffer.append('\n').append(Constants.DDL_ALTER_TABLE_KEY).append(' ').append(databaseFeatures.getSchema(captureTableDTO.getSchema())).append('.').append(databaseFeatures.getTableName(captureTableDTO.getName())).append(' ');
                    SQLUnique sqlUnique = (SQLUnique) sqlConstraint;
                    settingUniqueConstraint(buffer, sqlUnique.getName(), sqlUnique.getColumns(), databaseFeatures);
                    buffer.append(';').append('\n');
                } else if (sqlConstraint instanceof OracleCheck) {
                    buffer.append('\n').append(Constants.DDL_ALTER_TABLE_KEY).append(' ').append(databaseFeatures.getSchema(captureTableDTO.getSchema())).append('.').append(databaseFeatures.getTableName(captureTableDTO.getName())).append(' ');
                    OracleCheck oracleCheck = (OracleCheck) sqlConstraint;
                    String checkConstraintName;
                    if (Objects.nonNull(oracleCheck.getName())) {
                        checkConstraintName = oracleCheck.getName().getSimpleName();
                    } else {
                        checkConstraintName = 'c' + String.valueOf(Math.abs(IDTools.getUUID()));
                    }
                    buffer.append("ADD ").append("CONSTRAINT").append(' ').append(StringUtils.replace(checkConstraintName, "\"", "")).append(" CHECK ").append('(');
                    SQLExpr sqlExpr = oracleCheck.getExpr();
                    if (sqlExpr instanceof SQLBinaryOpExpr) {
                        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
                        generatorComplexParameter(sqlBinaryOpExpr, databaseFeatures, buffer);
                        buffer.append(')');
                        buffer.append(';').append('\n');
                    } else if (sqlExpr instanceof SQLBetweenExpr) {
                        SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) sqlExpr;
                        SQLExpr testExpr = sqlBetweenExpr.getTestExpr();
                        SQLExpr beginExpr = sqlBetweenExpr.getBeginExpr();
                        SQLExpr endExpr = sqlBetweenExpr.getEndExpr();
                        buffer.append(databaseFeatures.getOtherObjectName(testExpr.toString().toUpperCase())).append(" BETWEEN ").append(beginExpr.toString()).append(" AND ").append(endExpr.toString()).append(')');
                        buffer.append(';').append('\n');
                    }
                } else if (sqlConstraint instanceof OracleForeignKey) {
                    builderFk.append(' ').append(Constants.DDL_ALTER_TABLE_KEY).append(' ').append(databaseFeatures.getSchema(captureTableDTO.getSchema())).append('.').append(databaseFeatures.getColumnName(captureTableDTO.getName())).append(' ');
                    builderFk.append("ADD ").append(sqlConstraint);
                }
            }
        });
    }

    private void generatorComplexParameter(@NotNull SQLBinaryOpExpr sqlBinaryOpExpr, DatabaseFeatures databaseFeatures, StringBuffer buffer) {
        SQLExpr sqlLeftExpr = sqlBinaryOpExpr.getLeft();
        SQLExpr sqlRightExpr = sqlBinaryOpExpr.getRight();
        String operator = sqlBinaryOpExpr.getOperator().getName();
        if (sqlLeftExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlLeftExpr;
            buffer.append(databaseFeatures.getOtherObjectName(StringUtils.replace(sqlIdentifierExpr.getName().toUpperCase(), "\"", ""))).append(operator);
        } else if (sqlLeftExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr1 = (SQLBinaryOpExpr) sqlLeftExpr;
            generatorComplexParameter(sqlBinaryOpExpr1, databaseFeatures, buffer);
            buffer.append(' ').append(operator).append(' ');
        } else if (sqlLeftExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) sqlLeftExpr;
            buffer.append(sqlIntegerExpr.getNumber()).append(operator);
        }
        if (sqlRightExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) sqlRightExpr;
            buffer.append(sqlIntegerExpr.getNumber());
        } else if (sqlRightExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlRightExpr;
            buffer.append(databaseFeatures.getOtherObjectName(StringUtils.replace(sqlIdentifierExpr.getName().toUpperCase(), "\"", "")));
        } else if (sqlRightExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr1 = (SQLBinaryOpExpr) sqlRightExpr;
            generatorComplexParameter(sqlBinaryOpExpr1, databaseFeatures, buffer);
        } else if (sqlRightExpr instanceof SQLCharExpr) {
            SQLCharExpr sqlCharExpr = (SQLCharExpr) sqlRightExpr;
            buffer.append('\'').append(sqlCharExpr.getText().toUpperCase()).append('\'');
        } else if (sqlRightExpr instanceof SQLNullExpr) {
            buffer.append('\t').append(Constants.NULL_VALUE);
        }
    }

    private void generatorCreateTableObject(SQLStatement sqlStatement, DatabaseFeatures databaseFeatures, CaptureTableDTO captureTableDTO, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap, @NotNull StringBuffer buffer) {
        SQLCreateTableStatement sqlCreateTableStatement = (SQLCreateTableStatement) sqlStatement;
        buffer.append(Constants.DDL_CREATE_TABLE_KEY).append(' ');
        boolean ifNotExist = sqlCreateTableStatement.isIfNotExists();
        if (ifNotExist) {
            buffer.append("IF NOT EXISTS").append(' ');
        }
        SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlCreateTableStatement.getTableSource().getExpr();
        String schemaAndTable = sqlPropertyExpr.toString();
        if (Objects.nonNull(schemaAndTable)) {
            schemaAndTable = databaseFeatures.getSchema(captureTableDTO.getTargetSchema()) + '.' + databaseFeatures.getTableName(captureTableDTO.getTargetName());
            buffer.append(schemaAndTable).append('(');
            List<SQLTableElement> tableElementList = sqlCreateTableStatement.getTableElementList();
            for (int i = 0, n = tableElementList.size(); i < n; i++) {
                SQLTableElement sqlTableElement = tableElementList.get(i);
                if (sqlTableElement instanceof SQLColumnDefinition) {
                    splicingColumn(null, buffer, (SQLColumnDefinition) sqlTableElement, captureTableDTO, databaseFeatures, dataTypeMappingBeanMap);
                } else if (sqlTableElement instanceof SQLPrimaryKeyImpl) {
                    SQLPrimaryKeyImpl sqlPrimaryKey = (SQLPrimaryKeyImpl) sqlTableElement;
                    SQLIndexDefinition sqlIndexDefinition = sqlPrimaryKey.getIndexDefinition();
                    if (Objects.nonNull(sqlIndexDefinition.getName())) {
                        buffer.append("CONSTRAINT").append(' ').append(StringUtils.replace(sqlIndexDefinition.getName().getSimpleName(), "\"", "")).append(' ');
                    }
                    buffer.append("PRIMARY KEY").append(' ');
                    generatorColumn(buffer, sqlPrimaryKey.getColumns(), databaseFeatures);
                } else if (sqlTableElement instanceof SQLUnique) {
                    SQLUnique sqlUnique = (SQLUnique) sqlTableElement;
                    SQLIndexDefinition sqlIndexDefinition = sqlUnique.getIndexDefinition();
                    if (Objects.nonNull(sqlIndexDefinition.getName())) {
                        buffer.append("CONSTRAINT").append(' ').append(StringUtils.replace(sqlIndexDefinition.getName().getSimpleName(), "\"", "")).append(' ');
                    }
                    buffer.append("UNIQUE").append(' ');
                    generatorColumn(buffer, sqlIndexDefinition.getColumns(), databaseFeatures);
                } else if (sqlTableElement instanceof OracleCheck) {
                    OracleCheck oracleCheck = (OracleCheck) sqlTableElement;
                    String checkConstraintName;
                    if (Objects.nonNull(oracleCheck.getName())) {
                        checkConstraintName = oracleCheck.getName().getSimpleName();
                    } else {
                        checkConstraintName = 'c' + String.valueOf(Math.abs(IDTools.getUUID()));
                    }
                    buffer.append("CONSTRAINT").append(' ').append(StringUtils.replace(checkConstraintName, "\"", "")).append(" CHECK ").append('(');
                    SQLExpr sqlExpr = oracleCheck.getExpr();
                    if (sqlExpr instanceof SQLBinaryOpExpr) {
                        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
                        generatorComplexParameter(sqlBinaryOpExpr, databaseFeatures, buffer);
                        buffer.append(')').append('\n');
                    } else if (sqlExpr instanceof SQLBetweenExpr) {
                        SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) sqlExpr;
                        SQLExpr testExpr = sqlBetweenExpr.getTestExpr();
                        SQLExpr beginExpr = sqlBetweenExpr.getBeginExpr();
                        SQLExpr endExpr = sqlBetweenExpr.getEndExpr();
                        buffer.append(databaseFeatures.getOtherObjectName(testExpr.toString().toUpperCase())).append(" BETWEEN ").append(beginExpr.toString()).append(" AND ").append(endExpr.toString()).append(')').append('\n');
                    }
                } else {
                    continue;
                }
                if (i != n - 1) {
                    buffer.append(',');
                }
            }
            buffer.append(')');
            SQLPartitionBy sqlPartitionBy = sqlCreateTableStatement.getPartitioning();
            if (Objects.nonNull(sqlPartitionBy)) {
                List<SQLExpr> sqlExprList = sqlPartitionBy.getColumns();
                List<SQLPartition> partitionList = sqlPartitionBy.getPartitions();
                if (!CollectionUtils.isEmpty(sqlExprList)) {
                    buffer.append('\n').append("PARTITION BY");
                    String partitionType = null;
                    if (sqlPartitionBy instanceof SQLPartitionByHash) {
                        buffer.append(" HASH");
                        partitionType = "HASH";
                    } else if (sqlPartitionBy instanceof SQLPartitionByList) {
                        buffer.append(" LIST");
                        partitionType = "LIST";
                    } else if (sqlPartitionBy instanceof SQLPartitionByRange) {
                        buffer.append(" RANGE");
                        partitionType = "RANGE";
                    }
                    buffer.append(databaseFeatures.generatorPartitionParameters(sqlExprList, partitionList, partitionType, schemaAndTable));
                }
            }
            buffer.append(';');
        }
    }

    private void settingAddColumnSql(StringBuffer buffer, CaptureTableDTO captureTableDTO, SQLAlterTableItem sqlAlterTableItem, DatabaseFeatures databaseFeatures, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) {
        SQLAlterTableAddColumn sqlAlterTableAddColumn = (SQLAlterTableAddColumn) sqlAlterTableItem;
        SQLColumnDefinition sqlColumnDefinition = sqlAlterTableAddColumn.getColumns().get(0);
        splicingColumn("ADD COLUMN", buffer, sqlColumnDefinition, captureTableDTO, databaseFeatures, dataTypeMappingBeanMap);
    }

    private void settingUniqueConstraint(StringBuffer buffer, SQLName sqlName, List<SQLSelectOrderByItem> orderByItemList, DatabaseFeatures databaseFeatures) {
        if (Objects.nonNull(sqlName)) {
            buffer.append("ADD CONSTRAINT").append(' ').append(StringUtils.replace(sqlName.getSimpleName(), "\"", "")).append(' ').append("UNIQUE").append(' ');
        } else {
            String tempName = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
            buffer.append("ADD CONSTRAINT").append(' ').append('f').append(tempName).append(' ').append("UNIQUE").append(' ');
        }
        generatorParameter(orderByItemList, databaseFeatures, buffer);
    }

    private void generatorParameter(@NotNull List<SQLSelectOrderByItem> orderByItemList, DatabaseFeatures databaseFeatures, StringBuffer buffer) {
        for (int i = 0, n = orderByItemList.size(); i < n; i++) {
            SQLExpr sqlExpr = orderByItemList.get(i).getExpr();
            if (i == 0) {
                buffer.append('(');
            }
            if (sqlExpr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
                buffer.append(databaseFeatures.getColumnName(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", "")));
            } else if (sqlExpr instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) sqlExpr;
                List<SQLExpr> argumentList = sqlMethodInvokeExpr.getArguments();
                String methodName = sqlMethodInvokeExpr.getMethodName();
                buffer.append(methodName);
                generatorMethodParameter(argumentList, databaseFeatures, buffer);
            } else if (sqlExpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
                generatorComplexParameter(sqlBinaryOpExpr, databaseFeatures, buffer);
            }
            if (i != n - 1) {
                buffer.append(',');
            }
            if (i == n - 1) {
                buffer.append(')');
            }
        }
    }

    private void generatorMethodParameter(@NotNull List<SQLExpr> sqlExprList, DatabaseFeatures databaseFeatures, StringBuffer buffer) {
        for (int i = 0, n = sqlExprList.size(); i < n; i++) {
            SQLExpr sqlExpr = sqlExprList.get(i);
            if (i == 0) {
                buffer.append('(');
            }
            if (sqlExpr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
                buffer.append(databaseFeatures.getColumnName(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", "")));
            } else if (sqlExpr instanceof SQLCharExpr) {
                SQLCharExpr sqlCharExpr = (SQLCharExpr) sqlExpr;
                buffer.append('\'').append(databaseFeatures.getColumnName(StringUtils.replace(sqlCharExpr.getText(), "\"", ""))).append('\'');
            } else {
                buffer.append(sqlExpr);
            }
            if (i != n - 1) {
                buffer.append(',');
            }
            if (i == n - 1) {
                buffer.append(')');
            }
        }
    }

    private void generatorColumn(StringBuffer buffer, @NotNull List<SQLSelectOrderByItem> orderByItemList, DatabaseFeatures databaseFeatures) {
        for (int i = 0, n = orderByItemList.size(); i < n; i++) {
            SQLExpr sqlExpr = orderByItemList.get(i).getExpr();
            if (i == 0) {
                buffer.append('(');
            }
            if (sqlExpr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
                buffer.append(databaseFeatures.getColumnName(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", "")));
                if (i != n - 1) {
                    buffer.append(',');
                }
            }
            if (i == n - 1) {
                buffer.append(')');
            }
        }
    }

    private void settingColumnLength(@NotNull List<SQLExpr> sqlExprList, DataTypeMappingDTO dataTypeMappingDTO, StringBuffer buffer) {
        for (int i = 0, n = sqlExprList.size(); i < n; i++) {
            if (Objects.nonNull(dataTypeMappingDTO)) {
                String targetName = dataTypeMappingDTO.getTargetName();
                if (StringTools.equalsIgnoreCase(Constants.DATABASE_INTEGER_TYPES, targetName)) {
                    break;
                }
                boolean needLength = Boolean.parseBoolean(dataTypeMappingDTO.getNeedLength());
                if (needLength) {
                    SQLExpr sqlExpr = sqlExprList.get(i);
                    if (i == 0) {
                        buffer.append('(');
                    }
                    if (sqlExpr instanceof SQLIntegerExpr) {
                        SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) sqlExpr;
                        int number = sqlIntegerExpr.getNumber().intValue();
                        if (StringUtils.equalsIgnoreCase("NUMERIC", targetName)) {
                            if (i == 0) {
                                if (number == 0 || number == 1) {
                                    number = 11;
                                }
                            } else {
                                if (number <= 0) {
                                    number = 2;
                                }
                            }
                        }
                        buffer.append(number);
                        if (i != n - 1) {
                            buffer.append(',');
                        }
                        if (i == n - 1) {
                            buffer.append(')');
                        }
                    } else if (sqlExpr instanceof SQLAllColumnExpr) {
                        buffer.append(38).append(',');
                        if (i == n - 1) {
                            buffer.append(')');
                        }
                    }
                }
            } else {
                SQLExpr sqlExpr = sqlExprList.get(i);
                if (i == 0) {
                    buffer.append('(');
                }
                if (sqlExpr instanceof SQLIntegerExpr) {
                    SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) sqlExpr;
                    int number = sqlIntegerExpr.getNumber().intValue();
                    buffer.append(number);
                    if (i != n - 1) {
                        buffer.append(',');
                    }
                }
                if (i == n - 1) {
                    buffer.append(')');
                }
            }
        }
    }

    private void generatorType(StringBuffer buffer, CaptureTableDTO captureTableDTO, @NotNull SQLColumnDefinition sqlColumnDefinition, DatabaseFeatures databaseFeatures, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) {
        SQLDataTypeImpl sqlDataType = (SQLDataTypeImpl) sqlColumnDefinition.getDataType();
        String originTypeName = StringUtils.replace(sqlDataType.getName(), "\"", "");
        DataTypeMappingDTO dataTypeMappingDTO = null;
        String targetName = originTypeName;
        if (!CollectionUtils.isEmpty(dataTypeMappingBeanMap)) {
            dataTypeMappingDTO = dataTypeMappingBeanMap.get(originTypeName);
            if (Objects.nonNull(dataTypeMappingDTO)) {
                targetName = dataTypeMappingDTO.getTargetName();
            }
        }
        Boolean withTimeZone = sqlDataType.getWithTimeZone();
        if (Objects.nonNull(withTimeZone) && withTimeZone) {
            targetName = targetName + " WITH TIME ZONE";
        }
        targetName = getTargetType(databaseFeatures, sqlDataType, targetName);
        buffer.append(targetName);
        if (Objects.nonNull(dataTypeMappingDTO)) {
            dataTypeMappingDTO.setTargetName(targetName);
        }
        if (sqlDataType instanceof OracleDataTypeIntervalDay) {
            OracleDataTypeIntervalDay oracleDataTypeIntervalDay = (OracleDataTypeIntervalDay) sqlDataType;
            List<SQLExpr> fractionalSeconds = oracleDataTypeIntervalDay.getFractionalSeconds();
            settingColumnLength(fractionalSeconds, dataTypeMappingDTO, buffer);
        } else {
            List<SQLExpr> sqlExprList = sqlDataType.getArguments();
            settingColumnLength(sqlExprList, dataTypeMappingDTO, buffer);
        }
        buffer.append(' ');
        SQLExpr defaultExpr = sqlColumnDefinition.getDefaultExpr();
        if (Objects.nonNull(defaultExpr)) {
            if (defaultExpr instanceof SQLSequenceExpr) {
                buffer.append(databaseFeatures.autoIncrementScript(captureTableDTO.getTargetName(), sqlColumnDefinition.getColumnName())).append(' ');
            } else {
                buffer.append("DEFAULT").append(' ');
                if (defaultExpr instanceof SQLCharExpr) {
                    SQLCharExpr sqlCharExpr = (SQLCharExpr) defaultExpr;
                    buffer.append('\'').append(sqlCharExpr.getText()).append('\'');
                } else if (defaultExpr instanceof SQLIntegerExpr) {
                    SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) defaultExpr;
                    buffer.append(sqlIntegerExpr.getNumber().intValue());
                } else if (defaultExpr instanceof SQLNumberExpr) {
                    SQLNumberExpr sqlNumberExpr = (SQLNumberExpr) defaultExpr;
                    buffer.append(sqlNumberExpr.getNumber());
                } else if (defaultExpr instanceof SQLBooleanExpr) {
                    SQLBooleanExpr sqlBooleanExpr = (SQLBooleanExpr) defaultExpr;
                    buffer.append(sqlBooleanExpr.getBooleanValue());
                } else if (defaultExpr instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) defaultExpr;
                    buffer.append(sqlIdentifierExpr.getName());
                } else if (defaultExpr instanceof SQLNullExpr) {
                    buffer.append(Constants.NULL_VALUE);
                } else if (defaultExpr instanceof SQLMethodInvokeExpr) {
                    if (StringUtils.containsIgnoreCase(defaultExpr.toString(), "sysdate")) {
                        buffer.append("CURRENT_TIMESTAMP");
                    } else {
                        SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) defaultExpr;
                        buffer.append(sqlMethodInvokeExpr.getMethodName());
                        generatorMethodParameter(sqlMethodInvokeExpr.getArguments(), databaseFeatures, buffer);
                    }
                } else if (defaultExpr instanceof OracleSysdateExpr) {
                    buffer.append("CURRENT_TIMESTAMP");
                }
            }
        }
    }

    private void splicingColumn(String key, StringBuffer buffer, SQLColumnDefinition sqlColumnDefinition, CaptureTableDTO captureTableDTO, DatabaseFeatures databaseFeatures, Map<String, DataTypeMappingDTO> dataTypeMappingBeanMap) {
        if (StringUtils.isNotBlank(key)) {
            buffer.append(key).append(' ');
        }
        buffer.append(databaseFeatures.getColumnName(StringUtils.replace(sqlColumnDefinition.getName().getSimpleName(), "\"", ""))).append(' ');
        generatorType(buffer, captureTableDTO, sqlColumnDefinition, databaseFeatures, dataTypeMappingBeanMap);
    }

    private @Nullable String getTableScript(OracleConnection oracleConnection, @NotNull CaptureTableDTO captureTableDTO) {
        try (PreparedStatement statement = oracleConnection.connection().prepareStatement(IConstants.QUERY_TABLE_META, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, captureTableDTO.getName());
            statement.setString(2, captureTableDTO.getSchema());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    do {
                        Clob clob = resultSet.getClob(1);
                        Reader reader = clob.getCharacterStream();
                        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
                            StringBuilder buffer = new StringBuilder();
                            String temp;
                            while ((temp = bufferedReader.readLine()) != null) {
                                buffer.append(temp);
                            }
                            return buffer.toString();
                        }
                    } while (resultSet.next());
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(MessageFormat.format("{0}.{1}解析表结构异常!{2}", captureTableDTO.getSchema(), captureTableDTO.getName(), e.getMessage()));
        }
        return null;
    }

    private String getTargetType(@NotNull DatabaseFeatures databaseFeatures, SQLDataTypeImpl sqlDataType, String targetName) {
        try {
            switch (databaseFeatures.databaseType()) {
                case MYSQL:
                    return SQLTransformUtils.transformOracleToMySql(sqlDataType).getName();
                default:
                    return targetName;
            }
        } catch (Exception e) {
            return targetName;
        }
    }
}
