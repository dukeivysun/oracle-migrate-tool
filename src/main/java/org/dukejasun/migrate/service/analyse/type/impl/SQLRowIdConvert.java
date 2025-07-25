package org.dukejasun.migrate.service.analyse.type.impl;

import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("ROWID_CONVERT")
public class SQLRowIdConvert implements DataTypeConvert<SQLExprImpl> {
    @Override
    public void generatorColumnNameAndValue(SQLIdentifierExpr sqlIdentifierExpr, SQLExprImpl sqlDataType, LinkedHashMap<String, Object> linkedHashMap) {
        if (sqlDataType instanceof SQLNullExpr) {
            linkedHashMap.put(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", ""), null);
        } else if (sqlDataType instanceof SQLCharExpr) {
            linkedHashMap.put(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", ""), ((SQLCharExpr) sqlDataType).getText());
        }
    }
}
