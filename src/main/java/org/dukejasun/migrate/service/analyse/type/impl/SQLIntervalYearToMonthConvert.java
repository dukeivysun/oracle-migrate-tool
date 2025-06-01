package org.dukejasun.migrate.service.analyse.type.impl;

import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("INTERVAL_YEAR_TO_MONTH_CONVERT")
public class SQLIntervalYearToMonthConvert implements DataTypeConvert<SQLExprImpl> {
    @Override
    public void generatorColumnNameAndValue(SQLIdentifierExpr sqlIdentifierExpr, SQLExprImpl sqlDataType, LinkedHashMap<String, Object> linkedHashMap) {
        if (sqlDataType instanceof SQLTextLiteralExpr) {
            SQLTextLiteralExpr sqlTextLiteralExpr = (SQLTextLiteralExpr) sqlDataType;
            linkedHashMap.put(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", ""), sqlTextLiteralExpr.getText());
        }
    }
}
