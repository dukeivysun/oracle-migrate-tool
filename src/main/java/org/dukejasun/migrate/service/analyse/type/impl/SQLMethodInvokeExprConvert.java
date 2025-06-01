package org.dukejasun.migrate.service.analyse.type.impl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("SQLMethodInvokeExpr_CONVERT")
public class SQLMethodInvokeExprConvert implements DataTypeConvert<SQLMethodInvokeExpr> {
    @Override
    public void generatorColumnNameAndValue(SQLIdentifierExpr sqlIdentifierExpr, @NotNull SQLMethodInvokeExpr sqlDataType, LinkedHashMap<String, Object> linkedHashMap) {
        List<SQLExpr> argumentList = sqlDataType.getArguments();
        if (!CollectionUtils.isEmpty(argumentList)) {
            SQLExpr sqlExpr = argumentList.get(0);
            if (sqlExpr instanceof SQLCharExpr) {
                linkedHashMap.put(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", ""), ((SQLCharExpr) sqlExpr).getText());
            }
        }
    }
}
