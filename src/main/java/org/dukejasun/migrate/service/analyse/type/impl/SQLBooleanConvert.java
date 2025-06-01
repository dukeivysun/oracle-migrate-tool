package org.dukejasun.migrate.service.analyse.type.impl;

import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("SQLBooleanExpr_CONVERT")
public class SQLBooleanConvert implements DataTypeConvert<SQLBooleanExpr> {
    @Override
    public void generatorColumnNameAndValue(@NotNull SQLIdentifierExpr sqlIdentifierExpr, @NotNull SQLBooleanExpr sqlDataType, @NotNull LinkedHashMap<String, Object> linkedHashMap) {
        linkedHashMap.put(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", ""), sqlDataType.getBooleanValue());
    }
}
