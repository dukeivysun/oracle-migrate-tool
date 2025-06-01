package org.dukejasun.migrate.service.analyse.type.impl;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
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
@Component("SQLIntegerExpr_CONVERT")
public class SQLIntegerConvert implements DataTypeConvert<SQLNumericLiteralExpr> {

    @Override
    public void generatorColumnNameAndValue(@NotNull SQLIdentifierExpr sqlIdentifierExpr, @NotNull SQLNumericLiteralExpr sqlDataType, @NotNull LinkedHashMap<String, Object> linkedHashMap) {
        linkedHashMap.put(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", ""), sqlDataType.getNumber().intValue());
    }
}
