package org.dukejasun.migrate.service.analyse.type.impl;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import org.dukejasun.migrate.service.analyse.type.DataTypeConvert;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;

/**
 * @author dukdpsun
 */
@Slf4j
@Component("SQLCharExpr_CONVERT")
public class SQLCharConvert implements DataTypeConvert<SQLTextLiteralExpr> {

    @Override
    public void generatorColumnNameAndValue(@NotNull SQLIdentifierExpr sqlIdentifierExpr, @NotNull SQLTextLiteralExpr sqlDataType, @NotNull LinkedHashMap<String, Object> linkedHashMap) {
        linkedHashMap.put(StringUtils.replace(sqlIdentifierExpr.getName(), "\"", ""), sqlDataType.getText());
    }
}
