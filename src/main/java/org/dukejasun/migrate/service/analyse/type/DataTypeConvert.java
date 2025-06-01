package org.dukejasun.migrate.service.analyse.type;


import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;

import java.util.LinkedHashMap;

/**
 * 列数据转换
 *
 * @param <T>
 * @author dukedpsun
 */
public interface DataTypeConvert<T extends SQLExprImpl> {

    /**
     * 获取列名称和值
     *
     * @param sqlIdentifierExpr
     * @param sqlDataType
     * @param linkedHashMap
     */
    void generatorColumnNameAndValue(SQLIdentifierExpr sqlIdentifierExpr, T sqlDataType, LinkedHashMap<String, Object> linkedHashMap);
}
