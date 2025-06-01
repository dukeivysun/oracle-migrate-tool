package org.dukejasun.migrate.model.dto.metadata;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.dukejasun.migrate.utils.JacksonUtil;

import java.io.Serializable;

/**
 * @author dukedpsun
 */
@Setter
@Getter
@Accessors(chain = true)
public class CaptureColumnDTO implements Serializable {
    private static final long serialVersionUID = -3793649808718620687L;
    /**
     * 列序号
     */
    private Integer columnNo;
    /**
     * schema
     */
    private String tableSchema;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 列名
     */
    private String name;
    /**
     * 数据类型
     */
    private String dataType;
    /**
     * 数据类型
     */
    private String intervalType;
    /**
     * 数据长度
     */
    private Integer length;
    /**
     * 精度
     */
    private Integer scale;
    /**
     * 自增
     */
    private Boolean increment;
    /**
     * 是否是主键
     */
    private Boolean primaryKey;
    /**
     * 唯一键
     */
    private Boolean uniqueKey;
    /**
     * 是否为空
     */
    private Boolean nullable;
    /**
     * 默认值
     */
    private String defaultValue;
    /**
     * 虚拟列
     */
    private Boolean virtualColumn;
    /**
     * 列描述
     */
    private String comment;
    /**
     * 映射的目标类型
     */
    private String dataTypeMapping;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
