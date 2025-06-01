package org.dukejasun.migrate.model.dto.output;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.dukejasun.migrate.enums.OperationType;
import org.dukejasun.migrate.utils.JacksonUtil;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 用于从数据库解析增量数据的临时格式
 *
 * @author dukedpsun
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class OriginDataDTO implements Serializable {
    private static final long serialVersionUID = -9140886373399274424L;
    private String taskId;
    private Long timestamp;
    private Long captureTimestamp;
    private String schema;
    private String table;
    private String ddl;
    private String xId;
    private String rowId;
    private BigInteger scn;
    private OperationType type;
    private List<String> typeList;
    private List<String> pkColumnName;
    private LinkedHashMap<String, Object> before;
    private LinkedHashMap<String, Object> after;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
