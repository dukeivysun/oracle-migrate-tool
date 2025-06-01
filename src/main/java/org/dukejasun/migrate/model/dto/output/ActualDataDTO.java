package org.dukejasun.migrate.model.dto.output;

import org.dukejasun.migrate.enums.OperationType;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author dukedpsun
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ActualDataDTO implements Serializable {
    private static final long serialVersionUID = 1692622545506774952L;
    private String taskId;
    private String clientId;
    private String scn;
    private String script;
    private String schemaName;
    private String tableName;
    private String schemaAndTable;
    private Boolean hasKey;
    private Long transactionalTime;
    private List<String> pkColumnNameList;
    private List<ColumnDataDTO> before;
    private List<ColumnDataDTO> after;
    private LinkedHashMap<String, Object> linkedMap;
    private OperationType operation;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
