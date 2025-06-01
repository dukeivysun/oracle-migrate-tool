package org.dukejasun.migrate.model.dto.output;

import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author dukedpsun
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class CompareResultDTO implements Serializable {
    private static final long serialVersionUID = -1667501409341878267L;
    private String taskId;
    private String schema;
    private String tableName;
    private String sourceNumber;
    private String targetNumber;
    private String compareResult;

    private List<String> compareConditionList;
    private Map<String, Object> differenceMap;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
