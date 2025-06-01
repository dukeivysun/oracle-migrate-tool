package org.dukejasun.migrate.model.dto.event;

import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@ToString
public class DataLogFileDTO extends EventObjectParentDTO implements Comparable<DataLogFileDTO> {
    private static final long serialVersionUID = -3540360385657399556L;
    private String taskId;
    private String index;
    private Boolean empty;
    private String dataLogPath;
    private String key;
    private DatasourceDTO datasourceDTO;

    @Override
    public String getType() {
        return Constants.DATA_LOG_TYPE;
    }

    @Override
    public int compareTo(@NotNull DataLogFileDTO o) {
        return this.getIndex().compareTo(o.getIndex());
    }
}
