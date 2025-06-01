package org.dukejasun.migrate.model.dto.input;

import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.utils.JacksonUtil;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ParameterDTO implements Serializable {
    private static final long serialVersionUID = -1729414560114329928L;
    private String taskId;
    private Long startScn;
    private Integer batch;
    private String dictionaryPath;
    private String dictionaryLocation;
    private String pdbName;
    private List<String> schemaList;
    private DatasourceDTO dataSourceDTO;
    private DatasourceDTO targetSourceDTO;
    private MigrateType migrateType;
    private Map<String, Object> extra;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
