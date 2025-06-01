package org.dukejasun.migrate.model.vo;

import lombok.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class ParameterVO implements Serializable {
    private static final long serialVersionUID = -4122528467348899843L;
    private String jobId;
    private String taskName;
    private String sourceId;
    private String targetId;
    /**
     * DatasourceVO对象不能与sourceId和targetId对象同时存在
     */
    private DatasourceVO datasourceVO;
    private DatasourceVO targetSourceVO;
    private String migrateType;
    private List<String> schemaList;
    /**
     * startScn, batch, dictionaryPath, dictionaryLocation, captureTableSettingPath
     * 如果是增量上面的四个属性不能少
     * 全量需要增加flashBack，scn或者时间戳;captureTableSettingPath
     */
    private Map<String, Object> extra;

}
