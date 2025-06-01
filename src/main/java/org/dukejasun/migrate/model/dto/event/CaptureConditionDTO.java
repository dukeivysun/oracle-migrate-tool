package org.dukejasun.migrate.model.dto.event;

import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.vo.LogFile;
import lombok.Getter;
import lombok.Setter;

import java.math.BigInteger;
import java.util.List;

/**
 * @author dukedpsun
 */
@Getter
@Setter
public class CaptureConditionDTO extends EventObjectParentDTO {
    private static final long serialVersionUID = -647848475624316772L;
    private String taskId;
    private Integer index;
    private BigInteger startScn;
    private BigInteger endScn;
    private String dictionaryPath;
    private String dictionaryLocation;
    private String pdbName;
    private String schemas;
    private String tables;
    private String parentDir;
    private DatasourceDTO dataSourceDTO;
    private List<LogFile> logFiles;

    @Override
    public String getType() {
        return Constants.CONDITION_TYPE;
    }
}
