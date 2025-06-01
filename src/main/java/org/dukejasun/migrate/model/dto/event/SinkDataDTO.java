package org.dukejasun.migrate.model.dto.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.output.ActualDataDTO;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * @author dukedpsun
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(value = {"datasourceDTO", "actualDataDTOList"})
public class SinkDataDTO extends EventObjectParentDTO {
    private static final long serialVersionUID = 579760206292042414L;
    private Integer dealIndex;
    private Long serialNumber;
    private String taskId;
    private DatasourceDTO datasourceDTO;
    private List<ActualDataDTO> actualDataDTOList;

    @Override
    public String getType() {
        return Constants.WRITE_DATA_TYPE;
    }
}
