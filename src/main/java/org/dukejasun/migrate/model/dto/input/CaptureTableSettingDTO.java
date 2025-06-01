package org.dukejasun.migrate.model.dto.input;

import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.dukejasun.migrate.utils.JacksonUtil;

import java.io.Serializable;
import java.util.List;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CaptureTableSettingDTO implements Serializable {
    private static final long serialVersionUID = 1932971145232318148L;
    private List<CaptureTableDTO> includeTables;
    private List<CaptureTableDTO> excludeTables;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }

}

