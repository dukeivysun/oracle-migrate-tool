package org.dukejasun.migrate.model.dto.output;

import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author dukedpsun
 */
@Getter
@Setter
public class MigrateResultDTO implements Serializable {
    private static final long serialVersionUID = 4797170142714388811L;
    private String sourceRecord;
    private String targetRecord;
    private String errorReason;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
