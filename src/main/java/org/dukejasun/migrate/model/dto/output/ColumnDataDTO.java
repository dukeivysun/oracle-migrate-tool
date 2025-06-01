package org.dukejasun.migrate.model.dto.output;

import lombok.*;
import org.dukejasun.migrate.utils.JacksonUtil;

import java.io.Serializable;

/**
 * @author dukedpsun
 */
@Setter
@Getter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class ColumnDataDTO implements Serializable {
    private static final long serialVersionUID = -6326975339289132431L;
    private String name;
    private String type;
    private Object value;

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
