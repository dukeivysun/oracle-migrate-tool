package org.dukejasun.migrate.model.dto.output;

import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @author dukedpsun
 */
@Setter
@Getter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class AnalyseResultDTO implements Serializable {
    private static final long serialVersionUID = -1421416757681405099L;
    private String originScript;
    private String convertScript;
    private List<String> otherScript;
    private String fkScript;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotBlank(convertScript)) {
            builder.append(convertScript.trim()).append('\n');
        }
        if (!CollectionUtils.isEmpty(otherScript)) {
            for (String sql : otherScript) {
                if (StringUtils.isNotBlank(sql)) {
                    builder.append(sql.trim()).append('\n');
                }
            }
        }
        if (StringUtils.isNotBlank(fkScript)) {
            builder.append(fkScript.trim()).append('\n');
        }
        return builder.toString();
    }
}
