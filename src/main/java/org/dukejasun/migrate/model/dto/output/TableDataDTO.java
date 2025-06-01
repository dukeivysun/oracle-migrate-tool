package org.dukejasun.migrate.model.dto.output;

import lombok.*;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;

/**
 * @author dukedpsun
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class TableDataDTO implements Serializable {
    private static final long serialVersionUID = -2210553752555504299L;
    private String schemaName;
    private String tableName;
    private List<ColumnDataDTO> columnDataList;
}
