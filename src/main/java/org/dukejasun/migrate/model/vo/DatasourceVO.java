package org.dukejasun.migrate.model.vo;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class DatasourceVO implements Serializable {
    private static final long serialVersionUID = 7742158523359718719L;
    private String datasourceName;
    private String type;
    private String host;
    private String port;
    private String username;
    private String password;
    private String version;
    private String databaseName;
    private String characterSet;
    private String timezone;
    private Map<String, Object> extra;
}
