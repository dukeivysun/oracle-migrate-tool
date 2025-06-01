package org.dukejasun.migrate.model.dto.mapping;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@ToString
public class DataTypeMappingDTO implements Serializable {
    private static final long serialVersionUID = 5155831067454001758L;
    private String source;
    private String target;
    private String sourceVersion;
    private String targetVersion;
    private String sourceName;
    private String targetName;
    private String range;
    private String needLength;
    private String needScale;
    private String replaceType;
}
