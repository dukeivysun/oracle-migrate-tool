package org.dukejasun.migrate.model.dto.input;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@ToString
public class CurrentScnInfoDTO implements Serializable {
    private static final long serialVersionUID = 1340814110849679873L;
    private Long archivedLogFileNumber;
    private BigInteger minScn;
    private BigInteger maxScn;
    private BigInteger currentScn;
}
