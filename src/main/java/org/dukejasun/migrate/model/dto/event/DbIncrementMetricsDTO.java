package org.dukejasun.migrate.model.dto.event;

import org.dukejasun.migrate.common.Constants;
import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

/**
 * @author dukedpsun
 */
@Setter
@Getter
public class DbIncrementMetricsDTO extends EventObjectParentDTO {
    private static final long serialVersionUID = -1094861748741444065L;
    private String taskId;
    private String schemaName;
    private String tableName;
    private String targetSchema;
    private String targetTable;
    private Timestamp transactionTime;
    private Timestamp captureTime;
    private Timestamp sinkTime;
    private Long captureDelay;
    private Long sinkDelay;
    private String captureMetrics;
    private String sinkMetrics;

    @Override
    public String getType() {
        return Constants.INCREMENT_REPLICATOR_METRICS;
    }
}
