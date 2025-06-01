package org.dukejasun.migrate.model.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import org.dukejasun.migrate.common.Constants;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@JsonIgnoreProperties(value = {"type", "objectType", "createTime", "updateTime"})
public class DbMigrateResultStatisticsDTO extends EventObjectParentDTO {
    private static final long serialVersionUID = -8082769160330582213L;
    private String taskId;
    private String sourceSchema;
    private String sourceTable;
    private String targetSchema;
    private String targetTable;
    private String transmissionType;
    private String transmissionResult;
    private String sourceRecord;
    private String targetRecord;
    private String errorReason;
    private String objectType;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Shanghai")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime createTime;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Shanghai")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime updateTime;

    @Override
    public String getType() {
        return Constants.CALLBACK_TASK_TYPE;
    }
}
