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
@JsonIgnoreProperties(value = {"id", "jobId", "clientId", "updateTime"})
public class DbCaptureRecordDTO extends EventObjectParentDTO {
    private static final long serialVersionUID = 8964347846305769447L;
    private Integer id;
    private String jobId;
    private String taskId;
    private String clientId;
    private Integer captureIndex;
    private String captureStartScn;
    private String captureEndScn;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Shanghai")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime updateTime;

    @Override
    public String getType() {
        return Constants.CALLBACK_RECORD_TYPE;
    }
}
