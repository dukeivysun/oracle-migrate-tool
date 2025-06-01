package org.dukejasun.migrate.model.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import org.dukejasun.migrate.common.Constants;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@ToString
public class DbSinkRecordDTO extends EventObjectParentDTO {
    private static final long serialVersionUID = 1615749370253426969L;
    private Integer id;
    private String jobId;
    private String taskId;
    private String clientId;
    private Integer sinkIndex;
    private String sinkStartScn;
    private String sinkEndScn;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Shanghai")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime updateTime;

    @Override
    public String getType() {
        return Constants.CALLBACK_RECORD_TYPE;
    }
}
