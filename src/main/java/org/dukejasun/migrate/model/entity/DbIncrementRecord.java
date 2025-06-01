package org.dukejasun.migrate.model.entity;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@Entity
@Table(name = "t_increment_record")
@DynamicInsert
@DynamicUpdate
public class DbIncrementRecord implements Serializable {
    private static final long serialVersionUID = 3935185420769596306L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String jobId;
    private String taskId;
    private String clientId;
    private Integer captureIndex;
    private String captureStartScn;
    private String captureEndScn;
    private Integer sinkIndex;
    private String sinkStartScn;
    private String sinkEndScn;
    private String extra;
    @CreatedDate
    private LocalDateTime createTime;
    @LastModifiedDate
    private LocalDateTime updateTime;
}
