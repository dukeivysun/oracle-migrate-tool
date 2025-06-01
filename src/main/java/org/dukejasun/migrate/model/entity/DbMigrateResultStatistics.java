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
@Table(name = "t_migrate_result_statistics")
@DynamicInsert
@DynamicUpdate
public class DbMigrateResultStatistics implements Serializable {
    private static final long serialVersionUID = -8189780333832132838L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String jobId;
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
    @CreatedDate
    private LocalDateTime createTime;
    @LastModifiedDate
    private LocalDateTime updateTime;

}
