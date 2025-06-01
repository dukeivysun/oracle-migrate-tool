package org.dukejasun.migrate.model.entity;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
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
@Table(name = "t_migrate_task")
@DynamicInsert
@DynamicUpdate
public class DbMigrateTask implements Serializable {
    private static final long serialVersionUID = 3935185420769596306L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String jobId;
    private String taskName;
    private String sourceId;
    private String targetId;
    private String captureSchemas;
    private String status;
    private String migrateType;
    private String comments;
    private String extra;
    @CreatedDate
    private LocalDateTime createTime;
    @LastModifiedDate
    private LocalDateTime updateTime;
    @CreatedBy
    private String createUser;
    @LastModifiedBy
    private String updateUser;
    private Boolean isDeleted;

}
