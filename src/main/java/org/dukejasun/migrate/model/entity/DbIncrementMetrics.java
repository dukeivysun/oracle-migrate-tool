package org.dukejasun.migrate.model.entity;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@Entity
@Table(name = "t_increment_replicator_metrics")
@DynamicInsert
@DynamicUpdate
public class DbIncrementMetrics implements Serializable {
    private static final long serialVersionUID = -4566115131853477753L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
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
    @CreationTimestamp
    private Timestamp createTime;
    @UpdateTimestamp
    private Timestamp updateTime;
}
