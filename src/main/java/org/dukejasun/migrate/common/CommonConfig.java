package org.dukejasun.migrate.common;

import org.dukejasun.migrate.config.PropertyResourceFactory;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@Configuration("commonConfig")
@PropertySource(value = "classpath:common.yml", encoding = "utf-8", factory = PropertyResourceFactory.class)
@ConfigurationProperties(prefix = "common.config")
public class CommonConfig {
    /**
     * 同步类型
     */
    private String dataTypeMappingKey;
    /**
     * 任务开始时间
     */
    private LocalDateTime startTransmissionTime = LocalDateTime.now();
    /**
     * 名称转换规则；0表示不变；1表示大写；2表示小写
     */
    private Integer nameResolution;
    /**
     * 源端数据查询时的fetchSize
     */
    private Integer fetchSize;
    /**
     * 批量提交数量
     */
    private Integer commitSize;
    /**
     * 连接池初始连接数
     */
    private Integer minimumIdle;
    /**
     * maximumPoolSize
     */
    private Integer maximumPoolSize;
    /**
     * 连接池核心数
     */
    private Integer corePoolSize;
    /**
     * 内存队列大小
     */
    private Integer bufferSize;
    /**
     * 采集数据输出目录
     */
    private String outputPath;
    /**
     * 数据类型映射文件
     */
    private String dataTypeMappingLocation;
    /**
     * 启动任务入参
     */
    private String captureParameterLocation;
    /**
     * 断点增量启动序号
     */
    private Integer incrementStartNumber;
    /**
     * 断点增量已经完成采集的序号
     */
    private Integer incrementCaptureNumber;
    /**
     * 0表示启动oracle增量采集线程和启动目标写入线程；1表示只启动oracle全量、增量采集进程；2表示只启动目标写入进程
     */
    private Integer replicateType;
    /**
     * 采集并发数量，建议小于等于4
     */
    private Integer incrementCaptureThreadNumber;
    /**
     * 数据下沉线程数量
     */
    private Integer incrementWriteThreadNumber;
    /**
     * 增量数据文件前缀
     */
    private String incrementDataFilePrefix;
    /**
     * 增量数据采集类型true是按照事务采集；false是非事务采集
     */
    private Boolean incrementCaptureType = true;
    private Boolean supportEncrypt;
    /**
     * 如果是空则为新任务；如果不空则为存在的任务，配合replicateType属性，如果replicateType的值时0或者1则启动指定的Id的任务（全量或者增量），如果replicateType=2，则表示启动任务的写入进程
     */
    private String taskId;
    /**
     * 全量任务Id
     */
    private String totalTaskId;
    /**
     * 全量迁移并行线程数量
     */
    private Integer totalMigrateThreadNumber;
    /**
     * 全量迁移前清空目标表
     */
    private Boolean totalMigrateTruncateTargetTable;
    /**
     * 是否采用分段方式进行全量数据采集
     */
    private Boolean totalMigrateByPage;
    /**
     * 全量迁移是否通过数据文件
     */
    private Boolean totalDataToFile;
    /**
     * 全量数据写入目标通过copy还是insert
     */
    private String totalDataWriteTargetModel;
    /**
     * 结构迁移是否包含索引
     */
    private Boolean structureContainsIndex = true;
    /**
     * 数据文件大小阈值
     */
    private String dataFileThreshold;
    /**
     * 数据之间的分隔符
     */
    private String dataDelimiter = ",";
    /**
     * 主键冲突处理：0.抛异常 1.覆盖 2.跳过
     */
    private Integer primaryKeyConflictOperator;
    /**
     * 全量迁移完成后支持数据对比
     */
    private Boolean supportDataCompare;
    /**
     * oracle字符集这是
     *
     * @deprecated
     */
    private String oracleCharacterEncoding;
    /**
     * 字符集设置
     *
     * @deprecated
     */
    private String characterEncoding;
    /**
     * 指定连接池中连接的最大生存时间,单位:毫秒.默认是1800000,即30分钟
     */
    private Integer maxLifetimeMs;
    /**
     * 空闲连接超时时间,一个连接idle状态的最大时长（毫秒）,超时则被释放（retired）,默认是10分钟,只有空闲连接数大于最大连接数且空闲时间超过该值,才会被释放
     */
    private Integer idleTimeoutMs;
    /**
     * 数据库连接超时时间,默认30秒,即30000
     */
    private Integer connectionTimeoutMs;
    /**
     * 指定验证连接有效性的超时时间(默认是5秒,最小不能小于250毫秒)
     */
    private Integer validationTimeoutMs;

    public Boolean insertModel() {
        return Objects.nonNull(totalDataWriteTargetModel) && StringUtils.equalsIgnoreCase("insert", totalDataWriteTargetModel);
    }
}
