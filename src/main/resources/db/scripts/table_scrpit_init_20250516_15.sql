create TABLE if not exists `t_increment_replicator_metrics`
(
    `id`               bigint auto_increment comment '自增主键',
    `task_id`          varchar(32)  default ''                not null comment '任务Id',
    `schema_name`      varchar(128) default ''                not null comment '数据库名称或者schema名称',
    `table_name`       varchar(128) default ''                not null comment '数据库表名称',
    `target_schema`    varchar(128) default ''                not null comment '目标schema名称',
    `target_table`     varchar(128) default ''                not null comment '目标表名称',
    `transaction_time` datetime comment '事务产生时间',
    `capture_time`     datetime comment '数据采集时间',
    `sink_time`        datetime comment '数据下沉时间',
    `capture_delay`    bigint       default 0                 not null comment '采集延迟时间(毫秒)',
    `sink_delay`       bigint       default 0                 not null comment '数据下沉延迟时间(毫秒)',
    `capture_metrics`  varchar(128) default ''                not null comment '采集日志指标(rps/tps)',
    `sink_metrics`     varchar(128) default ''                not null comment '数据下沉指标(rps)',
    `create_time`      datetime     default current_timestamp not null comment '数据创建时间',
    `update_time`      datetime     default current_timestamp not null comment '数据更新时间',
    constraint t_increment_replicator_metrics_pk primary key (`id`)
) comment '增量同步指标';

