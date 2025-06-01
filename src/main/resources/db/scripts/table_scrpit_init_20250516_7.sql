##记录结构、全量的迁移结果
CREATE TABLE if not exists `t_migrate_result_statistics`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT,
    `job_id`              varchar(64)  NOT NULL DEFAULT '',
    `task_id`             varchar(32)  NOT NULL DEFAULT '',
    `source_schema`       varchar(176) NOT NULL DEFAULT '',
    `source_table`        varchar(176) NOT NULL DEFAULT '',
    `target_schema`       varchar(176) NOT NULL DEFAULT '',
    `target_table`        varchar(176) NOT NULL DEFAULT '',
    `transmission_type`   varchar(32)  NOT NULL DEFAULT '',
    `transmission_result` varchar(32)  NOT NULL DEFAULT '',
    `source_record`       text,
    `target_record`       text,
    `error_reason`        text,
    `object_type`         varchar(128) NOT NULL DEFAULT 'table',
    `create_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `t_migrate_result_statistics_pk_2` (`task_id`, `source_schema`, `source_table`, `target_schema`,
                                                   `target_table`, `transmission_type`)
) ROW_FORMAT=DYNAMIC ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
