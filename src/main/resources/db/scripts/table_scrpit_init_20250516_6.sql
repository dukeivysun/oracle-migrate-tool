##迁移任务表
CREATE TABLE if not exists `t_migrate_task`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT,
    `job_id`          varchar(64)  NOT NULL DEFAULT '',
    `task_name`       varchar(256) NOT NULL DEFAULT '',
    `source_id`       varchar(32)  NOT NULL DEFAULT '',
    `target_id`       varchar(32)  NOT NULL DEFAULT '',
    `capture_schemas` text,
    `status`          varchar(64)  NOT NULL DEFAULT 'NOT_START',
    `migrate_type`    varchar(64) NOT NULL DEFAULT 'TOTAL',
    `comments`        text,
    `extra`           text,
    `create_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `create_user`     varchar(32)  NOT NULL DEFAULT '',
    `update_user`     varchar(32)  NOT NULL DEFAULT '',
    `is_deleted`      bool   NOT NULL DEFAULT false,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

