##同步记录表
CREATE TABLE if not exists `t_increment_record`
(
    `id`                int(11)      NOT NULL AUTO_INCREMENT,
    `job_id`            varchar(64)  NOT NULL DEFAULT '',
    `task_id`           varchar(32)  NOT NULL DEFAULT '',
    `client_id`         varchar(256) NOT NULL DEFAULT '',
    `capture_index`     int(11)      NOT NULL DEFAULT '0',
    `capture_start_scn` varchar(32)  NOT NULL DEFAULT '',
    `capture_end_scn`   varchar(32)  NOT NULL DEFAULT '',
    `sink_index`        int(11)      NOT NULL DEFAULT '0',
    `sink_start_scn`    varchar(32)  NOT NULL DEFAULT '',
    `sink_end_scn`      varchar(32)  NOT NULL DEFAULT '',
    `extra`             text,
    `create_time`       timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`       timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `create_user`       varchar(32)  NOT NULL DEFAULT '',
    `update_user`       varchar(32)  NOT NULL DEFAULT '',
    `is_deleted`      bool   NOT NULL DEFAULT false,
    PRIMARY KEY (`id`),
    UNIQUE KEY `t_increment_record_pk_2` (`task_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;