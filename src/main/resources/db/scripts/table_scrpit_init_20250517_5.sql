##数据源配置表
CREATE TABLE if not exists `t_datasource`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT,
    `datasource_name` varchar(256) NOT NULL DEFAULT '',
    `db_type`         varchar(64)  NOT NULL DEFAULT '',
    `host`            varchar(256) NOT NULL DEFAULT '',
    `port`            varchar(10)  NOT NULL DEFAULT '',
    `user_name`       varchar(64)  NOT NULL DEFAULT '',
    `password`        varchar(256) NOT NULL DEFAULT '',
    `version`         varchar(256) NOT NULL DEFAULT '',
    `db_name`         varchar(256) NOT NULL DEFAULT '',
    `character_set`   varchar(20)  NOT NULL DEFAULT '',
    `timezone`        varchar(255) NOT NULL DEFAULT '',
    `extra`           text,
    `create_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `create_user`     varchar(32)  NOT NULL DEFAULT '',
    `update_user`     varchar(32)  NOT NULL DEFAULT '',
    `is_deleted`      bool   NOT NULL DEFAULT false,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4