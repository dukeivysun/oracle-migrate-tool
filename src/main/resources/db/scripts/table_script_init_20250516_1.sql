##数据源参数配置
CREATE TABLE if not exists `t_datasource_parameters`
(
    `id`         int(11)     NOT NULL AUTO_INCREMENT,
    `db_type`    varchar(64) NOT NULL DEFAULT '',
    `parameters` text,
    `is_deleted` bool        NOT NULL DEFAULT false,
    PRIMARY KEY (`id`),
    UNIQUE KEY `t_datasource_parameters_pk_2` (`db_type`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4