package org.dukejasun.migrate.cache.model;

import org.dukejasun.migrate.cache.enums.CachedTypeEnum;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@Configuration("dataCacheConfig")
@ConfigurationProperties(prefix = "data.cache")
public class DataCacheConfig {
    private String type = CachedTypeEnum.CAFFEINE.getCode();
    private String server;
    private Integer port;
    private Integer database;
    private String password;
    private Long expired;
}
