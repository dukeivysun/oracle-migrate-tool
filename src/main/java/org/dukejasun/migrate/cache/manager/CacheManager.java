package org.dukejasun.migrate.cache.manager;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dukejasun.migrate.cache.enums.CachedTypeEnum;
import org.dukejasun.migrate.cache.impl.LocalCacheOperation;
import org.dukejasun.migrate.cache.model.DataCacheConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("cacheManager")
public class CacheManager {
    private final DataCacheConfig dataCacheConfig;

    @Autowired
    public CacheManager(DataCacheConfig dataCacheConfig) {
        this.dataCacheConfig = dataCacheConfig;
    }

    @Bean
    public LocalCacheOperation cachedOperation() {
        CachedTypeEnum cachedTypeEnum = CachedTypeEnum.getCachedType(StringUtils.isBlank(dataCacheConfig.getType()) ? CachedTypeEnum.JVM.getCode() : dataCacheConfig.getType());
        return new LocalCacheOperation(dataCacheConfig);
    }
}
