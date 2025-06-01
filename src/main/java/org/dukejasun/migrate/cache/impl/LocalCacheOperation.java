package org.dukejasun.migrate.cache.impl;

import org.dukejasun.migrate.cache.CachedOperation;
import org.dukejasun.migrate.cache.LocalCache;
import org.dukejasun.migrate.cache.model.DataCacheConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author dukedpsun
 */
@Slf4j
public class LocalCacheOperation implements CachedOperation<String, LinkedList<Object>> {

    private Map<String, LinkedList<Object>> localCache;

    public LocalCacheOperation(DataCacheConfig dataCacheConfig) {
        localCache = new LocalCache<>(4096);
    }

    @Override
    public synchronized void set(String key, LinkedList<Object> linkedList) {
        if (!CollectionUtils.isEmpty(localCache)) {
            localCache.put(key, linkedList);
        }
    }

    @Override
    public synchronized void put(String key, Object value) {
        if (!CollectionUtils.isEmpty(localCache)) {
            LinkedList<Object> linkedList = get(key);
            if (!linkedList.contains(value)) {
                linkedList.add(value);
            }
        }
    }

    @Override
    public void set(String key, LinkedList value, long timeout, TimeUnit unit) {

    }

    @Override
    public synchronized LinkedList<Object> get(String key) {
        if (!CollectionUtils.isEmpty(localCache)) {
            return localCache.get(key);
        }
        return null;
    }

    @Override
    public Boolean testConnection() {
        return Objects.nonNull(localCache);
    }

    @Override
    public synchronized Boolean delete(String key) {
        if (!CollectionUtils.isEmpty(localCache)) {
            localCache.remove(key);
        }
        return false;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!CollectionUtils.isEmpty(localCache)) {
            localCache.clear();
            localCache = null;
        }
    }
}
