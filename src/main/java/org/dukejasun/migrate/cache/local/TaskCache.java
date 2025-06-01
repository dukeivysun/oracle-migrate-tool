package org.dukejasun.migrate.cache.local;


import org.dukejasun.migrate.cache.LocalCache;

/**
 * @author dukedpsun
 */
public enum TaskCache {
    /**
     *
     */
    INSTANCE;

    private final LocalCache<String, String> localCache = new LocalCache<>(16);

    public void put(String key, String pId) {
        localCache.put(key, pId);
    }

    public String get(String key) {
        return localCache.get(key);
    }

    public LocalCache<String, String> getAll() {
        return localCache;
    }
}
