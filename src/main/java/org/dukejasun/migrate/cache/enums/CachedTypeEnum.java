package org.dukejasun.migrate.cache.enums;

/**
 * @author dukedpsun
 */
public enum CachedTypeEnum {
    /**
     * redis
     */
    REDIS("REDIS"),
    /**
     * memcached
     */
    MEMCACHED("MEMCACHED"),
    /**
     * caffeine
     */
    CAFFEINE("CAFFEINE"),
    /**
     * jvm
     */
    JVM("JVM");
    private final String code;

    CachedTypeEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static CachedTypeEnum getCachedType(String cachedTypeValue) {
        for (CachedTypeEnum cachedTypeEnum : CachedTypeEnum.values()) {
            if (cachedTypeValue.equalsIgnoreCase(cachedTypeEnum.getCode())) {
                return cachedTypeEnum;
            }
        }
        return CachedTypeEnum.JVM;
    }
}
