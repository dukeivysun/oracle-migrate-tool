package org.dukejasun.migrate.cache.enums;

import lombok.Getter;

/**
 * @author dukedpsun
 */
@Getter
public enum CachedTypeEnum {
    /**
     * jvm
     */
    JVM("JVM");
    private final String code;

    CachedTypeEnum(String code) {
        this.code = code;
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
