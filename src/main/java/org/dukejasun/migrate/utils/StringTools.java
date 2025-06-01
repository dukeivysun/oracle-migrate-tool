package org.dukejasun.migrate.utils;

import org.dukejasun.migrate.cache.local.DataTypeMappingCache;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * @author dukedpsun
 */
public class StringTools {

    /**
     * 替换括号
     *
     * @param context
     * @param searchString
     * @param replacement
     * @return
     */
    public static String replaceAll(@NotNull String context, final String searchString, final String replacement, String migrateType) {
        int head = context.indexOf('(');
        if (head != -1) {
            int next = head + 1;
            int count = 1;
            do {
                if (context.charAt(next) == '(') {
                    count++;
                } else if (context.charAt(next) == ')') {
                    count--;
                }
                next++;
                if (count == 0) {
                    String temp = context.substring(head, next);
                    context = StringUtils.replace(context, temp, Constants.DOUBLE_QUOTATION);
                    head = context.indexOf('(');
                    next = head + 1;
                    count = 1;
                }
            } while (head != -1);
        }
        String dataType = StringUtils.replace(context, searchString, replacement);

        return getMappingType(dataType, migrateType);
    }

    public static boolean equalsIgnoreCase(String @NotNull [] items, String value) {
        for (String item : items) {
            if (item.equalsIgnoreCase(value.trim())) {
                return true;
            }
        }
        return false;
    }

    private static String getMappingType(String dataType, String migrateType) {
        List<DataTypeMappingDTO> dataTypeMappingDTOList = DataTypeMappingCache.INSTANCE.get(migrateType);
        for (DataTypeMappingDTO dataTypeMappingDTO : dataTypeMappingDTOList) {
            if (dataTypeMappingDTO.getSourceName().equals(dataType)) {
                return dataTypeMappingDTO.getTargetName();
            }
        }
        return dataType;
    }

}
