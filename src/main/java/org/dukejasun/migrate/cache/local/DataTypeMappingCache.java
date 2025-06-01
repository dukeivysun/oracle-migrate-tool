package org.dukejasun.migrate.cache.local;

import com.google.common.collect.Lists;
import org.dukejasun.migrate.cache.LocalCache;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
public enum DataTypeMappingCache {
    /**
     *
     */
    INSTANCE;
    private final LinkedHashMap<String, List<DataTypeMappingDTO>> localCache = new LocalCache<>(65536);

    public void put(String key, DataTypeMappingDTO dataTypeMappingDTO) {
        List<DataTypeMappingDTO> dataTypeMappingDTOList = localCache.get(key);
        if (CollectionUtils.isEmpty(dataTypeMappingDTOList)) {
            dataTypeMappingDTOList = Lists.newArrayList();
            localCache.put(key, dataTypeMappingDTOList);
        }
        dataTypeMappingDTOList.add(dataTypeMappingDTO);
    }

    public List<DataTypeMappingDTO> get(String key) {
        return localCache.get(key);
    }

    public Map<String, DataTypeMappingDTO> getDataTypeMapping(String key) {
        List<DataTypeMappingDTO> dataTypeMappingDTOList = localCache.get(key);
        if (!CollectionUtils.isEmpty(dataTypeMappingDTOList)) {
            return dataTypeMappingDTOList.stream().collect(Collectors.toMap(DataTypeMappingDTO::getSourceName, Function.identity(), (key1, key2) -> key2));
        }
        return null;
    }

    public LinkedHashMap<String, List<DataTypeMappingDTO>> linkedHashMap() {
        return localCache;
    }
}
