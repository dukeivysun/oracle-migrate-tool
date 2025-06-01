package org.dukejasun.migrate.cache.local;

import com.google.common.collect.Lists;
import org.dukejasun.migrate.cache.LocalCache;
import org.dukejasun.migrate.model.dto.output.CompareResultDTO;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author dukedpsun
 */
public enum CompareResultCache {

    /**
     *
     */
    INSTANCE;
    private final LinkedHashMap<String, List<CompareResultDTO>> localCache = new LocalCache<>(65536);

    public void put(String key, CompareResultDTO compareResultDTO) {
        List<CompareResultDTO> compareResultDTOList = localCache.get(key);
        if (CollectionUtils.isEmpty(compareResultDTOList)) {
            compareResultDTOList = Lists.newArrayList();
            localCache.put(key, compareResultDTOList);
        }
        compareResultDTOList.add(compareResultDTO);
    }

    public List<CompareResultDTO> get(String key) {
        return localCache.get(key);
    }


    public LinkedHashMap<String, List<CompareResultDTO>> linkedHashMap() {
        return localCache;
    }
}
