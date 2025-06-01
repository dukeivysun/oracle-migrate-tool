package org.dukejasun.migrate.cache.local;

import com.google.common.collect.Lists;
import org.dukejasun.migrate.cache.LocalCache;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author dukedpsun
 */
public enum CaptureTableCache {
    /**
     *
     */
    INSTANCE;
    private final LinkedHashMap<String, CaptureTableDTO> localCache = new LocalCache<>(65536);
    private final LinkedHashMap<String, List<CaptureTableDTO>> schemaAndTablesCache = new LocalCache<>(65536);

    public CaptureTableDTO get(String schemaAndTable) {
        return localCache.get(schemaAndTable);
    }

    public synchronized void put(String schemaAndTable, CaptureTableDTO captureTableDTO) {
        localCache.put(schemaAndTable, captureTableDTO);
    }

    public synchronized void put0(String schema, CaptureTableDTO captureTableDTO) {
        List<CaptureTableDTO> captureTableList = schemaAndTablesCache.get(schema);
        if (CollectionUtils.isEmpty(captureTableList)) {
            captureTableList = Lists.newLinkedList();
            schemaAndTablesCache.put(schema, captureTableList);
        }
        captureTableList.add(captureTableDTO);
    }

    public LinkedHashMap<String, List<CaptureTableDTO>> get() {
        return schemaAndTablesCache;
    }

}
