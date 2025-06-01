package org.dukejasun.migrate.cache.local;

import com.google.common.collect.Lists;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;

import java.util.List;

/**
 * @author dukedpsun
 */
public enum TableMetadataCache {
    /**
     *
     */
    INSTANCE;
    private List<CaptureTableDTO> metadataList = Lists.newLinkedList();

    public void add(List<CaptureTableDTO> metadataList) {
        this.metadataList = metadataList;
    }

    public List<CaptureTableDTO> get() {
        return metadataList;
    }

}
