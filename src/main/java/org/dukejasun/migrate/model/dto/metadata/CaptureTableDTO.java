package org.dukejasun.migrate.model.dto.metadata;

import com.google.common.collect.Lists;
import lombok.*;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CaptureTableDTO implements Serializable {
    private static final long serialVersionUID = -7691508000536971961L;
    private Integer tableId;
    private String schema;
    private String name;
    private String targetSchema;
    private String targetName;
    private String comments;
    private String encoding;
    private String tablespaceName;
    private String flashBack;
    private String conditions;
    private LongAdder longAdder;
    private LongAdder captureLongAddr;
    private LongAdder sinkLongAddr;
    private List<String> dataTypeList;
    private List<String> primaryKeyList;
    private List<String> indexNameList;
    private List<String> partitionNameList;
    private List<CaptureColumnDTO> columnMetaList;
    private LocalDateTime startTime = LocalDateTime.now();
    private LocalDateTime endTime = LocalDateTime.now();

    public void addIndexNameList(String indexName) {
        if (CollectionUtils.isEmpty(indexNameList)) {
            indexNameList = Lists.newArrayList();
        }
        indexNameList.add(indexName);
    }

    public void addPartitionNameList(String partitionName) {
        if (CollectionUtils.isEmpty(partitionNameList)) {
            partitionNameList = Lists.newArrayList();
        }
        partitionNameList.add(partitionName);
    }

}
