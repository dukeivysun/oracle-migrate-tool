package org.dukejasun.migrate.model.dto.entity;

import com.google.common.collect.Lists;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import lombok.*;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class DatasourceDTO implements Serializable {
    private static final long serialVersionUID = -8460274533357277316L;
    private boolean isCluster = false;
    private String name;
    private String dataSourceId;
    private String version;
    private String username;
    private String password;
    private String host;
    private Integer port;
    private String pdbName;
    private String databaseName;
    private String encode;
    private DatabaseExpandType type;
    private Map<String, Object> parameterMap;
    private List<DatasourceDTO> clusterList;

    @Override
    public int hashCode() {
        int result = 31;
        if (StringUtils.isNotBlank(dataSourceId)) {
            result = dataSourceId.hashCode();
        }
        result = 17 * result + username.hashCode();
        result = 17 * result + password.hashCode();
        result = 17 * result + host.hashCode();
        result = 17 * result + port.hashCode();
        result = 17 * result + databaseName.hashCode();
        result = 17 * result + version.hashCode();
        result = 17 * result + type.name().hashCode();
        return result;
    }

    public DatasourceDTO copyFromCurrentObject() {
        return new DatasourceDTO().setVersion(this.getVersion())
                .setUsername(this.getUsername())
                .setPassword(this.getPassword())
                .setPort(this.getPort())
                .setPdbName(this.getPdbName())
                .setDatabaseName(this.getDatabaseName())
                .setType(this.getType()).setEncode(this.getEncode());
    }

    public void addClusterList(DatasourceDTO datasourceDTO) {
        if (CollectionUtils.isEmpty(clusterList)) {
            clusterList = Lists.newLinkedList();
        }
        if (Objects.nonNull(datasourceDTO)) {
            clusterList.add(datasourceDTO);
            this.setCluster(true);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (Objects.isNull(object) || !(object instanceof DatasourceDTO)) {
            return false;
        }
        DatasourceDTO dataSourceDTO = (DatasourceDTO) object;
        if (Objects.nonNull(dataSourceId) && Objects.nonNull(dataSourceDTO.dataSourceId)) {
            return dataSourceId.equals(dataSourceDTO.getDataSourceId());
        }
        return this.username.equals(dataSourceDTO.getUsername()) && this.password.equals(dataSourceDTO.getPassword()) && this.version.equals(dataSourceDTO.getVersion()) && this.host.equals(dataSourceDTO.getHost()) && this.port.equals(dataSourceDTO.getPort()) && this.databaseName.equals(dataSourceDTO.getDatabaseName()) && this.type.name().equals(dataSourceDTO.getType().name());
    }

}
