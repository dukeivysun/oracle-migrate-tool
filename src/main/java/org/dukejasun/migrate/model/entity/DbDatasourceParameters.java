package org.dukejasun.migrate.model.entity;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.io.Serializable;

/**
 * @author dukedpsun
 */
@Getter
@Setter
@Entity
@Table(name = "t_datasource_parameters")
@DynamicInsert
@DynamicUpdate
public class DbDatasourceParameters implements Serializable {
    private static final long serialVersionUID = 2157156135298676118L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String dbType;
    private String parameters;
    private Boolean isDeleted;

}
