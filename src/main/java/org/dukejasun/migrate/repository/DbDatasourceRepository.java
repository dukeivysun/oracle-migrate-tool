package org.dukejasun.migrate.repository;

import org.dukejasun.migrate.model.entity.DbDatasource;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

/**
 * @author dukedpsun
 */
@Repository("dbDatasourceRepository")
public interface DbDatasourceRepository extends JpaRepository<DbDatasource, Integer>, JpaSpecificationExecutor<DbDatasource> {

}
