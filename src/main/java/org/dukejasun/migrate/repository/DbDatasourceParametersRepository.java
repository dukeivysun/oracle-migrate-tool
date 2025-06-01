package org.dukejasun.migrate.repository;

import org.dukejasun.migrate.model.entity.DbDatasourceParameters;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * @author dukedpsun
 */
@Repository("dbDatasourceParametersRepository")
public interface DbDatasourceParametersRepository extends JpaRepository<DbDatasourceParameters, Integer>, JpaSpecificationExecutor<DbDatasourceParameters> {

    /**
     * 通过数据库类型查询参数配置
     *
     * @param dbType
     * @return
     */
    Optional<DbDatasourceParameters> findByDbType(String dbType);
}
