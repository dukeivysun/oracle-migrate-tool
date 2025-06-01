package org.dukejasun.migrate.repository;

import org.dukejasun.migrate.model.entity.DbIncrementMetrics;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * @author dukedpsun
 */
@Repository("dbIncrementMetricsRepository")
public interface DbIncrementMetricsRepository extends JpaRepository<DbIncrementMetrics, Long>, JpaSpecificationExecutor<DbIncrementMetrics> {

    /**
     * 通过任务Id，目标schema和目标表名查询
     *
     * @param tasId
     * @param targetSchema
     * @param targetTable
     * @return
     */
    Optional<DbIncrementMetrics> findByTaskIdAndTargetSchemaAndTargetTable(String tasId, String targetSchema, String targetTable);

    List<DbIncrementMetrics> findAllByTaskId(String taskId);
}
