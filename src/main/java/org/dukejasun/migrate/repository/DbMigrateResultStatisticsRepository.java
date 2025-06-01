package org.dukejasun.migrate.repository;

import org.dukejasun.migrate.model.entity.DbMigrateResultStatistics;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * @author dukedpsun
 */
@Repository("dbMigrateResultStatisticsRepository")
public interface DbMigrateResultStatisticsRepository extends BaseRepository<DbMigrateResultStatistics, Integer>, JpaSpecificationExecutor<DbMigrateResultStatistics> {

    /**
     * 通过任务Id和迁移类型查询迁移结果
     *
     * @param taskId
     * @param transmissionType
     * @return
     */
    List<DbMigrateResultStatistics> findAllByTaskIdAndTransmissionType(String taskId, String transmissionType);

    /**
     * 通过任务Id、迁移类型、源schema和源表名称查询同步记录
     *
     * @param taskId
     * @param transmissionType
     * @param sourceSchema
     * @param sourceTable
     * @return
     */
    Optional<DbMigrateResultStatistics> findByTaskIdAndTransmissionTypeAndSourceSchemaAndSourceTable(String taskId, String transmissionType, String sourceSchema, String sourceTable);

    /**
     * 通过任务Id和源schema和源table删除数据
     *
     * @param taskId
     * @param sourceSchema
     * @param sourceTable
     */
    @Modifying
    void deleteByTaskIdAndSourceSchemaAndSourceTable(String taskId, String sourceSchema, String sourceTable);

}
