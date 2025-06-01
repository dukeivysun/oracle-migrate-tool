package org.dukejasun.migrate.repository;

import org.dukejasun.migrate.model.entity.DbMigrateTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * @author dukedpsun
 */
@Repository("dbMigrateTaskRepository")
public interface DbMigrateTaskRepository extends JpaRepository<DbMigrateTask, Integer>, JpaSpecificationExecutor<DbMigrateTask> {

    /**
     * 通过Id和comments查询对象
     *
     * @param taskId
     * @param comments
     * @return
     */
    Optional<DbMigrateTask> findByIdAndComments(Integer taskId, String comments);
}
