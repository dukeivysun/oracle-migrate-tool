package org.dukejasun.migrate.repository;

import org.dukejasun.migrate.model.entity.DbIncrementRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * @author dukedpsun
 */
@Repository("dbIncrementRecordRepository")
public interface DbIncrementRecordRepository extends JpaRepository<DbIncrementRecord, Integer>, JpaSpecificationExecutor<DbIncrementRecord> {

    /**
     * 通过任务Id查找同步记录
     *
     * @param taskId
     * @return
     */
    Optional<DbIncrementRecord> findByTaskId(String taskId);
}
