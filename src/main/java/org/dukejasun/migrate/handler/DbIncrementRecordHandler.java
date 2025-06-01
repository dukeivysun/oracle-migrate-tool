package org.dukejasun.migrate.handler;

import corg.dukejasun.migrate.model.entity.DbIncrementRecord;
import corg.dukejasun.migrate.repository.DbIncrementRecordRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author dukedpsun
 */
@Component("dbIncrementRecordHandler")
public class DbIncrementRecordHandler {
    private final DbIncrementRecordRepository dbIncrementRecordRepository;

    @Autowired
    public DbIncrementRecordHandler(DbIncrementRecordRepository dbIncrementRecordRepository) {
        this.dbIncrementRecordRepository = dbIncrementRecordRepository;
    }

    public DbIncrementRecord findIncrementRecord(String taskId) {
        return dbIncrementRecordRepository.findByTaskId(taskId).orElse(null);
    }

    @Transactional(rollbackFor = Exception.class)
    public void save(DbIncrementRecord dbIncrementRecord) {
        dbIncrementRecordRepository.save(dbIncrementRecord);
    }

}
