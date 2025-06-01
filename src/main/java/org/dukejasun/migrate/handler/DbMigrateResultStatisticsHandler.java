package org.dukejasun.migrate.handler;

import org.dukejasun.migrate.model.entity.DbMigrateResultStatistics;
import org.dukejasun.migrate.repository.DbMigrateResultStatisticsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @author dukedpsun
 */
@Component("dbMigrateResultStatisticsHandler")
public class DbMigrateResultStatisticsHandler {

    private final DbMigrateResultStatisticsRepository dbMigrateResultStatisticsRepository;

    @Autowired
    public DbMigrateResultStatisticsHandler(DbMigrateResultStatisticsRepository dbMigrateResultStatisticsRepository) {
        this.dbMigrateResultStatisticsRepository = dbMigrateResultStatisticsRepository;
    }

    public DbMigrateResultStatistics findByUniqueId(String taskId, String transmissionType, String schema, String tableName) {
        return dbMigrateResultStatisticsRepository.findByTaskIdAndTransmissionTypeAndSourceSchemaAndSourceTable(taskId, transmissionType, schema, tableName).orElse(null);
    }

    public List<DbMigrateResultStatistics> findAllByTaskIdAndTransmissionType(String taskId, String transmissionType) {
        return dbMigrateResultStatisticsRepository.findAllByTaskIdAndTransmissionType(taskId, transmissionType);
    }

    @Transactional(rollbackFor = Exception.class)
    public void save(DbMigrateResultStatistics dbMigrateResultStatistics) {
        dbMigrateResultStatisticsRepository.save(dbMigrateResultStatistics);
    }

    @Transactional(rollbackFor = Exception.class)
    public void deleteByTaskIdAndSourceSchemaAndSourceTable(String taskId, String schema, String tableName) {
        dbMigrateResultStatisticsRepository.deleteByTaskIdAndSourceSchemaAndSourceTable(taskId, schema, tableName);
    }

    @Transactional(rollbackFor = Exception.class)
    public void batchUpdateStatistics(List<DbMigrateResultStatistics> dbMigrateResultStatisticsList) {
        dbMigrateResultStatisticsRepository.batchUpdate(dbMigrateResultStatisticsList);
    }
}
