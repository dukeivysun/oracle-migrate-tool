package org.dukejasun.migrate.handler;

import org.dukejasun.migrate.model.entity.DbMigrateTask;
import org.dukejasun.migrate.repository.DbMigrateTaskRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author dukedpsun
 */
@Component("dbMigrateTaskHandler")
public class DbMigrateTaskHandler {

    private final DbMigrateTaskRepository dbMigrateTaskRepository;

    @Autowired
    public DbMigrateTaskHandler(DbMigrateTaskRepository dbMigrateTaskRepository) {
        this.dbMigrateTaskRepository = dbMigrateTaskRepository;
    }

    /**
     * 通过任务Id查询任务
     *
     * @param taskId
     * @return
     */
    public DbMigrateTask findMigrateTaskByTaskId(String taskId) {
        return dbMigrateTaskRepository.findById(Integer.valueOf(taskId)).orElse(null);
    }

    public DbMigrateTask findByIdAndProcessId(String taskId, String processId) {
        return dbMigrateTaskRepository.findByIdAndComments(Integer.valueOf(taskId), processId).orElse(null);
    }

    @Transactional(rollbackFor = Exception.class)
    public void saveMigrateTask(DbMigrateTask dbMigrateTask) {
        dbMigrateTaskRepository.save(dbMigrateTask);
    }
}
