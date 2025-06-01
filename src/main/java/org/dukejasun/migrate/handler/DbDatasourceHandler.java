package org.dukejasun.migrate.handler;

import org.dukejasun.migrate.model.entity.DbDatasource;
import org.dukejasun.migrate.repository.DbDatasourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author dukedpsun
 */
@Component("dbDatasourceHandler")
public class DbDatasourceHandler {
    private final DbDatasourceRepository dbDatasourceRepository;

    @Autowired
    public DbDatasourceHandler(DbDatasourceRepository dbDatasourceRepository) {
        this.dbDatasourceRepository = dbDatasourceRepository;
    }

    public DbDatasource findDatasourceById(String sourceId) {
        return dbDatasourceRepository.findById(Integer.valueOf(sourceId)).orElse(null);
    }

    @Transactional(rollbackFor = Exception.class)
    public Integer save(DbDatasource dbDatasource) {
        return dbDatasourceRepository.save(dbDatasource).getId();
    }
}

