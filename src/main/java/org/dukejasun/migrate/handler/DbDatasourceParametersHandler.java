package org.dukejasun.migrate.handler;

import org.dukejasun.migrate.model.entity.DbDatasourceParameters;
import org.dukejasun.migrate.repository.DbDatasourceParametersRepository;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author dukedpsun
 */
@Component("dbDatasourceParametersHandler")
public class DbDatasourceParametersHandler {
    private final DbDatasourceParametersRepository dbDatasourceParametersRepository;

    public DbDatasourceParametersHandler(DbDatasourceParametersRepository dbDatasourceParametersRepository) {
        this.dbDatasourceParametersRepository = dbDatasourceParametersRepository;
    }

    public Optional<DbDatasourceParameters> getDatasourceParameter(String databaseType) {
        return dbDatasourceParametersRepository.findByDbType(databaseType);
    }
}
