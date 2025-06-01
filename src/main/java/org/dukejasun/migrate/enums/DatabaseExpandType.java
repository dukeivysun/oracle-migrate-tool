package org.dukejasun.migrate.enums;

import lombok.Getter;
import org.jetbrains.annotations.Nullable;

/**
 * @author dukedpsun
 */
@Getter
public enum DatabaseExpandType {
    /**
     * mysql
     */
    MYSQL("mysql", "com.mysql.cj.jdbc.Driver", "jdbc:mysql://{0}:{1}/{2}", "select 1"),
    /**
     * oracle
     */
    ORACLE("oracle", "oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@//{0}:{1}/{2}", "select 1 from dual");
    private final String type;
    private final String driver;
    private final String urlTemplate;
    private final String testQuery;

    DatabaseExpandType(String type, String driver, String urlTemplate, String testQuery) {
        this.type = type;
        this.driver = driver;
        this.urlTemplate = urlTemplate;
        this.testQuery = testQuery;
    }

    public static @Nullable DatabaseExpandType getDatabaseType(String dbType) {
        for (DatabaseExpandType databaseType : DatabaseExpandType.values()) {
            if (dbType.equalsIgnoreCase(databaseType.getType())) {
                return databaseType;
            }
        }
        return null;
    }
}

