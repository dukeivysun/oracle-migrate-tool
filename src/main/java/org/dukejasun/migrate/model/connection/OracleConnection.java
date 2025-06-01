package org.dukejasun.migrate.model.connection;

import org.dukejasun.migrate.utils.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.sql.*;
import java.util.Objects;

/**
 * @author dukedpsun
 */
@Slf4j
public class OracleConnection implements Closeable {

    private final String url;
    private final String userName;
    private final String password;
    private Connection connection;

    public OracleConnection(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.connection = RetryUtil.executeWithRetry(() -> DriverManager.getConnection(url, userName, password), 6, 2000L, false);
    }

    public Connection connection() throws SQLException {
        if (Objects.isNull(connection) || connection.isClosed()) {
            this.connection = DriverManager.getConnection(url, userName, password);
        }
        return connection;
    }

    public void executeWithoutCommitting(String @NotNull ... scripts) throws SQLException {
        Connection connection = connection();
        try (Statement statement = connection.createStatement()) {
            for (String script : scripts) {
                statement.execute(script);
            }
        }
    }

    public void query(String query, ResultSetConsumer resultConsumer) throws SQLException {
        query(query, Connection::createStatement, resultConsumer);
    }

    public void query(String query, StatementFactory statementFactory, ResultSetConsumer resultConsumer) throws SQLException {
        Connection connection = connection();
        try (Statement statement = statementFactory.createStatement(connection); ResultSet resultSet = statement.executeQuery(query)) {
            if (Objects.nonNull(resultConsumer)) {
                resultConsumer.accept(resultSet);
            }
        }
    }

    public <T> T queryAndMap(String query, ResultSetMapper<T> mapper) throws SQLException {
        return queryAndMap(query, Connection::createStatement, mapper);
    }

    public <T> T queryAndMap(String query, StatementFactory statementFactory, ResultSetMapper<T> mapper) throws SQLException {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        try (Statement statement = statementFactory.createStatement(connection()); ResultSet resultSet = statement.executeQuery(query)) {
            return mapper.apply(resultSet);
        }
    }

    @Override
    public void close() {
        if (Objects.nonNull(connection)) {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }

    public interface ResultSetConsumer {
        /**
         * 处理结果集
         *
         * @param resultSet
         * @throws SQLException
         */
        void accept(ResultSet resultSet) throws SQLException;
    }

    @FunctionalInterface
    public interface StatementFactory {
        /**
         * 使用给定的连接创建语句
         *
         * @param connection
         * @return
         * @throws SQLException
         */
        Statement createStatement(Connection connection) throws SQLException;
    }

    public interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }
}
