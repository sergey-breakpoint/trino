package io.trino.plugin.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingBvsvClickHouseServer extends CommonTestingClickHouseServer {

    public TestingBvsvClickHouseServer() {
        System.out.println("Run!");
    }

    @Override
    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl());
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    @Override
    public String getJdbcUrl()
    {
        return format("jdbc:clickhouse://%s:%s/", "127.0.0.1", "8123");
    }

    @Override
    public void close() {
        System.out.println("Close!");
    }
}
