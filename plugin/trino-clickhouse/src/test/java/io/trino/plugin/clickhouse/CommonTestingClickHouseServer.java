package io.trino.plugin.clickhouse;

import java.io.Closeable;

public abstract class CommonTestingClickHouseServer implements Closeable {

    protected abstract void execute(String sql);

    protected abstract String getJdbcUrl();
}
