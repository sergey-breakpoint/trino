package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import static io.trino.plugin.clickhouse.BvsvClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;

public class TestBvsvClickHouseConnectorTest extends BvsvBaseClickHouseConnectorTest {

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception {
        this.clickhouseServer = closeAfterClass(new TestingBvsvClickHouseServer());
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("clickhouse.map-string-as-varchar", "true")
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @Test
    public void truncateSupportTest() {

        String tableName = "test_truncate_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);
        assertUpdate("TRUNCATE TABLE " + tableName);
    }

    @Override
    protected SqlExecutor onRemoteDatabase() {
        return clickhouseServer::execute;
    }
}
