package io.trino.plugin.clickhouse;

import org.testng.annotations.Test;

import static io.trino.testing.sql.TestTable.randomTableSuffix;

public class TestBvsvClickHouseConnectorTest extends TestClickHouseConnectorTest {

    @Test
    public void truncateSupportTest() {

        String tableName = "test_drop_column_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);
        assertUpdate("TRUNCATE TABLE " + tableName);
    }
}
