package io.trino.plugin.clickhouse;

import org.testng.annotations.Test;

import static io.trino.testing.sql.TestTable.randomTableSuffix;

public class TestBvsvClickHouseConnectorTest extends TestClickHouseConnectorTest {

    @Test
    public void truncateSupportTest() {

        String tableName = "test_truncate_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);
        assertUpdate("TRUNCATE TABLE " + tableName);
    }

    @Test
    public void dateTypeWriteAndReadTest() {
        String tableName = "test_date_table_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(orderdate date, orderstatus varchar) WITH (engine = 'LOG')");
        assertUpdate("INSERT INTO " + tableName + " VALUES( date '2022-01-01', 'NEW')", 1);
        assertQuery("SELECT orderdate FROM " + tableName, "VALUES(date '2022-01-01')");


    }

    @Test
    public void dateTypeWriteAndReadTestFail() {
        String tableName = "test_date_table_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(orderdate date, orderstatus varchar) WITH (engine = 'LOG')");
        assertQueryFails("INSERT INTO " + tableName + " VALUES( date '222-01-01', 'NEW')", "Date must be between 1970-01-01 and 2106-02-07 in ClickHouse: 0222-01-01");
    }
}
