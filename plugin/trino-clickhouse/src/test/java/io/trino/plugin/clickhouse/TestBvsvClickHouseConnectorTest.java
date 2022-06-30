package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import java.util.Map;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;

public class TestBvsvClickHouseConnectorTest extends BaseJdbcConnectorTest {

    private static final Logger log = Logger.get(TestBvsvClickHouseConnectorTest.class);

    private QueryRunner queryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception {
        log.info("Init QueryRunner!");

        queryRunner = LocalQueryRunner.create(testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema("system")
                .build());
        queryRunner.installPlugin(new ClickHousePlugin());
        queryRunner.createCatalog("clickhouse", "clickhouse", createProperties());
        return queryRunner;

    }

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", "jdbc:clickhouse://172.18.0.5:8123/")
                .put("connection-user", "clickhouse-user")
                .put("connection-password", "secret")
                .buildOrThrow();
    }


    @Test
    public void truncateSupportTest() {
        log.info("Start test!");
        String tableName = "test_truncate_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);
        assertUpdate("TRUNCATE TABLE " + tableName);
    }


    @Test
    public void test2() {
        String tableName = "test_truncate_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " as select 1 a, 2 b ");
    }

    @Override
    protected SqlExecutor onRemoteDatabase() {
        return queryRunner::execute;
    }
}
