package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class BvsvClickHouseQueryRunner {

    public static final String TPCH_SCHEMA = "tpch";

    public static BvsvQueryRunner createClickHouseQueryRunner(
            CommonTestingClickHouseServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables
    ) throws Exception {
        BvsvQueryRunner queryRunner = null;
        try {
            queryRunner = BvsvQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            //queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());

            queryRunner.installPlugin(new ClickHousePlugin());
            queryRunner.createCatalog("clickhouse", "clickhouse", connectorProperties);
            //queryRunner.execute("CREATE SCHEMA " + TPCH_SCHEMA);
            //copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}
