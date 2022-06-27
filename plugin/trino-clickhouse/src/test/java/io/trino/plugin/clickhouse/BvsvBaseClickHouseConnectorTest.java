package io.trino.plugin.clickhouse;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.TestingConnectorBehavior;

public abstract class BvsvBaseClickHouseConnectorTest extends BaseJdbcConnectorTest {

    protected CommonTestingClickHouseServer clickhouseServer;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
                return false;

            case SUPPORTS_NEGATIVE_DATE:
                return false;

            case SUPPORTS_DELETE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
