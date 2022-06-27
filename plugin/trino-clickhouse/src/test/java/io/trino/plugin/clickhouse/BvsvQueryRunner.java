package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.FailureInjector;
import io.trino.execution.QueryManager;
import io.trino.metadata.*;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.testing.*;
import io.trino.transaction.TransactionManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;

public class BvsvQueryRunner implements QueryRunner {

    private static final Logger log = Logger.get(BvsvQueryRunner.class);
    private static final String ENVIRONMENT = "testing";

    public static final URI DISCOVERY_URI = URI.create("http://localhost:8080");

    private List<TestingTrinoServer> servers;

    private final TestingTrinoClient trinoClient;
    private final TestingTrinoServer coordinator;

    private final Closer closer = Closer.create();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public BvsvQueryRunner(
            Session defaultSession,

            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            String environment,
            Module additionalModule,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners
    ) throws Exception {

        Map<String, String> extraCoordinatorProperties = new HashMap<>();
        extraCoordinatorProperties.putAll(extraProperties);
        extraCoordinatorProperties.putAll(coordinatorProperties);

        try {
            ImmutableList.Builder<TestingTrinoServer> servers = ImmutableList.builder();

            if (!extraCoordinatorProperties.containsKey("web-ui.authentication.type")) {
                // Make it possible to use Trino UI when running multiple tests (or tests and SomeQueryRunner.main) at once.
                // This is necessary since cookies are shared (don't discern port number) and logging into one instance logs you out from others.
                extraCoordinatorProperties.put("web-ui.authentication.type", "fixed");
                extraCoordinatorProperties.put("web-ui.user", "admin");
            }

            coordinator  = closer.register(createTestingTrinoServer(
                    DISCOVERY_URI,
                    true,
                    extraCoordinatorProperties,
                    environment,
                    additionalModule,
                    baseDataDir,
                    systemAccessControls,
                    eventListeners
            ));
            servers.add(coordinator);

            this.servers = servers.build();

            this.trinoClient = closer.register(new TestingTrinoClient(coordinator, defaultSession));

        } catch (Exception e) {
            try {
                throw closer.rethrow(e, Exception.class);
            }
            finally {
                closer.close();
            }
        }
    }


    @Override
    public void close() {
        cancelAllQueries();
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void cancelAllQueries()
    {
        QueryManager queryManager = coordinator.getQueryManager();
        for (BasicQueryInfo queryInfo : queryManager.getQueries()) {
            if (!queryInfo.getState().isDone()) {
                try {
                    queryManager.cancelQuery(queryInfo.getQueryId());
                }
                catch (RuntimeException e) {
                    log.warn(e, "Failed to cancel query");
                }
            }
        }
    }

    @Override
    public int getNodeCount() {
        return servers.size();
    }

    @Override
    public Session getDefaultSession() {
        return trinoClient.getDefaultSession();
    }

    @Override
    public TransactionManager getTransactionManager() {
        return coordinator.getTransactionManager();
    }

    @Override
    public Metadata getMetadata() {
        return coordinator.getMetadata();
    }

    @Override
    public TypeManager getTypeManager() {
        return coordinator.getTypeManager();
    }

    @Override
    public QueryExplainer getQueryExplainer() {
        return coordinator.getQueryExplainer();
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager() {
        return coordinator.getSessionPropertyManager();
    }

    @Override
    public FunctionManager getFunctionManager() {
        return coordinator.getFunctionManager();
    }

    @Override
    public SplitManager getSplitManager() {
        return coordinator.getSplitManager();
    }

    @Override
    public PageSourceManager getPageSourceManager() {
        return coordinator.getPageSourceManager();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager() {
        return coordinator.getNodePartitioningManager();
    }

    @Override
    public StatsCalculator getStatsCalculator() {
        return coordinator.getStatsCalculator();
    }

    @Override
    public TestingGroupProvider getGroupProvider() {
        return coordinator.getGroupProvider();
    }

    @Override
    public TestingAccessControlManager getAccessControl() {
        return coordinator.getAccessControl();
    }

    @Override
    public MaterializedResult execute(String sql) {
        lock.readLock().lock();
        try {
            return trinoClient.execute(sql).getResult();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResult execute(Session session, String sql) {
        lock.readLock().lock();
        try {
            return trinoClient.execute(session, sql).getResult();
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("SQL: " + sql));
            throw e;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema) {
        lock.readLock().lock();
        try {
            return trinoClient.listTables(session, catalog, schema);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean tableExists(Session session, String table) {
        lock.readLock().lock();
        try {
            return trinoClient.tableExists(session, table);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void installPlugin(Plugin plugin) {
        long start = System.nanoTime();
        for (TestingTrinoServer server : servers) {
            server.installPlugin(plugin);
        }
        log.info("Installed plugin %s in %s", plugin.getClass().getSimpleName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    @Override
    public void addFunctions(FunctionBundle functionBundle) {
        servers.forEach(server -> server.addFunctions(functionBundle));
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties) {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }


    @Override
    public Lock getExclusiveLock() {
        return lock.writeLock();
    }

    @Override
    public void injectTaskFailure(String traceToken, int stageId, int partitionId, int attemptId, FailureInjector.InjectedFailureType injectionType, Optional<ErrorType> errorType) {
        for (TestingTrinoServer server : servers) {
            server.injectTaskFailure(
                    traceToken,
                    stageId,
                    partitionId,
                    attemptId,
                    injectionType,
                    errorType);
        }
    }

    @Override
    public void loadExchangeManager(String name, Map<String, String> properties) {
        for (TestingTrinoServer server : servers) {
            server.loadExchangeManager(name, properties);
        }
    }


    private static TestingTrinoServer createTestingTrinoServer(
            URI discoveryUri,
            boolean coordinator,
            Map<String, String> extraProperties,
            String environment,
            Module additionalModule,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners)
    {
        long start = System.nanoTime();
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                // Use few threads in tests to preserve resources on CI
                .put("discovery.http-client.min-threads", "1") // default 8
                .put("exchange.http-client.min-threads", "1") // default 8
                .put("node-manager.http-client.min-threads", "1") // default 8
                .put("exchange.page-buffer-client.max-callback-threads", "5") // default 25
                .put("exchange.http-client.idle-timeout", "1h")
                .put("task.max-index-memory", "16kB") // causes index joins to fault load
                .put("distributed-index-joins-enabled", "true");
        if (coordinator) {
            propertiesBuilder.put("node-scheduler.include-coordinator", "true");
            propertiesBuilder.put("join-distribution-type", "PARTITIONED");

            // Use few threads in tests to preserve resources on CI
            propertiesBuilder.put("failure-detector.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("memoryManager.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("scheduler.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("workerInfo.http-client.min-threads", "1"); // default 8
        }
        HashMap<String, String> properties = new HashMap<>(propertiesBuilder.buildOrThrow());
        properties.putAll(extraProperties);

        TestingTrinoServer server = TestingTrinoServer.builder()
                .setCoordinator(coordinator)
                .setProperties(properties)
                .setEnvironment(environment)
                .setDiscoveryUri(discoveryUri)
                .setAdditionalModule(additionalModule)
                .setBaseDataDir(baseDataDir)
                .setSystemAccessControls(systemAccessControls)
                .setEventListeners(eventListeners)
                .build();

        String nodeRole = coordinator ? "coordinator" : "worker";
        log.info("Created %s TestingTrinoServer in %s: %s", nodeRole, nanosSince(start).convertToMostSuccinctTimeUnit(), server.getBaseUrl());

        return server;
    }

    public static Builder<?> builder(Session defaultSession)
    {
        return new Builder<>(defaultSession);
    }

    public static class Builder<SELF extends Builder<?>> {
        private Session defaultSession;
        private Map<String, String> extraProperties = new HashMap<>();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private Optional<Map<String, String>> backupCoordinatorProperties = Optional.empty();
        private Consumer<QueryRunner> additionalSetup = querRunner -> {};
        private String environment = ENVIRONMENT;
        private Module additionalModule = EMPTY_MODULE;
        private Optional<Path> baseDataDir = Optional.empty();
        private List<SystemAccessControl> systemAccessControls = ImmutableList.of();
        private List<EventListener> eventListeners = ImmutableList.of();

        protected Builder(Session defaultSession)
        {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        }

        public SELF amendSession(Function<Session.SessionBuilder, Session.SessionBuilder> amendSession)
        {
            Session.SessionBuilder builder = Session.builder(defaultSession);
            this.defaultSession = amendSession.apply(builder).build();
            return self();
        }

        public SELF setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = new HashMap<>(extraProperties);
            return self();
        }

        public SELF addExtraProperty(String key, String value)
        {
            this.extraProperties.put(key, value);
            return self();
        }

        public SELF setCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = coordinatorProperties;
            return self();
        }

        public SELF setBackupCoordinatorProperties(Map<String, String> backupCoordinatorProperties)
        {
            this.backupCoordinatorProperties = Optional.of(backupCoordinatorProperties);
            return self();
        }

        /**
         * Additional configuration to be applied on {@link QueryRunner} being built.
         * Invoked after engine configuration is applied, but before connector-specific configurations
         * (if any) are applied.
         */
        public SELF setAdditionalSetup(Consumer<QueryRunner> additionalSetup)
        {
            this.additionalSetup = requireNonNull(additionalSetup, "additionalSetup is null");
            return self();
        }

        /**
         * Sets coordinator properties being equal to a map containing given key and value.
         * Note, that calling this method OVERWRITES previously set property values.
         * As a result, it should only be used when only one coordinator property needs to be set.
         */
        public SELF setSingleCoordinatorProperty(String key, String value)
        {
            return setCoordinatorProperties(ImmutableMap.of(key, value));
        }

        public SELF setEnvironment(String environment)
        {
            this.environment = environment;
            return self();
        }

        public SELF setAdditionalModule(Module additionalModule)
        {
            this.additionalModule = requireNonNull(additionalModule, "additionalModules is null");
            return self();
        }

        public SELF setBaseDataDir(Optional<Path> baseDataDir)
        {
            this.baseDataDir = requireNonNull(baseDataDir, "baseDataDir is null");
            return self();
        }

        @SuppressWarnings("unused")
        public SELF setSystemAccessControl(SystemAccessControl systemAccessControl)
        {
            return setSystemAccessControls(ImmutableList.of(requireNonNull(systemAccessControl, "systemAccessControl is null")));
        }

        @SuppressWarnings("unused")
        public SELF setSystemAccessControls(List<SystemAccessControl> systemAccessControls)
        {
            this.systemAccessControls = ImmutableList.copyOf(requireNonNull(systemAccessControls, "systemAccessControls is null"));
            return self();
        }

        @SuppressWarnings("unused")
        public SELF setEventListener(EventListener eventListener)
        {
            return setEventListeners(ImmutableList.of(requireNonNull(eventListener, "eventListener is null")));
        }

        @SuppressWarnings("unused")
        public SELF setEventListeners(List<EventListener> eventListeners)
        {
            this.eventListeners = ImmutableList.copyOf(requireNonNull(eventListeners, "eventListeners is null"));
            return self();
        }

        public SELF enableBackupCoordinator()
        {
            if (backupCoordinatorProperties.isEmpty()) {
                setBackupCoordinatorProperties(ImmutableMap.of());
            }
            return self();
        }

        @SuppressWarnings("unchecked")
        protected SELF self()
        {
            return (SELF) this;
        }

        public BvsvQueryRunner build() throws Exception {
            BvsvQueryRunner queryRunner = new BvsvQueryRunner(
                    defaultSession,
                    extraProperties,
                    coordinatorProperties,
                    environment,
                    additionalModule,
                    baseDataDir,
                    systemAccessControls,
                    eventListeners);

            try {
                additionalSetup.accept(queryRunner);
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }

            return queryRunner;
        }
    }
}
