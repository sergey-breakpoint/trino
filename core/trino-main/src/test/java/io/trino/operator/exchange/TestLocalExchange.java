/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.SequencePageBuilder;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PageAssertions;
import io.trino.operator.PipelineExecutionStrategy;
import io.trino.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.trino.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.trino.operator.exchange.LocalExchange.LocalExchangeSinkFactoryId;
import io.trino.spi.Page;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.testing.TestingTransactionHandle;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static io.trino.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLocalExchange
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final DataSize RETAINED_PAGE_SIZE = DataSize.ofBytes(createPage(42).getRetainedSizeInBytes());
    private static final DataSize LOCAL_EXCHANGE_MAX_BUFFERED_BYTES = DataSize.of(32, DataSize.Unit.MEGABYTE);
    private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(new TypeOperators());
    private static final Session SESSION = testSessionBuilder().build();

    private final ConcurrentMap<CatalogName, ConnectorNodePartitioningProvider> partitionManagers = new ConcurrentHashMap<>();
    private NodePartitioningManager nodePartitioningManager;

    @BeforeMethod
    public void setUp()
    {
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(new FinalizerService())));
        nodePartitioningManager = new NodePartitioningManager(
                nodeScheduler,
                new BlockTypeOperators(new TypeOperators()),
                catalogName -> {
                    ConnectorNodePartitioningProvider result = partitionManagers.get(catalogName);
                    checkArgument(result != null, catalogName + " does not have a partition manager");
                    return result;
                });
    }

    @DataProvider
    public static Object[][] executionStrategy()
    {
        return new Object[][] {{UNGROUPED_EXECUTION}, {GROUPED_EXECUTION}};
    }

    @Test(dataProvider = "executionStrategy")
    public void testGatherSingleWriter(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                SINGLE_DISTRIBUTION,
                8,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                DataSize.ofBytes(retainedSizeOfPages(99)),
                TYPE_OPERATOR_FACTORY);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 1);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);

            LocalExchangeSource source = exchange.getSource(0);
            assertSource(source, 0);

            LocalExchangeSink sink = sinkFactory.createSink();
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            assertSinkCanWrite(sink);
            assertSource(source, 0);

            // add the first page which should cause the reader to unblock
            ListenableFuture<Void> readFuture = source.waitForReading();
            assertFalse(readFuture.isDone());
            sink.addPage(createPage(0));
            assertTrue(readFuture.isDone());
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertSource(source, 1);

            sink.addPage(createPage(1));
            assertSource(source, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(source, createPage(0));
            assertSource(source, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(source, createPage(1));
            assertSource(source, 0);
            assertExchangeTotalBufferedBytes(exchange, 0);

            sink.addPage(createPage(2));
            sink.addPage(createPage(3));
            assertSource(source, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sink.finish();
            assertSinkFinished(sink);

            assertSource(source, 2);

            assertRemovePage(source, createPage(2));
            assertSource(source, 1);
            assertSinkFinished(sink);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(source, createPage(3));
            assertSourceFinished(source);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testBroadcast(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_BROADCAST_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sinkA = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkB);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            sinkA.addPage(createPage(0));

            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 0);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sinkA.finish();
            assertSinkFinished(sinkA);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sinkB.addPage(createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            sinkB.finish();
            assertSinkFinished(sinkB);
            assertSource(sourceA, 1);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            assertRemovePage(sourceA, createPage(0));
            assertSourceFinished(sourceA);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            assertRemovePage(sourceB, createPage(0));
            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceA);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testRandom(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_ARBITRARY_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            for (int i = 0; i < 100; i++) {
                Page page = createPage(0);
                sink.addPage(page);
                assertExchangeTotalBufferedBytes(exchange, i + 1);

                LocalExchangeBufferInfo bufferInfoA = sourceA.getBufferInfo();
                LocalExchangeBufferInfo bufferInfoB = sourceB.getBufferInfo();
                assertEquals(bufferInfoA.getBufferedBytes() + bufferInfoB.getBufferedBytes(), retainedSizeOfPages(i + 1));
                assertEquals(bufferInfoA.getBufferedPages() + bufferInfoB.getBufferedPages(), i + 1);
            }

            // we should get ~50 pages per source, but we should get at least some pages in each buffer
            assertTrue(sourceA.getBufferInfo().getBufferedPages() > 0);
            assertTrue(sourceB.getBufferInfo().getBufferedPages() > 0);
            assertExchangeTotalBufferedBytes(exchange, 100);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testPassthrough(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_PASSTHROUGH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                DataSize.ofBytes(retainedSizeOfPages(1)),
                TYPE_OPERATOR_FACTORY);

        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sinkA = sinkFactory.createSink();
            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            assertSinkCanWrite(sinkB);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 0);
            assertSinkWriteBlocked(sinkA);

            assertSinkCanWrite(sinkB);
            sinkB.addPage(createPage(1));
            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertSinkWriteBlocked(sinkA);

            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 0);
            assertSinkCanWrite(sinkA);
            assertSinkWriteBlocked(sinkB);
            assertExchangeTotalBufferedBytes(exchange, 1);

            sinkA.finish();
            assertSinkFinished(sinkA);
            assertSource(sourceB, 1);

            sourceA.finish();
            sourceB.finish();
            assertRemovePage(sourceB, createPage(1));
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);

            assertSinkFinished(sinkB);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testPartition(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_HASH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(0),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sink.addPage(createPage(0));

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertTrue(exchange.getBufferedBytes() >= retainedSizeOfPages(1));

            sink.addPage(createPage(0));

            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertTrue(exchange.getBufferedBytes() >= retainedSizeOfPages(2));

            assertPartitionedRemovePage(sourceA, 0, 2);
            assertSource(sourceA, 1);
            assertSource(sourceB, 2);

            assertPartitionedRemovePage(sourceA, 0, 2);
            assertSource(sourceA, 0);
            assertSource(sourceB, 2);

            sink.finish();
            assertSinkFinished(sink);
            assertSourceFinished(sourceA);
            assertSource(sourceB, 2);

            assertPartitionedRemovePage(sourceB, 1, 2);
            assertSourceFinished(sourceA);
            assertSource(sourceB, 1);

            assertPartitionedRemovePage(sourceB, 1, 2);
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testPartitionCustomPartitioning(PipelineExecutionStrategy executionStrategy)
    {
        ConnectorPartitioningHandle connectorPartitioningHandle = new ConnectorPartitioningHandle() {};
        ConnectorNodePartitioningProvider connectorNodePartitioningProvider = new ConnectorNodePartitioningProvider()
        {
            @Override
            public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
            {
                return createBucketNodeMap(2);
            }

            @Override
            public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
            {
                return (page, position) -> {
                    long rowValue = BIGINT.getLong(page.getBlock(0), position);
                    if (rowValue == 42) {
                        return 0;
                    }
                    return 1;
                };
            }
        };
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT);
        partitionManagers.put(
                new CatalogName("foo"),
                connectorNodePartitioningProvider);
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.of(new CatalogName("foo")),
                Optional.of(TestingTransactionHandle.create()),
                connectorPartitioningHandle);
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                partitioningHandle,
                2,
                types,
                ImmutableList.of(1),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sink = sinkFactory.createSink();
            assertSinkCanWrite(sink);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(1);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(0);
            assertSource(sourceB, 0);

            Page pageA = SequencePageBuilder.createSequencePage(types, 1, 100, 42);
            sink.addPage(pageA);

            assertSource(sourceA, 1);
            assertSource(sourceB, 0);

            assertRemovePage(types, sourceA, pageA);
            assertSource(sourceA, 0);

            Page pageB = SequencePageBuilder.createSequencePage(types, 100, 100, 43);
            sink.addPage(pageB);

            assertSource(sourceA, 0);
            assertSource(sourceB, 1);

            assertRemovePage(types, sourceB, pageB);
            assertSource(sourceB, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void writeUnblockWhenAllReadersFinish(PipelineExecutionStrategy executionStrategy)
    {
        ImmutableList<Type> types = ImmutableList.of(BIGINT);

        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_BROADCAST_DISTRIBUTION,
                2,
                types,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sinkA = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkB);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sourceA.finish();
            assertSourceFinished(sourceA);

            assertSinkCanWrite(sinkA);
            assertSinkCanWrite(sinkB);

            sourceB.finish();
            assertSourceFinished(sourceB);

            assertSinkFinished(sinkA);
            assertSinkFinished(sinkB);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void writeUnblockWhenAllReadersFinishAndPagesConsumed(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_BROADCAST_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                DataSize.ofBytes(1),
                TYPE_OPERATOR_FACTORY);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sinkA = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkB);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));
            ListenableFuture<Void> sinkAFuture = assertSinkWriteBlocked(sinkA);
            ListenableFuture<Void> sinkBFuture = assertSinkWriteBlocked(sinkB);

            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            sourceA.finish();
            assertSource(sourceA, 1);
            assertRemovePage(sourceA, createPage(0));
            assertSourceFinished(sourceA);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertSource(sourceB, 1);
            assertSinkWriteBlocked(sinkA);
            assertSinkWriteBlocked(sinkB);

            sourceB.finish();
            assertSource(sourceB, 1);
            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);

            assertTrue(sinkAFuture.isDone());
            assertTrue(sinkBFuture.isDone());

            assertSinkFinished(sinkA);
            assertSinkFinished(sinkB);
        });
    }

    @Test
    public void testMismatchedExecutionStrategy()
    {
        // If sink/source didn't create a matching set of exchanges, operators will block forever,
        // waiting for the other half that will never show up.
        // The most common reason of mismatch is when one of sink/source created the wrong kind of local exchange.
        // In such case, we want to fail loudly.
        LocalExchangeFactory ungroupedLocalExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_HASH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(0),
                Optional.empty(),
                UNGROUPED_EXECUTION,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);
        assertThatThrownBy(() -> ungroupedLocalExchangeFactory.getLocalExchange(Lifespan.driverGroup(3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("LocalExchangeFactory is declared as UNGROUPED_EXECUTION. Driver-group exchange cannot be created.");

        LocalExchangeFactory groupedLocalExchangeFactory = new LocalExchangeFactory(
                nodePartitioningManager,
                SESSION,
                FIXED_HASH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(0),
                Optional.empty(),
                GROUPED_EXECUTION,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES,
                TYPE_OPERATOR_FACTORY);
        assertThatThrownBy(() -> groupedLocalExchangeFactory.getLocalExchange(Lifespan.taskWide()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("LocalExchangeFactory is declared as GROUPED_EXECUTION. Task-wide exchange cannot be created.");
    }

    private void run(LocalExchangeFactory localExchangeFactory, PipelineExecutionStrategy pipelineExecutionStrategy, Consumer<LocalExchange> test)
    {
        switch (pipelineExecutionStrategy) {
            case UNGROUPED_EXECUTION:
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.taskWide()));
                return;
            case GROUPED_EXECUTION:
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.driverGroup(1)));
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.driverGroup(12)));
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.driverGroup(23)));
                return;
        }
        throw new IllegalArgumentException("Unknown pipelineExecutionStrategy");
    }

    private static void assertSource(LocalExchangeSource source, int pageCount)
    {
        LocalExchangeBufferInfo bufferInfo = source.getBufferInfo();
        assertEquals(bufferInfo.getBufferedPages(), pageCount);
        assertFalse(source.isFinished());
        if (pageCount == 0) {
            assertFalse(source.waitForReading().isDone());
            assertNull(source.removePage());
            assertFalse(source.waitForReading().isDone());
            assertFalse(source.isFinished());
            assertEquals(bufferInfo.getBufferedBytes(), 0);
        }
        else {
            assertTrue(source.waitForReading().isDone());
            assertTrue(bufferInfo.getBufferedBytes() > 0);
        }
    }

    private static void assertSourceFinished(LocalExchangeSource source)
    {
        assertTrue(source.isFinished());
        LocalExchangeBufferInfo bufferInfo = source.getBufferInfo();
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getBufferedBytes(), 0);

        assertTrue(source.waitForReading().isDone());
        assertNull(source.removePage());
        assertTrue(source.waitForReading().isDone());

        assertTrue(source.isFinished());
    }

    private static void assertRemovePage(LocalExchangeSource source, Page expectedPage)
    {
        assertRemovePage(TYPES, source, expectedPage);
    }

    private static void assertRemovePage(List<Type> types, LocalExchangeSource source, Page expectedPage)
    {
        assertTrue(source.waitForReading().isDone());
        Page actualPage = source.removePage();
        assertNotNull(actualPage);

        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        PageAssertions.assertPageEquals(types, actualPage, expectedPage);
    }

    private static void assertPartitionedRemovePage(LocalExchangeSource source, int partition, int partitionCount)
    {
        assertTrue(source.waitForReading().isDone());
        Page page = source.removePage();
        assertNotNull(page);

        LocalPartitionGenerator partitionGenerator = new LocalPartitionGenerator(new InterpretedHashGenerator(TYPES, new int[] {0}, TYPE_OPERATOR_FACTORY), partitionCount);
        for (int position = 0; position < page.getPositionCount(); position++) {
            assertEquals(partitionGenerator.getPartition(page, position), partition);
        }
    }

    private static void assertSinkCanWrite(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static ListenableFuture<Void> assertSinkWriteBlocked(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished());
        ListenableFuture<Void> writeFuture = sink.waitForWriting();
        assertFalse(writeFuture.isDone());
        return writeFuture;
    }

    private static void assertSinkFinished(LocalExchangeSink sink)
    {
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());

        // this will be ignored
        sink.addPage(createPage(0));
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static void assertExchangeTotalBufferedBytes(LocalExchange exchange, int pageCount)
    {
        assertEquals(exchange.getBufferedBytes(), retainedSizeOfPages(pageCount));
    }

    private static Page createPage(int i)
    {
        return SequencePageBuilder.createSequencePage(TYPES, 100, i);
    }

    public static long retainedSizeOfPages(int count)
    {
        return RETAINED_PAGE_SIZE.toBytes() * count;
    }
}
