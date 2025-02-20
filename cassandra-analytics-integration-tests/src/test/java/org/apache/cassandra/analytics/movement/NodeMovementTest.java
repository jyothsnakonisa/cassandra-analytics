/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics.movement;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.analytics.TestConsistencyLevel;
import org.apache.cassandra.analytics.TestUninterruptibles;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * Integration tests to verify bulk writes when a Cassandra's node range is moved and the operation is expected
 * to succeed
 */
class NodeMovementTest extends NodeMovementTestBase
{
    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("singleDCTestInputs")
    void testMoveNode(TestConsistencyLevel cl)
    {
        runMovingNodeTest(cl);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        singleDCTestInputs().forEach(arguments -> {
            QualifiedName tableName = uniqueTestTableFullName(TEST_KEYSPACE, arguments.get());
            createTestTable(tableName, CREATE_TEST_TABLE_STATEMENT);
        });
    }

    @Override
    protected CountDownLatch transitioningStateStart()
    {
        return BBHelperMovingNode.transitioningStateStart;
    }

    @Override
    protected void beforeClusterShutdown()
    {
        completeTransitionAndValidateWrites(BBHelperMovingNode.transitioningStateEnd, singleDCTestInputs(), false);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return clusterConfig().nodesPerDc(5)
                              .requestFeature(Feature.NETWORK)
                              .instanceInitializer(BBHelperMovingNode::install);
    }

    static Stream<Arguments> singleDCTestInputs()
    {
        return Stream.of(
        Arguments.of(TestConsistencyLevel.of(ONE, ALL)),
        Arguments.of(TestConsistencyLevel.of(QUORUM, QUORUM)),
        Arguments.of(TestConsistencyLevel.of(ALL, ONE))
        );
    }

    /**
     * ByteBuddy Helper for a single moving node
     */
    public static class BBHelperMovingNode
    {
        static final CountDownLatch transitioningStateStart = new CountDownLatch(1);
        static final CountDownLatch transitioningStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == SINGLE_DC_MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMovingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            transitioningStateStart.countDown();
            TestUninterruptibles.awaitUninterruptiblyOrThrow(transitioningStateEnd, 2, TimeUnit.MINUTES);
            return res;
        }
    }
}
