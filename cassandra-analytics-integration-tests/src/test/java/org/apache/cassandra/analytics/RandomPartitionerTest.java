/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.analytics;

import org.apache.cassandra.testing.ClusterBuilderConfiguration;

/**
 * Runs test from {@link CassandraAnalyticsSimpleTest} with the {@code RandomPartitioner}.
 */
class RandomPartitionerTest extends CassandraAnalyticsSimpleTest
{
    /**
     * Inherit the configuration for the cluster from {@link CassandraAnalyticsSimpleTest} and set the
     * partitioner to be the RandomPartitioner.
     *
     * @return the updated configuration for the cluster
     */
    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().partitioner("org.apache.cassandra.dht.RandomPartitioner");
    }
}
