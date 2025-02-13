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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Tests bulk writes with different digest options
 */
class WriterDigestIntegrationTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName DEFAULT_DIGEST_TABLE = uniqueTestTableFullName("default_digest");
    static final QualifiedName MD5_DIGEST_TABLE = uniqueTestTableFullName("md5_digest");
    static final QualifiedName CORRUPT_SSTABLE_TABLE = uniqueTestTableFullName("corrupt_sstable");
    static final List<QualifiedName> TABLE_NAMES = Arrays.asList(DEFAULT_DIGEST_TABLE, MD5_DIGEST_TABLE,
                                                                 CORRUPT_SSTABLE_TABLE);
    Dataset<Row> df;

    @Test
    void testDefaultDigest()
    {
        bulkWriterDataFrameWriter(df, DEFAULT_DIGEST_TABLE).save();
        sparkTestUtils.validateWrites(df.collectAsList(), queryAllData(DEFAULT_DIGEST_TABLE));
    }

    @Test
    void testMD5Digest()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        bulkWriterDataFrameWriter(df, MD5_DIGEST_TABLE).option(WriterOptions.DIGEST.name(), "MD5").save();
        sparkTestUtils.validateWrites(df.collectAsList(), queryAllData(MD5_DIGEST_TABLE));
    }

    @Test
    void failsOnInvalidDigestOption()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> bulkWriterDataFrameWriter(df, DEFAULT_DIGEST_TABLE).option(WriterOptions.DIGEST.name(), "invalid")
                                                                             .save())
        .withMessageContaining("Key digest type with value invalid is not a valid Enum of type class org.apache.cassandra.spark.bulkwriter.DigestAlgorithms");
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        SparkSession spark = getOrCreateSparkSession();
        df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        TABLE_NAMES.forEach(name -> {
            createTestKeyspace(name, DC1_RF1);
            createTestTable(name, CREATE_TEST_TABLE_STATEMENT);
        });
    }
}
