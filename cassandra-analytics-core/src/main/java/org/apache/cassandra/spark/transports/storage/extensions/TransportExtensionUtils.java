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

package org.apache.cassandra.spark.transports.storage.extensions;

import java.util.Objects;

import org.apache.cassandra.spark.bulkwriter.JobInfo;

public class TransportExtensionUtils
{
    private TransportExtensionUtils()
    {
        throw new UnsupportedOperationException();
    }

    public static void validateReceivedJobId(String receivedJobId, JobInfo jobInfo)
    {
        String actualJobId = jobInfo.getId();
        if (!Objects.equals(receivedJobId, actualJobId))
        {
            throw new IllegalStateException("Received jobId does not match. Received: " + receivedJobId
                                            + "; actual: " + actualJobId);
        }
    }
}
