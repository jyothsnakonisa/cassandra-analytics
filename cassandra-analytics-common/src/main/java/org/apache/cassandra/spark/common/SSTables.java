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

package org.apache.cassandra.spark.common;

import java.nio.file.Path;

import org.apache.cassandra.bridge.SSTableDescriptor;

public final class SSTables
{
    private SSTables()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Get the sstable base name from data file path.
     * For example, the base name of data file '/path/to/table/nb-1-big-Data.db' is 'nb-1-big'
     *
     * @deprecated use {@code #getSSTableDescriptor(Path).baseFilename} instead
     *
     * @param dataFile data file path
     * @return sstable base name
     */
    @Deprecated
    public static String getSSTableBaseName(Path dataFile)
    {
        String fileName = dataFile.getFileName().toString();
        return fileName.substring(0, fileName.lastIndexOf("-") + 1);
    }

    /**
     * Get the {@link SSTableDescriptor} from the data file path.
     * @param dataFile data file path
     * @return sstable descriptor
     */
    public static SSTableDescriptor getSSTableDescriptor(Path dataFile)
    {
        String baseFilename = getSSTableBaseName(dataFile);
        return new SSTableDescriptor(baseFilename);
    }
}
