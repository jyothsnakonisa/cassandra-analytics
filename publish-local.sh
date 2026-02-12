#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Script to publish all Cassandra Analytics JARs to Maven local repository
# Version: 0.100

set -e

# Set JDK 11
export JAVA_HOME=/Library/Java/JavaVirtualMachines/applejdk-11.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

# Configuration
VERSION="0.100"
GROUP_ID="org.apache.cassandra"
MAVEN_LOCAL_REPO="${HOME}/.m2/repository"
GROUP_PATH="${MAVEN_LOCAL_REPO}/org/apache/cassandra"

echo "========================================"
echo "Publishing Cassandra Analytics JARs"
echo "Version: ${VERSION}"
echo "Java Home: ${JAVA_HOME}"
echo "Maven Local: ${MAVEN_LOCAL_REPO}"
echo "========================================"
echo

# Verify Java version
echo "Checking Java version..."
java -version
echo

# Step 1: Remove existing JARs with version 0.100 from Maven local
echo "Step 1: Removing existing version ${VERSION} from Maven local..."
if [ -d "${GROUP_PATH}" ]; then
    # Find all directories with version 0.100
    FOUND_DIRS=$(find "${GROUP_PATH}" -type d -name "${VERSION}" 2>/dev/null || true)

    if [ -n "$FOUND_DIRS" ]; then
        echo "Found artifacts with version ${VERSION}:"
        echo "$FOUND_DIRS"
        echo
        echo "Removing..."
        find "${GROUP_PATH}" -type d -name "${VERSION}" -exec rm -rf {} + 2>/dev/null || true
        echo "Removed version ${VERSION} artifacts"
    else
        echo "No existing artifacts with version ${VERSION} found"
    fi
else
    echo "Maven local repository path does not exist yet: ${GROUP_PATH}"
fi
echo

# Step 2: Verify removal
echo "Step 2: Verifying removal..."
REMAINING=$(find "${GROUP_PATH}" -type d -name "${VERSION}" 2>/dev/null || true)
if [ -z "$REMAINING" ]; then
    echo "✓ Successfully verified: No version ${VERSION} artifacts remain"
else
    echo "✗ Warning: Some version ${VERSION} artifacts still exist:"
    echo "$REMAINING"
    exit 1
fi
echo

# Step 3: Publish to Maven local (skipping tests, RAT checks, and clean)
echo "Step 3: Publishing to Maven local..."
./gradlew publishToMavenLocal -PskipSigning -x test -x rat -x check --no-daemon --parallel
echo "✓ Publishing completed"
echo

# Step 4: Verify published JARs
echo "Step 4: Verifying published JARs in Maven local..."
echo

if [ ! -d "${GROUP_PATH}" ]; then
    echo "✗ Error: Maven local repository path does not exist: ${GROUP_PATH}"
    exit 1
fi

# Find all published artifacts with version 0.100
PUBLISHED_ARTIFACTS=$(find "${GROUP_PATH}" -type d -name "${VERSION}" 2>/dev/null || true)

if [ -z "$PUBLISHED_ARTIFACTS" ]; then
    echo "✗ Error: No artifacts with version ${VERSION} found in Maven local"
    exit 1
fi

echo "✓ Found published artifacts with version ${VERSION}:"
echo "$PUBLISHED_ARTIFACTS" | while read -r artifact_dir; do
    ARTIFACT_NAME=$(basename "$(dirname "$artifact_dir")")
    echo "  - ${ARTIFACT_NAME}:${VERSION}"

    # List JAR files in the artifact directory
    JAR_FILES=$(find "$artifact_dir" -type f -name "*.jar" 2>/dev/null || true)
    if [ -n "$JAR_FILES" ]; then
        echo "$JAR_FILES" | while read -r jar_file; do
            JAR_SIZE=$(ls -lh "$jar_file" | awk '{print $5}')
            echo "    └── $(basename "$jar_file") (${JAR_SIZE})"
        done
    fi
done

echo
echo "========================================"
echo "✓ Successfully published all JARs to Maven local!"
echo "========================================"
echo
echo "Summary:"
echo "  Group ID: ${GROUP_ID}"
echo "  Version: ${VERSION}"
echo "  Location: ${GROUP_PATH}"
echo
echo "You can now use these artifacts in your projects by adding them as dependencies."
