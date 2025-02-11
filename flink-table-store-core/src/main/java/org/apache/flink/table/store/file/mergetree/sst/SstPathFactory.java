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

package org.apache.flink.table.store.file.mergetree.sst;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;

import java.util.UUID;

/** Factory which produces new {@link Path}s for sst files. */
public class SstPathFactory {

    private final Path bucketDir;
    private final String uuid;

    private int pathCount;

    public SstPathFactory(Path root, String partition, int bucket) {
        this.bucketDir = new Path(root + "/" + partition + "/bucket-" + bucket);
        this.uuid = UUID.randomUUID().toString();

        this.pathCount = 0;
    }

    public Path newPath() {
        return new Path(bucketDir + "/sst-" + uuid + "-" + (pathCount++));
    }

    public Path toPath(String fileName) {
        return new Path(bucketDir + "/" + fileName);
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }
}
