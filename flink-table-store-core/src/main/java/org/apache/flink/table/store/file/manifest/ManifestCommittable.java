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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Manifest commit message. */
public class ManifestCommittable {

    private final Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> newFiles;

    private final Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> compactBefore;

    private final Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> compactAfter;

    public ManifestCommittable() {
        this.newFiles = new HashMap<>();
        this.compactBefore = new HashMap<>();
        this.compactAfter = new HashMap<>();
    }

    public ManifestCommittable(
            Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> newFiles,
            Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> compactBefore,
            Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> compactAfter) {
        this.newFiles = newFiles;
        this.compactBefore = compactBefore;
        this.compactAfter = compactAfter;
    }

    public void add(BinaryRowData partition, int bucket, Increment increment) {
        addFiles(newFiles, partition, bucket, increment.newFiles());
        addFiles(compactBefore, partition, bucket, increment.compactBefore());
        addFiles(compactAfter, partition, bucket, increment.compactAfter());
    }

    private static void addFiles(
            Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> map,
            BinaryRowData partition,
            int bucket,
            List<SstFileMeta> files) {
        map.computeIfAbsent(partition, k -> new HashMap<>())
                .computeIfAbsent(bucket, k -> new ArrayList<>())
                .addAll(files);
    }

    public Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> newFiles() {
        return newFiles;
    }

    public Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> compactBefore() {
        return compactBefore;
    }

    public Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> compactAfter() {
        return compactAfter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManifestCommittable that = (ManifestCommittable) o;
        return Objects.equals(newFiles, that.newFiles)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(newFiles, compactBefore, compactAfter);
    }
}
