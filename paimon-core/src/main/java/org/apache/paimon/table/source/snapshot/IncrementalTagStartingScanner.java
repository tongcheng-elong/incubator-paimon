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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.util.*;

/** {@link StartingScanner} for incremental changes by tag. */
public class IncrementalTagStartingScanner extends AbstractStartingScanner {

    private final String start;
    private final String end;
    private final ScanMode scanMode;
    private final CoreOptions.TagCreationMode tagCreationMode;

    public IncrementalTagStartingScanner(
            SnapshotManager snapshotManager,
            String start,
            String end,
            ScanMode scanMode,
            CoreOptions.TagCreationMode tagCreationMode) {
        super(snapshotManager);
        this.start = start;
        this.end = end;
        this.scanMode = scanMode;
        this.tagCreationMode = tagCreationMode;
    }

    @Override
    public Result scan(SnapshotReader reader) {
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());

        if (tagCreationMode == CoreOptions.TagCreationMode.BATCH) {
            Snapshot tagStart;
            Snapshot tagEnd;
            try {
                tagStart = tagManager.taggedSnapshot(start);
            } catch (IllegalArgumentException e) {
                tagStart = null;
            }
            try {
                tagEnd = tagManager.taggedSnapshot(end);
            } catch (IllegalArgumentException e) {
                tagEnd = null;
            }

            if (tagStart == null && tagEnd == null) {
                throw new NullPointerException(
                        "The tags of the incremental query cannot all be empty");
            }
            Snapshot tagResult = tagEnd == null ? tagStart : tagEnd;
            Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> grouped = new HashMap<>();
            // 兼容先知场景首次创建的情况
            if (tagStart == null || tagEnd == null) {
                List<DataSplit> currentSplits = readSplits(reader, tagResult);
                for (DataSplit split : currentSplits) {
                    grouped.computeIfAbsent(
                                    Pair.of(split.partition(), split.bucket()),
                                    k -> new ArrayList<>())
                            .addAll(split.dataFiles());
                }
            } else {
                if (tagEnd.id() <= tagStart.id()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Tag end %s with snapshot id %s should be larger than tag start %s with snapshot id %s",
                                    end, tagEnd.id(), start, tagStart.id()));
                }
                SortedMap<Snapshot, String> tags = tagManager.tags();
                for (Snapshot snapshot : tags.keySet()) {
                    if (snapshot.id() > tagStart.id() && snapshot.id() <= tagEnd.id()) {
                        List<DataSplit> currentSplits = readSplits(reader, snapshot);
                        for (DataSplit split : currentSplits) {
                            grouped.computeIfAbsent(
                                            Pair.of(split.partition(), split.bucket()),
                                            k -> new ArrayList<>())
                                    .addAll(split.dataFiles());
                        }
                    }
                }
            }

            List<DataSplit> result = new ArrayList<>();
            for (Map.Entry<Pair<BinaryRow, Integer>, List<DataFileMeta>> entry :
                    grouped.entrySet()) {
                BinaryRow partition = entry.getKey().getLeft();
                int bucket = entry.getKey().getRight();
                for (List<DataFileMeta> files :
                        reader.splitGenerator().splitForBatch(entry.getValue())) {
                    result.add(
                            DataSplit.builder()
                                    .withSnapshot(tagResult.id())
                                    .withPartition(partition)
                                    .withBucket(bucket)
                                    .withDataFiles(files)
                                    .build());
                }
            }
            return StartingScanner.fromPlan(
                    new SnapshotReader.Plan() {
                        @Override
                        public Long watermark() {
                            return null;
                        }

                        @Override
                        public Long snapshotId() {
                            return tagResult.id();
                        }

                        @Override
                        public List<Split> splits() {
                            return (List) result;
                        }
                    });
        } else {
            Snapshot tagStart = tagManager.taggedSnapshot(start);
            Snapshot tagEnd = tagManager.taggedSnapshot(end);

            if (tagEnd.id() <= tagStart.id()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Tag end %s with snapshot id %s should be larger than tag start %s with snapshot id %s",
                                end, tagEnd.id(), start, tagStart.id()));
            }

            if (tagEnd.id() <= tagStart.id()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Tag end %s with snapshot id %s should be larger than tag start %s with snapshot id %s",
                                end, tagEnd.id(), start, tagStart.id()));
            }
            return StartingScanner.fromPlan(
                    reader.withSnapshot(tagEnd).readIncrementalDiff(tagStart));
        }
    }

    private List<DataSplit> readSplits(SnapshotReader reader, Snapshot s) {
        switch (scanMode) {
            case CHANGELOG:
                return readChangeLogSplits(reader, s);
            case DELTA:
                return readDeltaSplits(reader, s);
            default:
                throw new UnsupportedOperationException("Unsupported scan kind: " + scanMode);
        }
    }

    private List<DataSplit> readDeltaSplits(SnapshotReader reader, Snapshot s) {
        if (s.commitKind() == Snapshot.CommitKind.OVERWRITE) {
            // ignore OVERWRITE
            return Collections.emptyList();
        }
        return (List) reader.withSnapshot(s).withMode(ScanMode.DELTA).read().splits();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<DataSplit> readChangeLogSplits(SnapshotReader reader, Snapshot s) {
        if (s.commitKind() == Snapshot.CommitKind.OVERWRITE) {
            // ignore OVERWRITE
            return Collections.emptyList();
        }
        return (List) reader.withSnapshot(s).withMode(ScanMode.CHANGELOG).read().splits();
    }
}
