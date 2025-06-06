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

package org.apache.paimon.table;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A table provides basic abstraction for table type and table scan and table read.
 *
 * @since 0.4.0
 */
@Public
public interface Table extends Serializable {

    // ================== Table Metadata =====================

    /** A name to identify this table. */
    String name();

    /** Full name of the table, default is database.tableName. */
    default String fullName() {
        return name();
    }

    /**
     * UUID of the table, metastore can provide the true UUID of this table, default is the full
     * name.
     */
    default String uuid() {
        return fullName();
    }

    /** Returns the row type of this table. */
    RowType rowType();

    /** Partition keys of this table. */
    List<String> partitionKeys();

    /** Primary keys of this table. */
    List<String> primaryKeys();

    /** Options of this table. */
    Map<String, String> options();

    /** Optional comment of this table. */
    Optional<String> comment();

    /** Optional statistics of this table. */
    @Experimental
    Optional<Statistics> statistics();

    // ================= Table Operations ====================

    /** File io of this table. */
    FileIO fileIO();

    /** Copy this table with adding dynamic options. */
    Table copy(Map<String, String> dynamicOptions);

    /** Get the latest snapshot for this table, or empty if there are no snapshots. */
    @Experimental
    Optional<Snapshot> latestSnapshot();

    /** Get the {@link Snapshot} from snapshot id. */
    @Experimental
    Snapshot snapshot(long snapshotId);

    /** Reader to read manifest file meta from manifest list file. */
    @Experimental
    SimpleFileReader<ManifestFileMeta> manifestListReader();

    /** Reader to read manifest entry from manifest file. */
    @Experimental
    SimpleFileReader<ManifestEntry> manifestFileReader();

    /** Reader to read index manifest entry from index manifest file. */
    @Experimental
    SimpleFileReader<IndexManifestEntry> indexManifestFileReader();

    /** Rollback table's state to a specific snapshot. */
    @Experimental
    void rollbackTo(long snapshotId);

    /** Create a tag from given snapshot. */
    @Experimental
    void createTag(String tagName, long fromSnapshotId);

    @Experimental
    void createTag(String tagName, long fromSnapshotId, @Nullable Duration timeRetained);

    /** Create a tag from latest snapshot. */
    @Experimental
    void createTag(String tagName);

    @Experimental
    void createTag(String tagName, @Nullable Duration timeRetained);

    @Experimental
    void renameTag(String tagName, String targetTagName);

    /** Replace a tag with new snapshot id and new time retained. */
    @Experimental
    void replaceTag(String tagName, @Nullable Long fromSnapshotId, @Nullable Duration timeRetained);

    /** Delete a tag by name. */
    @Experimental
    void deleteTag(String tagName);

    /** Delete tags, tags are separated by commas. */
    @Experimental
    default void deleteTags(String tagStr) {
        String[] tagNames =
                Arrays.stream(tagStr.split(",")).map(String::trim).toArray(String[]::new);
        for (String tagName : tagNames) {
            deleteTag(tagName);
        }
    }

    /** Rollback table's state to a specific tag. */
    @Experimental
    void rollbackTo(String tagName);

    /** Create an empty branch. */
    @Experimental
    void createBranch(String branchName);

    /** Create a branch from given tag. */
    @Experimental
    void createBranch(String branchName, String tagName);

    /** Delete a branch by branchName. */
    @Experimental
    void deleteBranch(String branchName);

    /** Delete branches, branches are separated by commas. */
    @Experimental
    default void deleteBranches(String branchNames) {
        for (String branch : branchNames.split(",")) {
            deleteBranch(branch);
        }
    }

    /** Merge a branch to main branch. */
    @Experimental
    void fastForward(String branchName);

    /** Manually expire snapshots, parameters can be controlled independently of table options. */
    @Experimental
    ExpireSnapshots newExpireSnapshots();

    @Experimental
    ExpireSnapshots newExpireChangelog();

    // =============== Read & Write Operations ==================

    /** Returns a new read builder. */
    ReadBuilder newReadBuilder();

    /** Returns a new batch write builder. */
    BatchWriteBuilder newBatchWriteBuilder();

    /** Returns a new stream write builder. */
    StreamWriteBuilder newStreamWriteBuilder();
}
