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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.ChannelComputer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link ChannelComputer} for {@link RowData}. */
public class RowDataChannelComputer implements ChannelComputer<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataChannelComputer.class);

    private static final long serialVersionUID = 1L;

    private final TableSchema schema;
    private final boolean hasLogSink;

    private transient int numChannels;
    private transient KeyAndBucketExtractor<RowData> extractor;

    public RowDataChannelComputer(TableSchema schema, boolean hasLogSink) {
        this.schema = schema;
        this.hasLogSink = hasLogSink;
    }

    @Override
    public void setup(int numChannels) {
        LOG.info("setup channel numbers:{}", numChannels);
        this.numChannels = numChannels;
        this.extractor = new RowDataKeyAndBucketExtractor(schema);
    }

    @Override
    public int channel(RowData record) {
        extractor.setRecord(record);
        return channel(extractor.partition(), extractor.bucket());
    }

    public int channel(BinaryRow partition, int bucket) {
        // log sinks like Kafka only consider bucket and don't care about partition
        // so same bucket, even from different partition, must go to the same channel
        return hasLogSink
                ? ChannelComputer.select(bucket, numChannels)
                : ChannelComputer.select(partition, bucket, numChannels);
    }

    @Override
    public String toString() {
        return "shuffle by bucket";
    }
}
