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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Bytes {@link ColumnReader}. A int length and bytes data. */
public class BytesColumnReader extends AbstractColumnReader<WritableBytesVector> {

    public BytesColumnReader(ColumnDescriptor descriptor, PageReadStore pageReadStore)
            throws IOException {
        super(descriptor, pageReadStore);
        checkTypeName(PrimitiveType.PrimitiveTypeName.BINARY);
    }

    @Override
    protected void readBatch(int rowId, int num, WritableBytesVector column) {
        int left = num;
        while (left > 0) {
            if (runLenDecoder.currentCount == 0) {
                runLenDecoder.readNextGroup();
            }
            int n = Math.min(left, runLenDecoder.currentCount);
            switch (runLenDecoder.mode) {
                case RLE:
                    if (runLenDecoder.currentValue == maxDefLevel) {
                        readBinary(n, column, rowId);
                    } else {
                        column.setNulls(rowId, n);
                    }
                    break;
                case PACKED:
                    for (int i = 0; i < n; ++i) {
                        if (runLenDecoder.currentBuffer[runLenDecoder.currentBufferIdx++]
                                == maxDefLevel) {
                            readBinary(1, column, rowId + i);
                        } else {
                            column.setNullAt(rowId + i);
                        }
                    }
                    break;
            }
            rowId += n;
            left -= n;
            runLenDecoder.currentCount -= n;
        }
    }

    @Override
    protected void skipBatch(int num) {
        int left = num;
        while (left > 0) {
            if (runLenDecoder.currentCount == 0) {
                runLenDecoder.readNextGroup();
            }
            int n = Math.min(left, runLenDecoder.currentCount);
            switch (runLenDecoder.mode) {
                case RLE:
                    if (runLenDecoder.currentValue == maxDefLevel) {
                        skipBinary(n);
                    }
                    break;
                case PACKED:
                    for (int i = 0; i < n; ++i) {
                        if (runLenDecoder.currentBuffer[runLenDecoder.currentBufferIdx++]
                                == maxDefLevel) {
                            skipBinary(1);
                        }
                    }
                    break;
            }
            left -= n;
            runLenDecoder.currentCount -= n;
        }
    }

    private void skipBinary(int num) {
        for (int i = 0; i < num; i++) {
            int len = readDataBuffer(4).getInt();
            skipDataBuffer(len);
        }
    }

    @Override
    protected void readBatchFromDictionaryIds(
            int rowId, int num, WritableBytesVector column, WritableIntVector dictionaryIds) {
        for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
                byte[] bytes = dictionary.decodeToBinary(dictionaryIds.getInt(i)).getBytesUnsafe();
                column.putByteArray(i, bytes, 0, bytes.length);
            }
        }
    }

    private void readBinary(int total, WritableBytesVector v, int rowId) {
        for (int i = 0; i < total; i++) {
            int len = readDataBuffer(4).getInt();
            ByteBuffer buffer = readDataBuffer(len);
            if (buffer.hasArray()) {
                v.putByteArray(
                        rowId + i, buffer.array(), buffer.arrayOffset() + buffer.position(), len);
            } else {
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                v.putByteArray(rowId + i, bytes, 0, bytes.length);
            }
        }
    }
}
