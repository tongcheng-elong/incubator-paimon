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

package org.apache.paimon.hive;

import org.apache.paimon.client.ClientPool;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link HiveCatalogLock}. */
public class HiveCatalogLockTest {

    @Test
    public void unlockClearsInterruptFlagBeforeReleasingLock() throws Exception {
        IMetaStoreClient client = mock(IMetaStoreClient.class);
        when(client.lock(any())).thenReturn(new LockResponse(1L, LockState.ACQUIRED));

        AtomicInteger unlockCount = new AtomicInteger();
        ClientPool<IMetaStoreClient, TException> pool =
                new ClientPool<IMetaStoreClient, TException>() {
                    @Override
                    public <R> R run(Action<R, IMetaStoreClient, TException> action)
                            throws TException {
                        return action.run(client);
                    }

                    @Override
                    public void execute(ExecuteAction<IMetaStoreClient, TException> action)
                            throws TException {
                        unlockCount.incrementAndGet();
                        action.run(client);
                    }
                };

        HiveCatalogLock lock = new HiveCatalogLock(pool, 10, 1000);
        Thread.currentThread().interrupt();
        try {
            assertThat(lock.runWithLock("db", "table", () -> "ok")).isEqualTo("ok");
            assertThat(unlockCount).hasValue(1);
            verify(client).unlock(1L);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
