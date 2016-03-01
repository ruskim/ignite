/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.datastructures;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

public class GridCacheMultimapProxy<K,V> implements IgniteMultimap<K,V> {

    /** Delegate multimap. */
    private GridCacheMultimapImpl<K,V> delegate;

    /** Cache context. */
    private GridCacheContext cctx;

    /** Cache gateway. */
    private GridCacheGateway gate;

    /** Busy lock. */
    private GridSpinBusyLock busyLock;

    public GridCacheMultimapProxy(GridCacheContext cctx, GridCacheMultimapImpl<K,V> delegate) {
        this.cctx = cctx;
        this.delegate = delegate;

        gate = cctx.gate();

        busyLock = new GridSpinBusyLock();
    }

    @Override public boolean put(final K key, final V value) {
        /* TODO: do we need enter/leave Busy like in Set*/
        // enterBusy();
        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<Boolean>() {
                        @Override public Boolean call() throws Exception {
                            return delegate.put(key, value);
                        }
                    }, cctx);

                return delegate.put(key, value);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            //leaveBusy();
        }
    };

    public V get(final K key, final int index) {
        /* TODO: implement enter/leave Busy */
        // enterBusy();
        try {
            gate.enter();

            try {
                if (cctx.transactional())
                    return CU.outTx(new Callable<V>() {
                        @Override public V call() throws Exception {
                            return delegate.get(key, index);
                        }
                    }, cctx);

                return delegate.get(key, index);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                gate.leave();
            }
        }
        finally {
            //leaveBusy();
        }
    };
}
