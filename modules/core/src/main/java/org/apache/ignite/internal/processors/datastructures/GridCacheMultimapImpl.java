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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.retry;

public class GridCacheMultimapImpl<K,V> implements IgniteMultimap<K,V> {
    /** Cache context. */
    private final GridCacheContext ctx;

    /** Cache. */
    private final IgniteInternalCache<K, ArrayList<V>> cache;

    /** Logger. */
    private final IgniteLogger log;

    /** Multimap name. */
    private final String name;

    /** Multimap unique ID. */
    private final IgniteUuid id;

    /** Collocation flag. */
    private final boolean collocated;

    /** Queue header partition. */
    private final int hdrPart;

    /** Removed flag. */
    private volatile boolean rmvd;

    /** */
    private final boolean binaryMarsh;

    /**
     * @param call Callable.
     * @return Callable result.
     */
    private <R> R retry(Callable<R> call) {
        try {
            return DataStructuresProcessor.retry(log, call);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public GridCacheMultimapImpl(GridCacheContext ctx, String name, GridCacheMultimapHeader hdr) {
        this.ctx = ctx;
        this.name = name;
        id = hdr.id();
        collocated = hdr.collocated();
        binaryMarsh = ctx.binaryMarshaller();

        cache = ctx.cache();

        log = ctx.logger(GridCacheMultimapImpl.class);

        hdrPart = ctx.affinity().partition(new GridCacheMultimapHeaderKey(name));
    }

    public @Override boolean put(final K key, final V value) {
        return retry(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                EntryProcessorResult<Boolean> res =
                    cache.invoke(key, new EntryProcessor<K, ArrayList<V>, Boolean>() {
                        @Override public Boolean process(MutableEntry<K, ArrayList<V>> entry, Object... arg)
                            throws EntryProcessorException {
                        ArrayList<V> l = entry.getValue();

                        if(l == null)
                            l = new ArrayList<V>();

                        l.add(value);
                        entry.setValue(l);

                        return Boolean.TRUE;
                    }
                });

                if(res == null) {
                    return Boolean.FALSE;
                }
                return res.get();
            }
        });
    }

    public V get(final K key, final int index) {
        return retry(new Callable<V>() {
            @Override public V call() throws Exception {
                EntryProcessorResult<V> res = cache.invoke(key, new EntryProcessor<K, ArrayList<V>, V>() {
                    @Override public V process(MutableEntry<K, ArrayList<V>> entry, Object... arg)
                        throws EntryProcessorException {
                        ArrayList<V> l = entry.getValue();

                        if(l == null)
                            return null;

                        if(l.size() <= index)
                            return null;

                        return l.get(index);
                    }
                });
                if(res == null) {
                    return null;
                }
                return res.get();
            }
        });
    };
}
