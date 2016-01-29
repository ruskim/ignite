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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test getEntry and getEntries methods.
 */
public abstract class CacheGetEntryAbstractSeltTest extends GridCacheAbstractSelfTest {

    @Override protected int gridCount() {
        return 3;
    }

    abstract protected TransactionConcurrency concurrency();

    abstract protected TransactionIsolation isolation();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** */
    public void testNear() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName("near");
        cfg.setNearConfiguration(new NearCacheConfiguration());

        test(cfg);
    }

    /** */
    public void testNearTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("nearT");
        cfg.setNearConfiguration(new NearCacheConfiguration());

        test(cfg);
    }

    /** */
    public void testPartitioned() {
        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName("partitioned");

        test(cfg);
    }

    /** */
    public void testPartitionedTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("partitionedT");

        test(cfg);
    }

    /** */
    public void testLocal() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.LOCAL);
        cfg.setName("local");

        test(cfg);
    }

    /** */
    public void testLocalTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.LOCAL);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("localT");

        test(cfg);
    }

    /** */
    public void testReplicated() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setName("replicated");

        test(cfg);
    }

    /** */
    public void testReplicatedTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("replicatedT");

        test(cfg);
    }

    /** */
    private void test(CacheConfiguration cfg) {
        test(cfg, true);
        test(cfg, false);
    }

    /** */
    private void test(CacheConfiguration cfg, boolean oneEntry) {
        IgniteCache<Integer, TestValue> cache = grid(0).createCache(cfg);
        try {
            init(cache);

            test(cache, null, null, null, oneEntry);

            if (cfg.getAtomicityMode() == TRANSACTIONAL) {
                TransactionConcurrency txConcurrency = concurrency();
                TransactionIsolation txIsolation = isolation();
                try (Transaction tx = grid(0).transactions().txStart(txConcurrency, txIsolation, 100000, 1000)) {
                    initTx(cache);

                    test(cache, txConcurrency, txIsolation, tx, oneEntry);

                    tx.commit();
                }
            }
        }
        finally {
            cache.destroy();
        }
    }

    private Set<Integer> getKeys(int base) {
        int start = 0;
        int finish = 100;

        Set<Integer> keys = new HashSet<>(finish - start);

        for (int i = base + start; i < base + finish; ++i)
            keys.add(i);

        return keys;
    }

    private Set<Integer> createdBeforeTxKeys() {
        return getKeys(0);
    }

    private Set<Integer> createdBeforeTxWithBinaryKeys() {
        return getKeys(1_000);
    }

    private Set<Integer> createdBeforeTxKeys2() {
        return getKeys(2_000);
    }

    private Set<Integer> createdBeforeTxWithBinaryKeys2() {
        return getKeys(3_000);
    }

    private Set<Integer> createdBeforeTxKeys3() {
        return getKeys(4_000);
    }

    private Set<Integer> createdBeforeTxWithBinaryKeys3() {
        return getKeys(5_000);
    }

    private Set<Integer> removedBeforeTxKeys() {
        return getKeys(6_000);
    }

    private Set<Integer> removedBeforeTxWithBinaryKeys() {
        return getKeys(7_000);
    }

    private Set<Integer> createdAtTxKeys() {
        return getKeys(8_000);
    }

    private Set<Integer> createdAtTxWithBinaryKeys() {
        return getKeys(9_000);
    }

    private Set<Integer> removedAtTxKeys() {
        return getKeys(10_000);
    }

    private Set<Integer> removedAtTxWithBinaryKeys() {
        return getKeys(11_000);
    }

    /** */
    private void init(IgniteCache<Integer, TestValue> cache) {
        Set<Integer> keys = new HashSet<>();

        keys.addAll(createdBeforeTxKeys());
        keys.addAll(createdBeforeTxWithBinaryKeys());
        keys.addAll(createdBeforeTxKeys2());
        keys.addAll(createdBeforeTxWithBinaryKeys2());
        keys.addAll(createdBeforeTxKeys3());
        keys.addAll(createdBeforeTxWithBinaryKeys3());
        keys.addAll(removedBeforeTxKeys());
        keys.addAll(removedBeforeTxWithBinaryKeys());
        keys.addAll(removedAtTxKeys());
        keys.addAll(removedAtTxWithBinaryKeys());

        for (int i : keys)
            cache.put(i, new TestValue(i));

        for (int i : removedBeforeTxKeys())
            cache.remove(i);

        for (int i : removedBeforeTxWithBinaryKeys())
            cache.remove(i);
    }

    /** */
    private void initTx(IgniteCache<Integer, TestValue> cache) {
        for (int i : createdAtTxKeys())
            cache.put(i, new TestValue(i));

        for (int i : createdAtTxWithBinaryKeys())
            cache.put(i, new TestValue(i));

        for (int i : removedAtTxKeys())
            cache.remove(i);

        for (int i : removedAtTxWithBinaryKeys())
            cache.remove(i);
    }

    /** */
    private void compareVersionWithPrimaryNode(CacheEntry<Integer, ?> e, IgniteCache<Integer, TestValue> cache) {
        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

        if (cfg.getCacheMode() != CacheMode.LOCAL) {
            Ignite prim = primaryNode(e.getKey(), cache.getName());

            GridCacheAdapter<Object, Object> cacheAdapter = ((IgniteKernal)prim).internalCache(cache.getName());

            if (cfg.getNearConfiguration() != null)
                cacheAdapter = ((GridNearCacheAdapter)cacheAdapter).dht();

            IgniteCacheObjectProcessor cacheObjects = cacheAdapter.context().cacheObjects();

            CacheObjectContext cacheObjCtx = cacheAdapter.context().cacheObjectContext();

            GridCacheMapEntry me = cacheAdapter.map().getEntry(cacheObjects.toCacheKeyObject(
                cacheObjCtx, e.getKey(), true));

            try {
                assertEquals(me.version(), e.version());
            }
            catch (GridCacheEntryRemovedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void checkData(IgniteCache<Integer, TestValue> cache, int i, boolean oneEntry, GridCacheVersion txVer) {
        if (oneEntry) {
            CacheEntry<Integer, TestValue> e = cache.getEntry(i);

            if (txVer != null)
                assertEquals(txVer, e.version());
            else
                compareVersionWithPrimaryNode(e, cache);

            assertEquals(e.getValue().val, i);
        }
        else {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, TestValue>> es = cache.getEntries(set);

            for (CacheEntry<Integer, TestValue> e : es) {
                if (txVer != null)
                    assertEquals(txVer, e.version());
                else
                    compareVersionWithPrimaryNode(e, cache);

                assertEquals((Integer)e.getValue().val, e.getKey());

                assertTrue(set.contains(e.getValue().val));
            }
        }
    }

    private void checkBinaryData(IgniteCache<Integer, TestValue> cache, int i, boolean oneEntry,
        GridCacheVersion txVer) {
        IgniteCache<Integer, BinaryObject> cacheB = cache.withKeepBinary();

        if (oneEntry) {
            CacheEntry<Integer, BinaryObject> e = cacheB.getEntry(i);

            if (txVer != null)
                assertEquals(txVer, e.version());
            else
                compareVersionWithPrimaryNode(e, cache);

            assertEquals(((TestValue)e.getValue().deserialize()).val, i);
        }
        else {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, BinaryObject>> es = cacheB.getEntries(set);

            for (CacheEntry<Integer, BinaryObject> e : es) {
                if (txVer != null)
                    assertEquals(txVer, e.version());
                else
                    compareVersionWithPrimaryNode(e, cache);

                TestValue tv = e.getValue().deserialize();

                assertEquals((Integer)tv.val, e.getKey());

                assertTrue(set.contains((tv).val));
            }
        }
    }

    private void checkRemoved(IgniteCache<Integer, TestValue> cache, int i, boolean oneEntry) {
        if (oneEntry) {
            CacheEntry<Integer, TestValue> e = cache.getEntry(i);

            assertNull(e);
        }
        else {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, TestValue>> es = cache.getEntries(set);

            assertTrue(es.isEmpty());
        }
    }

    private void checkBinaryRemoved(IgniteCache<Integer, TestValue> cache, int i, boolean oneEntry) {
        IgniteCache<Integer, BinaryObject> cacheB = cache.withKeepBinary();

        if (oneEntry) {
            CacheEntry<Integer, BinaryObject> e = cacheB.getEntry(i);

            assertNull(e);
        }
        else {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, BinaryObject>> es = cacheB.getEntries(set);

            assertTrue(es.isEmpty());
        }
    }

    /** */
    private void test(IgniteCache<Integer, TestValue> cache,
        TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation,
        Transaction tx,
        boolean oneEntry) {
        if (tx == null) {
            for (int i : createdBeforeTxKeys()) {
                checkData(cache, i, oneEntry, null);
            }

            for (int i : createdBeforeTxWithBinaryKeys()) {
                checkBinaryData(cache, i, oneEntry, null);
            }

            for (int i : removedBeforeTxKeys()) {
                checkRemoved(cache, i, oneEntry);
            }

            for (int i : removedBeforeTxWithBinaryKeys()) {
                checkBinaryRemoved(cache, i, oneEntry);
            }
        }
        else {
            GridCacheVersion txVer = ((TransactionProxyImpl)tx).tx().xidVersion();

            for (int i : createdBeforeTxKeys2()) {
                checkData(cache, i, oneEntry, null);
                checkData(cache, i, oneEntry, null);
            }

            for (int i : createdBeforeTxWithBinaryKeys2()) {
                checkBinaryData(cache, i, oneEntry, null);
                checkBinaryData(cache, i, oneEntry, null);
            }

            try {
                for (int i : createdBeforeTxKeys3()) {
                    cache.get(i);

                    checkData(cache, i, oneEntry, null);
                }

                for (int i : createdBeforeTxWithBinaryKeys3()) {
                    cache.get(i);

                    checkBinaryData(cache, i, oneEntry, null);
                }

                assertFalse(txIsolation == TransactionIsolation.REPEATABLE_READ &&
                    txConcurrency == TransactionConcurrency.OPTIMISTIC);
            }
            catch (Exception e) {
                assertTrue(txIsolation == TransactionIsolation.REPEATABLE_READ &&
                    txConcurrency == TransactionConcurrency.OPTIMISTIC);
            }

            for (int i : createdAtTxKeys()) {
                checkData(cache, i, oneEntry, txVer);
            }

            for (int i : createdAtTxWithBinaryKeys()) {
                checkBinaryData(cache, i, oneEntry, txVer);
            }

            for (int i : removedBeforeTxKeys()) {
                checkRemoved(cache, i, oneEntry);
            }

            for (int i : removedBeforeTxWithBinaryKeys()) {
                checkBinaryRemoved(cache, i, oneEntry);
            }
            for (int i : removedAtTxKeys()) {
                checkRemoved(cache, i, oneEntry);
            }

            for (int i : removedAtTxWithBinaryKeys()) {
                checkBinaryRemoved(cache, i, oneEntry);
            }
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
