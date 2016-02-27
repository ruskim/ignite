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

package org.apache.ignite.internal.processors.cache.datastructures;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Check that all server nodes in grid have configured the same swap space spi.
 * Check that client nodes could have any swap space spi.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
public class GridCacheMultimapSelfTest extends GridCommonAbstractTest {

    protected static final String GRID_NODE = "grid-node";

    protected static final String GRID_CLIENT_1 = "grid-client-1";

    protected static final String GRID_CLIENT_2 = "grid-client-2";

    protected static final String MULTIMAP_NAME = "TestMultimap";

    public GridCacheMultimapSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (GRID_CLIENT_1.equals(gridName) ||
            GRID_CLIENT_2.equals(gridName)) {
            cfg.setClientMode(true);
        }

        return cfg;
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Client nodes should join to grid with any swap policy
     */
    public void testClientNodeAnySwapSpaceSpi() throws Exception {
        startGrid(GRID_NODE);

        Ignite gclnt1 = startGrid(GRID_CLIENT_1);

        Ignite gclnt2 = startGrid(GRID_CLIENT_2);

        CollectionConfiguration cfg = new CollectionConfiguration();

        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setCacheMode(PARTITIONED);

        IgniteMultimap<Integer,String> mmap1 = gclnt1.multimap(MULTIMAP_NAME, cfg);

        mmap1.put(1,"one");
        mmap1.put(1,"two");
        mmap1.put(2, "A");
        mmap1.put(2, "B");

        IgniteMultimap<Integer,String> mmap2 = gclnt2.multimap(MULTIMAP_NAME, cfg);

        assert mmap2.get(1,0).equals("one");
        assert mmap2.get(1,1).equals("two");
        assert mmap2.get(2,0).equals("A");
        assert mmap2.get(2,1).equals("B");
    }
}
