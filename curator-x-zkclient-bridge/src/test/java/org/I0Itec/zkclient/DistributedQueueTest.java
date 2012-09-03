/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient;

import com.netflix.curator.test.TestingServer;
import org.I0Itec.zkclient.testutil.ZkTestSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DistributedQueueTest {

    private TestingServer _zkServer;
    private ZkClient _zkClient;

    @Before
    public void setUp() throws Exception {
        _zkServer = new TestingServer(4711);
        _zkClient = ZkTestSystem.createZkClient(_zkServer.getConnectString());
    }

    @After
    public void tearDown() throws IOException {
        if (_zkClient != null) {
            _zkClient.close();
        }
        if (_zkServer != null) {
            _zkServer.close();
        }
    }

    @Test(timeout = 15000)
    public void testDistributedQueue() {
        _zkClient.createPersistent("/queue");

        DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(_zkClient, "/queue");
        distributedQueue.offer(17L);
        distributedQueue.offer(18L);
        distributedQueue.offer(19L);

        assertEquals(Long.valueOf(17L), distributedQueue.poll());
        assertEquals(Long.valueOf(18L), distributedQueue.poll());
        assertEquals(Long.valueOf(19L), distributedQueue.poll());
        assertNull(distributedQueue.poll());
    }

    @Test(timeout = 15000)
    public void testPeek() {
        _zkClient.createPersistent("/queue");

        DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(_zkClient, "/queue");
        distributedQueue.offer(17L);
        distributedQueue.offer(18L);

        assertEquals(Long.valueOf(17L), distributedQueue.peek());
        assertEquals(Long.valueOf(17L), distributedQueue.peek());
        assertEquals(Long.valueOf(17L), distributedQueue.poll());
        assertEquals(Long.valueOf(18L), distributedQueue.peek());
        assertEquals(Long.valueOf(18L), distributedQueue.poll());
        assertNull(distributedQueue.peek());
    }

    @Test(timeout = 30000)
    public void testMultipleReadingThreads() throws InterruptedException {
        _zkClient.createPersistent("/queue");

        final DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(_zkClient, "/queue");

        // insert 100 elements
        for (int i = 0; i < 100; i++) {
            distributedQueue.offer(new Long(i));
        }

        // 3 reading threads
        final Set<Long> readElements = Collections.synchronizedSet(new HashSet<Long>());
        List<Thread> threads = new ArrayList<Thread>();
        final List<Exception> exceptions = new Vector<Exception>();

        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            Long value = distributedQueue.poll();
                            if (value == null) {
                                return;
                            }
                            readElements.add(value);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                        e.printStackTrace();
                    }
                }
            };
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(0, exceptions.size());
        assertEquals(100, readElements.size());
    }
}
