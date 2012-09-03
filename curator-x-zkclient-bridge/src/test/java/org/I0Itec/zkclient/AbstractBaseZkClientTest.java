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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.curator.test.TestingServer;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.I0Itec.zkclient.testutil.ZkTestSystem;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractBaseZkClientTest {

    protected static final Logger LOG = Logger.getLogger(AbstractBaseZkClientTest.class);
    protected TestingServer _zkServer;
    protected ZkClient _client;

    @Before
    public void setUp() throws Exception {
        LOG.info("------------ BEFORE -------------");

    }

    @After
    public void tearDown() throws Exception {
        LOG.info("------------ AFTER -------------");
    }

    @Test(expected = ZkTimeoutException.class, timeout = 5000)
    public void testUnableToConnect() throws Exception {
        LOG.info("--- testUnableToConnect");
        // we are using port 4711 to avoid conflicts with the zk server that is
        // started by the Spring context
        ZkTestSystem.createZkClient("localhost:4712");
    }

    @Test
    public void testWriteAndRead() throws Exception {
        LOG.info("--- testWriteAndRead");
        String data = "something";
        String path = "/a";
        _client.createPersistent(path, data);
        String data2 = _client.readData(path);
        Assert.assertEquals(data, data2);
        _client.delete(path);
    }

    @Test
    public void testDelete() throws Exception {
        LOG.info("--- testDelete");
        String path = "/a";
        assertFalse(_client.delete(path));
        _client.createPersistent(path, null);
        assertTrue(_client.delete(path));
        assertFalse(_client.delete(path));
    }

    @Test
    public void testDeleteRecursive() throws Exception {
        LOG.info("--- testDeleteRecursive");

        // should be able to call this on a not existing directory
        _client.deleteRecursive("/doesNotExist");
    }

    @Test
    public void testWaitUntilExists() {
        LOG.info("--- testWaitUntilExists");
        // create /gaga node asynchronously
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                    _client.createPersistent("/gaga");
                } catch (Exception e) {
                    // ignore
                }
            }
        }.start();

        // wait until this was created
        assertTrue(_client.waitUntilExists("/gaga", TimeUnit.SECONDS, 5));
        assertTrue(_client.exists("/gaga"));

        // waiting for /neverCreated should timeout
        assertFalse(_client.waitUntilExists("/neverCreated", TimeUnit.MILLISECONDS, 100));
    }

    @Test
    public void testDataChanges1() throws Exception {
        LOG.info("--- testDataChanges1");
        String path = "/a";
        final Holder<String> holder = new Holder<String>();

        IZkDataListener listener = new IZkDataListener() {

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                holder.set((String) data);
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                holder.set(null);
            }
        };
        _client.subscribeDataChanges(path, listener);
        _client.createPersistent(path, "aaa");

        // wait some time to make sure the event was triggered
        String contentFromHolder = TestUtil.waitUntil("b", new Callable<String>() {

            @Override
            public String call() throws Exception {
                return holder.get();
            }
        }, TimeUnit.SECONDS, 5);

        assertEquals("aaa", contentFromHolder);
    }

    @Test
    public void testDataChanges2() throws Exception {
        LOG.info("--- testDataChanges2");
        String path = "/a";
        final AtomicInteger countChanged = new AtomicInteger(0);
        final AtomicInteger countDeleted = new AtomicInteger(0);

        IZkDataListener listener = new IZkDataListener() {

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                countChanged.incrementAndGet();
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                countDeleted.incrementAndGet();
            }
        };
        _client.subscribeDataChanges(path, listener);

        // create node
        _client.createPersistent(path, "aaa");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countChanged.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, countChanged.get());
        assertEquals(0, countDeleted.get());

        countChanged.set(0);
        countDeleted.set(0);
        // delete node, this should trigger a delete event
        _client.delete(path);
        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countDeleted.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(0, countChanged.get());
        assertEquals(1, countDeleted.get());

        // test if watch was reinstalled after the file got deleted
        countChanged.set(0);
        _client.createPersistent(path, "aaa");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countChanged.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, countChanged.get());

        // test if changing the contents notifies the listener
        _client.writeData(path, "bbb");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(2, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return countChanged.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(2, countChanged.get());
    }

    @Test(timeout = 15000)
    public void testHandleChildChanges() throws Exception {
        LOG.info("--- testHandleChildChanges");
        String path = "/a";
        final AtomicInteger count = new AtomicInteger(0);
        final Holder<List<String>> children = new Holder<List<String>>();

        IZkChildListener listener = new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                count.incrementAndGet();
                children.set(currentChilds);
            }
        };
        _client.subscribeChildChanges(path, listener);

        // ----
        // Create the root node should throw the first child change event
        // ----
        _client.createPersistent(path);

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return count.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, count.get());
        assertEquals(0, children.get().size());

        // ----
        // Creating a child node should throw another event
        // ----
        count.set(0);
        _client.createPersistent(path + "/child1");

        // wait some time to make sure the event was triggered
        TestUtil.waitUntil(1, new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return count.get();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals(1, count.get());
        assertEquals(1, children.get().size());
        assertEquals("child1", children.get().get(0));

        // ----
        // Creating another child and deleting the node should also throw an event
        // ----
        count.set(0);
        _client.createPersistent(path + "/child2");
        _client.deleteRecursive(path);

        // wait some time to make sure the event was triggered
        Boolean eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0 && children.get() == null;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertNull(children.get());

        // ----
        // Creating root again should throw an event
        // ----
        count.set(0);
        _client.createPersistent(path);

        // wait some time to make sure the event was triggered
        eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0 && children.get() != null;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertEquals(0, children.get().size());

        // ----
        // Creating child now should throw an event
        // ----
        count.set(0);
        _client.createPersistent(path + "/child");

        // wait some time to make sure the event was triggered
        eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertEquals(1, children.get().size());
        assertEquals("child", children.get().get(0));

        // ----
        // Deleting root node should throw an event
        // ----
        count.set(0);
        _client.deleteRecursive(path);

        // wait some time to make sure the event was triggered
        eventReceived = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return count.get() > 0 && children.get() == null;
            }
        }, TimeUnit.SECONDS, 5);
        assertTrue(eventReceived);
        assertNull(children.get());
    }

    @Test
    public void testGetCreationTime() throws Exception {
        long start = System.currentTimeMillis();
        Thread.sleep(100);
        String path = "/a";
        _client.createPersistent(path);
        Thread.sleep(100);
        long end = System.currentTimeMillis();
        long creationTime = _client.getCreationTime(path);
        assertTrue(start < creationTime && end > creationTime);
    }

    @Test
    public void testNumberOfListeners() {
        IZkChildListener zkChildListener = mock(IZkChildListener.class);
        _client.subscribeChildChanges("/", zkChildListener);
        assertEquals(1, _client.numberOfListeners());

        IZkDataListener zkDataListener = mock(IZkDataListener.class);
        _client.subscribeDataChanges("/a", zkDataListener);
        assertEquals(2, _client.numberOfListeners());

        _client.subscribeDataChanges("/b", zkDataListener);
        assertEquals(3, _client.numberOfListeners());

        IZkStateListener zkStateListener = mock(IZkStateListener.class);
        _client.subscribeStateChanges(zkStateListener);
        assertEquals(4, _client.numberOfListeners());

        _client.unsubscribeChildChanges("/", zkChildListener);
        assertEquals(3, _client.numberOfListeners());

        _client.unsubscribeDataChanges("/b", zkDataListener);
        assertEquals(2, _client.numberOfListeners());

        _client.unsubscribeDataChanges("/a", zkDataListener);
        assertEquals(1, _client.numberOfListeners());

        _client.unsubscribeStateChanges(zkStateListener);
        assertEquals(0, _client.numberOfListeners());
    }
}
