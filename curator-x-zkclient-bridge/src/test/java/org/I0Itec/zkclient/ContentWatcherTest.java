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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.netflix.curator.test.TestingServer;
import org.I0Itec.zkclient.testutil.ZkTestSystem;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ContentWatcherTest {

    private static final Logger LOG = Logger.getLogger(ContentWatcherTest.class);

    private static final String FILE_NAME = "/ContentWatcherTest";
    private TestingServer _zkServer;
    private ZkClient _zkClient;

    @Before
    public void setUp() throws Exception {
        LOG.info("------------ BEFORE -------------");
        _zkServer = new TestingServer(4711);
        _zkClient = ZkTestSystem.createZkClient(_zkServer.getConnectString());
    }

    @After
    public void tearDown() throws Exception {
        if (_zkClient != null) {
            _zkClient.close();
        }
        if (_zkServer != null) {
            _zkServer.close();
        }
        LOG.info("------------ AFTER -------------");
    }

    @Test
    public void testGetContent() throws Exception {
        LOG.info("--- testGetContent");
        _zkClient.createPersistent(FILE_NAME, "a");
        final ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
        watcher.start();
        assertEquals("a", watcher.getContent());

        // update the content
        _zkClient.writeData(FILE_NAME, "b");

        String contentFromWatcher = TestUtil.waitUntil("b", new Callable<String>() {

            @Override
            public String call() throws Exception {
                return watcher.getContent();
            }
        }, TimeUnit.SECONDS, 5);

        assertEquals("b", contentFromWatcher);
        watcher.stop();
    }

    @Test
    public void testGetContentWaitTillCreated() throws InterruptedException {
        LOG.info("--- testGetContentWaitTillCreated");
        final Holder<String> contentHolder = new Holder<String>();

        Thread thread = new Thread() {
            @Override
            public void run() {
                ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
                try {
                    watcher.start();
                    contentHolder.set(watcher.getContent());
                    watcher.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        thread.start();

        // create content after 200ms
        Thread.sleep(200);
        _zkClient.createPersistent(FILE_NAME, "aaa");

        // we give the thread some time to pick up the change
        thread.join(1000);
        assertEquals("aaa", contentHolder.get());
    }

    @Test
    public void testHandlingNullContent() throws InterruptedException {
        LOG.info("--- testHandlingNullContent");
        _zkClient.createPersistent(FILE_NAME, null);
        ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
        watcher.start();
        assertEquals(null, watcher.getContent());
        watcher.stop();
    }

    @Test(timeout = 20000)
    public void testHandlingOfConnectionLoss() throws Exception {
        LOG.info("--- testHandlingOfConnectionLoss");
        final Gateway gateway = new Gateway(4712, 4711);
        gateway.start();
        final ZkClient zkClient = ZkTestSystem.createZkClient("localhost:4712");

        // disconnect
        gateway.stop();

        // reconnect after 250ms and create file with content
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(250);
                    gateway.start();
                    zkClient.createPersistent(FILE_NAME, "aaa");
                    zkClient.writeData(FILE_NAME, "b");
                } catch (Exception e) {
                    // ignore
                }
            }
        }.start();

        final ContentWatcher<String> watcher = new ContentWatcher<String>(zkClient, FILE_NAME);
        watcher.start();

        TestUtil.waitUntil("b", new Callable<String>() {

            @Override
            public String call() throws Exception {
                return watcher.getContent();
            }
        }, TimeUnit.SECONDS, 5);
        assertEquals("b", watcher.getContent());

        watcher.stop();
        zkClient.close();
        gateway.stop();
    }
}
