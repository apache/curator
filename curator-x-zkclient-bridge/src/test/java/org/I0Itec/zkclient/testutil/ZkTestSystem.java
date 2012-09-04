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
package org.I0Itec.zkclient.testutil;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.test.Timing;
import com.netflix.curator.x.zkclientbridge.CuratorZKClientBridge;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;
import java.io.IOException;
import java.util.List;

public class ZkTestSystem extends ExternalResource {

    protected static final Logger LOG = Logger.getLogger(ZkTestSystem.class);

    private static int PORT = 10002;
    private static ZkTestSystem _instance;
    private TestingServer _zkServer;
    private ZkClient _zkClient;

    private ZkTestSystem() {
        LOG.info("~~~~~~~~~~~~~~~ starting zk system ~~~~~~~~~~~~~~~");
        try {
            _zkServer = new TestingServer(PORT);
            _zkClient = ZkTestSystem.createZkClient(_zkServer.getConnectString());
        }
        catch ( Exception e ) {
            throw new RuntimeException(e);
        }
        LOG.info("~~~~~~~~~~~~~~~ zk system started ~~~~~~~~~~~~~~~");
    }

    @Override
    // executed before every test method
    protected void before() throws Throwable {
        cleanupZk();
    }

    @Override
    // executed after every test method
    protected void after() {
        cleanupZk();
    }

    private void cleanupZk() {
        LOG.info("cleanup zk namespace");
        List<String> children = getZkClient().getChildren("/");
        for (String child : children) {
            if (!child.equals("zookeeper")) {
                getZkClient().deleteRecursive("/" + child);
            }
        }
        LOG.info("unsubscribing " + getZkClient().numberOfListeners() + " listeners");
        getZkClient().unsubscribeAll();
    }

    public static ZkTestSystem getInstance() {
        if (_instance == null) {
            _instance = new ZkTestSystem();
            _instance.cleanupZk();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    LOG.info("shutting zk down");
                    try {
                        getInstance().getZkClient().close();
                        getInstance().getZkServer().close();
                    }
                    catch ( IOException e ) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        return _instance;
    }

    public TestingServer getZkServer() {
        return _zkServer;
    }

    public String getZkServerAddress() {
        return "localhost:" + getServerPort();
    }

    public ZkClient getZkClient() {
        return _zkClient;
    }

    public int getServerPort() {
        return PORT;
    }

    public static IZkConnection createZkConnection(String connectString) {
        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.newClient(connectString, timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            return new CuratorZKClientBridge(client);
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    public static ZkClient createZkClient(String connectString) {
        try
        {
            Timing                  timing = new Timing();
            return new ZkClient(createZkConnection(connectString), timing.connection());
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    public ZkClient createZkClient() {
        return createZkClient("localhost:" + PORT);
    }

    public void showStructure() {
        getZkClient().showFolders(System.out);
    }

}
