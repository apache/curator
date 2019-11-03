/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.curator.framework.recipes.cache.CuratorCache.Options.DO_NOT_CLEAR_ON_CLOSE;
import static org.apache.curator.framework.recipes.cache.CuratorCacheListener.builder;

/**
 * Randomly create nodes in a tree while a set of CuratorCaches listens. Afterwards, validate
 * that the caches contain the same values as ZK itself
 */
@Test(groups = CuratorTestBase.zk36Group)
public class TestCuratorCacheConsistency extends CuratorTestBase
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private static final Duration testLength = Duration.ofSeconds(30);
    private static final Duration thirdOfTestLength = Duration.ofMillis(testLength.toMillis() / 3);
    private static final Duration sleepLength = Duration.ofMillis(5);
    private static final int nodesPerLevel = 10;
    private static final int clusterSize = 5;
    private static final int maxServerKills = 2;

    private static final String BASE_PATH = "/test";

    private class Client implements Closeable
    {
        private final CuratorFramework client;
        private final CuratorCache cache;
        private final int index;
        private final Map<String, ChildData> listenerDataMap = new HashMap<>();

        Client(int index, String connectionString, AtomicReference<Exception> errorSignal)
        {
            this.index = index;
            client = buildClient(connectionString);
            cache = CuratorCache.builder(client, BASE_PATH).withOptions(DO_NOT_CLEAR_ON_CLOSE).withExceptionHandler(errorSignal::set).build();

            // listenerDataMap is a local data map that will hold values sent by listeners
            // this way, the listener code can be tested for validity and consistency
            CuratorCacheListener listener = builder().forCreates(node -> {
                ChildData previous = listenerDataMap.put(node.getPath(), node);
                if ( previous != null )
                {
                    errorSignal.set(new Exception(String.format("Client: %d - Create for existing node: %s", index, node.getPath())));
                }
            }).forChanges((oldNode, node) -> {
                ChildData previous = listenerDataMap.put(node.getPath(), node);
                if ( (previous == null) || !Arrays.equals(previous.getData(), oldNode.getData()) )
                {
                    errorSignal.set(new Exception(String.format("Client: %d - Bad old value for change node: %s", index, node.getPath())));
                }
            }).forDeletes(node -> {
                ChildData previous = listenerDataMap.remove(node.getPath());
                if ( previous == null )
                {
                    errorSignal.set(new Exception(String.format("Client: %d - Delete for non-existent node: %s", index, node.getPath())));
                }
            }).build();
            cache.listenable().addListener(listener);
        }

        void start()
        {
            client.start();
            cache.start();
        }

        @Override
        public void close()
        {
            cache.close();
            client.close();
        }
    }

    @Test
    public void testConsistencyAfterSimulation() throws Exception
    {
        int clientQty = random.nextInt(10, 20);
        int maxDepth = random.nextInt(5, 10);

        log.info("clientQty: {}, maxDepth: {}", clientQty, maxDepth);

        List<Client> clients = Collections.emptyList();
        Map<String, String> actualTree;

        AtomicReference<Exception> errorSignal = new AtomicReference<>();
        try (TestingCluster cluster = new TestingCluster(clusterSize))
        {
            cluster.start();

            initializeBasePath(cluster);
            try
            {
                clients = buildClients(cluster, clientQty, errorSignal);
                workLoop(cluster, clients, maxDepth, errorSignal);

                log.info("Test complete - sleeping to allow events to complete");
                timing.sleepABit();
            }
            finally
            {
                clients.forEach(Client::close);
            }

            actualTree = buildActual(cluster);
        }

        log.info("client qty: {}", clientQty);

        Map<Integer, List<String>> errorsList = clients.stream()
            .map(client ->  findErrors(client, actualTree))
            .filter(errorsEntry -> !errorsEntry.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if ( !errorsList.isEmpty() )
        {
            log.error("{} clients had errors", errorsList.size());
            errorsList.forEach((index, errorList) -> {
                log.error("Client {}", index);
                errorList.forEach(log::error);
                log.error("");
            });

            Assert.fail("Errors found");
        }
    }

    // build a data map recursively from the actual values in ZK
    private Map<String, String> buildActual(TestingCluster cluster)
    {
        Map<String, String> actual = new HashMap<>();
        try (CuratorFramework client = buildClient(cluster.getConnectString()))
        {
            client.start();
            buildActual(client, actual, BASE_PATH);
        }
        return actual;
    }

    private void buildActual(CuratorFramework client, Map<String, String> actual, String fromPath)
    {
        try
        {
            byte[] bytes = client.getData().forPath(fromPath);
            actual.put(fromPath, new String(bytes));
            client.getChildren().forPath(fromPath).forEach(child -> buildActual(client, actual, ZKPaths.makePath(fromPath, child)));
        }
        catch ( Exception e )
        {
            Assert.fail("", e);
        }
    }

    private List<Client> buildClients(TestingCluster cluster, int clientQty, AtomicReference<Exception> errorSignal)
    {
        return IntStream.range(0, clientQty)
            .mapToObj(index -> new Client(index, cluster.getConnectString(), errorSignal))
            .peek(Client::start)
            .collect(Collectors.toList());
    }

    private void initializeBasePath(TestingCluster cluster) throws Exception
    {
        try (CuratorFramework client = buildClient(cluster.getConnectString()))
        {
            client.start();
            client.create().forPath(BASE_PATH, "".getBytes());
        }
    }

    private void workLoop(TestingCluster cluster, List<Client> clients, int maxDepth, AtomicReference<Exception> errorSignal) throws Exception
    {
        Instant start = Instant.now();
        Instant lastServerKill = Instant.now();
        int serverKillIndex = 0;
        while ( true )
        {
            Duration elapsed = Duration.between(start, Instant.now());
            if ( elapsed.compareTo(testLength) >= 0 )
            {
                break;
            }

            Exception errorSignalException = errorSignal.get();
            if ( errorSignalException != null )
            {
                Assert.fail("A client's error handler was called", errorSignalException);
            }

            Duration elapsedFromLastServerKill = Duration.between(lastServerKill, Instant.now());
            if ( elapsedFromLastServerKill.compareTo(thirdOfTestLength) >= 0 )
            {
                lastServerKill = Instant.now();
                if ( serverKillIndex < maxServerKills )
                {
                    doKillServer(cluster, serverKillIndex++);
                }
            }

            int thisDepth = random.nextInt(0, maxDepth);
            String thisPath = randomPath(thisDepth);
            CuratorFramework client = randomClient(clients);
            if ( random.nextBoolean() )
            {
                doDelete(client, thisPath);
            }
            else
            {
                doChange(client, thisPath);
            }

            Thread.sleep(sleepLength.toMillis());
        }
    }

    private void doChange(CuratorFramework client, String thisPath)
    {
        try
        {
            String thisData = Long.toString(random.nextLong());
            client.create().orSetData().creatingParentsIfNeeded().forPath(thisPath, thisData.getBytes());
        }
        catch ( Exception e )
        {
            Assert.fail("Could not create/set: " + thisPath);
        }
    }

    private void doDelete(CuratorFramework client, String thisPath)
    {
        if ( thisPath.equals(BASE_PATH) )
        {
            return;
        }
        try
        {
            client.delete().quietly().deletingChildrenIfNeeded().forPath(thisPath);
        }
        catch ( Exception e )
        {
            Assert.fail("Could not delete: " + thisPath);
        }
    }

    private void doKillServer(TestingCluster cluster, int serverKillIndex) throws Exception
    {
        log.info("Killing server {}", serverKillIndex);
        InstanceSpec killSpec = new ArrayList<>(cluster.getInstances()).get(serverKillIndex);
        cluster.killServer(killSpec);
    }

    private CuratorFramework randomClient(List<Client> clients)
    {
        return clients.get(random.nextInt(clients.size())).client;
    }

    private Map.Entry<Integer, List<String>> findErrors(Client client, Map<String, String> tree)
    {
        CuratorCacheStorage storage = ((CuratorCacheImpl)client.cache).storage();
        List<String> errors = new ArrayList<>();
        if ( tree.size() != storage.size() )
        {
            errors.add(String.format("Size mismatch. Expected: %d - Actual: %d", tree.size(), storage.size()));
        }
        tree.keySet().forEach(path -> {
            if ( !storage.get(path).isPresent() )
            {
                errors.add(String.format("Path %s in master but not client", path));
            }
        });
        storage.stream().forEach(data -> {
            String treeValue = tree.get(data.getPath());
            if ( treeValue != null )
            {
                if ( !treeValue.equals(new String(data.getData())) )
                {
                    errors.add(String.format("Data at %s is not the same", data.getPath()));
                }

                ChildData listenersMapData = client.listenerDataMap.get(data.getPath());
                if ( listenersMapData == null )
                {
                    errors.add(String.format("listenersMap missing data at: %s", data.getPath()));
                }
                else if ( !treeValue.equals(new String(listenersMapData.getData())) )
                {
                    errors.add(String.format("Data at %s in listenersMap is not the same", data.getPath()));
                }
            }
            else
            {
                errors.add(String.format("Path %s in client but not master", data.getPath()));
            }
        });

        client.listenerDataMap.keySet().forEach(path -> {
            if ( !storage.get(path).isPresent() )
            {
                errors.add(String.format("Path %s in listenersMap but not storage", path));
            }
        });

        return new AbstractMap.SimpleEntry<>(client.index, errors);
    }

    private String randomPath(int depth)
    {
        StringBuilder str = new StringBuilder(BASE_PATH);
        while ( depth-- > 0 )
        {
            int levelNodeName = random.nextInt(nodesPerLevel);
            str.append("/").append(levelNodeName);
        }
        return str.toString();
    }

    @Override
    protected void createServer()
    {
        // do nothing - we'll be using TestingCluster instead
    }

    private CuratorFramework buildClient(String connectionString)
    {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(100, 100);
        return CuratorFrameworkFactory.newClient(connectionString, timing.session(), timing.connection(), retryPolicy);
    }
}