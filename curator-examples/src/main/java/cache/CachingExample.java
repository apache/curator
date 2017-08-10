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

package cache;

import com.google.common.collect.Lists;
import discovery.ExampleServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.watch.CacheEvent;
import org.apache.curator.framework.recipes.watch.CacheListener;
import org.apache.curator.framework.recipes.watch.CachedNode;
import org.apache.curator.framework.recipes.watch.CuratorCache;
import org.apache.curator.framework.recipes.watch.CuratorCacheBuilder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * An example of the CuratorCache. The example "harness" is a command processor
 * that allows adding/updating/removed nodes in a path. A CuratorCache keeps a
 * cache of these changes and outputs when updates occurs.
 */
public class CachingExample
{
    private static final String PATH = "/example/cache";

    public static void main(String[] args) throws Exception
    {
        TestingServer server = new TestingServer();
        CuratorFramework client = null;
        CuratorCache cache = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.start();

            // in this example we will cache data. Note that this is optional.
            cache = CuratorCacheBuilder.builder(client, PATH).build();
            cache.start();

            processCommands(client, cache);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(server);
        }
    }

    private static void addListener(CuratorCache cache)
    {
        // a PathChildrenCacheListener is optional. Here, it's used just to log changes
        CacheListener listener = new CacheListener()
        {
            @Override
            public void process(CacheEvent event, String path, CachedNode node)
            {
                switch ( event )
                {
                    case NODE_CREATED:
                    {
                        System.out.println("Node added: " + ZKPaths.getNodeFromPath(path));
                        break;
                    }

                    case NODE_CHANGED:
                    {
                        System.out.println("Node changed: " + ZKPaths.getNodeFromPath(path));
                        break;
                    }

                    case NODE_DELETED:
                    {
                        System.out.println("Node removed: " + ZKPaths.getNodeFromPath(path));
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private static void processCommands(CuratorFramework client, CuratorCache cache) throws Exception
    {
        // More scaffolding that does a simple command line processor

        printHelp();

        List<ExampleServer> servers = Lists.newArrayList();
        try
        {
            addListener(cache);

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            boolean done = false;
            while ( !done )
            {
                System.out.print("> ");

                String line = in.readLine();
                if ( line == null )
                {
                    break;
                }

                String command = line.trim();
                String[] parts = command.split("\\s");
                if ( parts.length == 0 )
                {
                    continue;
                }
                String operation = parts[0];
                String args[] = Arrays.copyOfRange(parts, 1, parts.length);

                if ( operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("?") )
                {
                    printHelp();
                }
                else if ( operation.equalsIgnoreCase("q") || operation.equalsIgnoreCase("quit") )
                {
                    done = true;
                }
                else if ( operation.equals("set") )
                {
                    setValue(client, command, args);
                }
                else if ( operation.equals("remove") )
                {
                    remove(client, command, args);
                }
                else if ( operation.equals("list") )
                {
                    list(cache);
                }

                Thread.sleep(1000); // just to allow the console output to catch up
            }
        }
        finally
        {
            for ( ExampleServer server : servers )
            {
                CloseableUtils.closeQuietly(server);
            }
        }
    }

    private static void list(CuratorCache cache)
    {
        if ( cache.size() == 0 )
        {
            System.out.println("* empty *");
        }
        else
        {
            for ( Map.Entry<String, CachedNode> entry : cache.view().entrySet() )
            {
                System.out.println(entry.getKey() + " = " + new String(entry.getValue().getData()));
            }
        }
    }

    private static void remove(CuratorFramework client, String command, String[] args) throws Exception
    {
        if ( args.length != 1 )
        {
            System.err.println("syntax error (expected remove <path>): " + command);
            return;
        }

        String name = args[0];
        if ( name.contains("/") )
        {
            System.err.println("Invalid node name" + name);
            return;
        }
        String path = ZKPaths.makePath(PATH, name);

        try
        {
            client.delete().forPath(path);
        }
        catch ( KeeperException.NoNodeException e )
        {
            // ignore
        }
    }

    private static void setValue(CuratorFramework client, String command, String[] args) throws Exception
    {
        if ( args.length != 2 )
        {
            System.err.println("syntax error (expected set <path> <value>): " + command);
            return;
        }

        String name = args[0];
        if ( name.contains("/") )
        {
            System.err.println("Invalid node name" + name);
            return;
        }
        String path = ZKPaths.makePath(PATH, name);

        byte[] bytes = args[1].getBytes();
        try
        {
            client.setData().forPath(path, bytes);
        }
        catch ( KeeperException.NoNodeException e )
        {
            client.create().creatingParentContainersIfNeeded().forPath(path, bytes);
        }
    }

    private static void printHelp()
    {
        System.out.println("An example of using CuratorCache. This example is driven by entering commands at the prompt:\n");
        System.out.println("set <name> <value>: Adds or updates a node with the given name");
        System.out.println("remove <name>: Deletes the node with the given name");
        System.out.println("list: List the nodes/values in the cache");
        System.out.println("quit: Quit the example");
        System.out.println();
    }
}
