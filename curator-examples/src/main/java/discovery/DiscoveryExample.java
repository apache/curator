/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package discovery;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;

public class DiscoveryExample
{
    private static final String     PATH = "/discovery/example";

    public static void main(String[] args) throws Exception
    {
        TestingServer       server = new TestingServer();
        CuratorFramework    client = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.start();

            processCommands(client);
        }
        finally
        {
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(server);
        }
    }

    private static void processCommands(CuratorFramework client) throws Exception
    {
        printHelp();

        List<ExampleServer>     servers = Lists.newArrayList();
        try
        {
            BufferedReader          in = new BufferedReader(new InputStreamReader(System.in));
            boolean                 done = false;
            while ( !done )
            {
                System.out.print("> ");

                String      command = in.readLine().trim();
                if ( command.equalsIgnoreCase("help") || command.equalsIgnoreCase("?") )
                {
                    printHelp();
                }
                else if ( command.equalsIgnoreCase("q") || command.equalsIgnoreCase("quit") )
                {
                    done = true;
                }
                else if ( command.startsWith("add ") )
                {
                    addInstance(client, command, servers);
                }
                else if ( command.startsWith("delete ") )
                {
                    deleteInstance(command, servers);
                }
                else if ( command.equals("list") )
                {
                    listInstances(client);
                }
            }
        }
        finally
        {
            for ( ExampleServer server : servers )
            {
                Closeables.closeQuietly(server);
            }
        }
    }

    private static void listInstances(CuratorFramework client) throws Exception
    {
        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class);
        ServiceDiscovery<InstanceDetails> serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class).client(client).basePath(PATH).serializer(serializer).build();
        try
        {
            serviceDiscovery.start();

            Collection<String>  serviceNames = serviceDiscovery.queryForNames();
            System.out.println(serviceNames.size() + " type(s)");
            for ( String serviceName : serviceNames )
            {
                Collection<ServiceInstance<InstanceDetails>> instances = serviceDiscovery.queryForInstances(serviceName);
                System.out.println(serviceName);
                for ( ServiceInstance<InstanceDetails> instance : instances )
                {
                    System.out.println("\t" + instance.getPayload().getDescription() + ": " + instance.buildUriSpec());
                }
            }
        }
        finally
        {
            Closeables.closeQuietly(serviceDiscovery);
        }
    }

    private static void deleteInstance(String command, List<ExampleServer> servers)
    {
        String[]        parts = command.split("\\s");
        if ( parts.length != 2 )
        {
            System.err.println("syntax error (expected delete <name>): " + command);
            return;
        }

        final String        serverName = parts[1];
        ExampleServer   server = Iterables.find
        (
            servers,
            new Predicate<ExampleServer>()
            {
                @Override
                public boolean apply(ExampleServer server)
                {
                    return server.getThisInstance().getName().endsWith(serverName);
                }
            },
            null
        );
        if ( server == null )
        {
            System.err.println("No servers found named: " + serverName);
            return;
        }

        servers.remove(server);
        Closeables.closeQuietly(server);
        System.out.println("Removed a random instance of: " + serverName);
    }

    private static void addInstance(CuratorFramework client, String command, List<ExampleServer> servers) throws Exception
    {
        String[]        parts = command.split("\\s");
        if ( parts.length < 3 )
        {
            System.err.println("syntax error (expected add <name> <description>): " + command);
            return;
        }

        StringBuilder   description = new StringBuilder();
        for ( int i = 2; i < parts.length; ++i )
        {
            if ( i > 2 )
            {
                description.append(' ');
            }
            description.append(parts[i]);
        }

        ExampleServer   server = new ExampleServer(client, PATH, parts[1], description.toString());
        servers.add(server);
        server.start();

        System.out.println(parts[1] + " added");
    }

    private static void printHelp()
    {
        System.out.println("An example of using the ServiceDiscovery APIs. This example is driven by entering commands at the prompt:\n");
        System.out.println("add <name> <description>: Adds a mock service with the given name and description");
        System.out.println("delete <name>: Deletes one of the mock services with the given name");
        System.out.println("list: Lists all the currently registered services");
        System.out.println("quit: Quit the example");
        System.out.println();
    }
}
