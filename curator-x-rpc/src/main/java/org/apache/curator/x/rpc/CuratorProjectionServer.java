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
package org.apache.curator.x.rpc;

import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.curator.x.rpc.idl.event.EventService;
import org.apache.curator.x.rpc.idl.projection.CuratorProjectionService;
import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.introspect.AnnotatedMethod;
import org.codehaus.jackson.map.introspect.NopAnnotationIntrospector;
import org.codehaus.jackson.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorProjectionServer
{
    private final RpcManager rpcManager;
    private final ThriftServer server;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    public static void main(String[] args) throws IOException
    {
        if ( (args.length == 0) || args[0].equalsIgnoreCase("?") || args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("--help") )
        {
            printHelp();
            return;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String options;
        File f = new File(args[0]);
        if ( f.exists() )
        {
            options = Files.toString(f, Charset.defaultCharset());
        }
        else
        {
            System.out.println("First argument is not a file. Treating the command line as a list of field/values");
            options = buildOptions(objectMapper, args);
        }

        AnnotationIntrospector introspector = new NopAnnotationIntrospector()
        {
            @Override
            public String findSettablePropertyName(AnnotatedMethod am)
            {
                Config config = am.getAnnotated().getAnnotation(Config.class);
                return (config != null) ? config.value() : super.findSettablePropertyName(am);
            }
        };
        DeserializationConfig deserializationConfig = objectMapper.getDeserializationConfig().withAnnotationIntrospector(introspector);
        objectMapper.setDeserializationConfig(deserializationConfig);
        Configuration configuration = objectMapper.reader().withType(Configuration.class).readValue(options);

        final CuratorProjectionServer server = new CuratorProjectionServer(configuration);
        server.start();

        Runnable shutdown = new Runnable()
        {
            @Override
            public void run()
            {
                server.stop();
            }
        };
        Thread hook = new Thread(shutdown);
        Runtime.getRuntime().addShutdownHook(hook);
    }

    public CuratorProjectionServer(Configuration thriftServerConfig)
    {
        rpcManager = new RpcManager(TimeUnit.SECONDS.toMillis(10));
        EventService eventService = new EventService(rpcManager, 5000); // TODO
        CuratorProjectionService projectionService = new CuratorProjectionService(rpcManager);
        ThriftServiceProcessor processor = new ThriftServiceProcessor(new ThriftCodecManager(), Lists.<ThriftEventHandler>newArrayList(), projectionService, eventService);
        server = new ThriftServer(processor, thriftServerConfig);
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        server.start();
    }

    public void stop()
    {
        if ( state.compareAndSet(State.STARTED, State.STOPPED) )
        {
            rpcManager.close();
            server.close();
        }
    }

    private static void printHelp()
    {
        System.out.println("Curator RPC - an RPC server for using Apache Curator APIs and recipes from non JVM languages.");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("\t<none>              show this help");
        System.out.println("\t<path>              path to a JSON configuration file");
        System.out.println("\t<field value> ...   list of values that would be in the JSON configuration file");
        System.out.println();
        System.out.println("Values:");

        for ( Method method : Configuration.class.getMethods() )
        {
            Config config = method.getAnnotation(Config.class);
            if ( (config != null) && (method.getParameterTypes().length == 1) )
            {
                System.out.println("\t" + config.value() + ": " + getType(method));
            }
        }
    }

    private static String getType(Method method)
    {
        Class<?> type = method.getParameterTypes()[0];
        String result = type.getSimpleName();
        if ( type.equals(Duration.class) )
        {
            result += example(new Duration(10, TimeUnit.MINUTES));
        }
        else if ( type.equals(DataSize.class) )
        {
            result += example(new DataSize(1.5, DataSize.Unit.GIGABYTE));
        }
        return result;
    }

    private static String example(Object s)
    {
        return " (e.g. \"" + s + "\")";
    }

    private static String buildOptions(ObjectMapper objectMapper, String[] args) throws IOException
    {
        ObjectNode node = objectMapper.createObjectNode();
        for ( int i = 0; i < args.length; i += 2 )
        {
            if ( (i + 1) >= args.length )
            {
                throw new IOException("Bad command line. Must be list of fields and values of the form: \"field1 value1 ... fieldN valueN\"");
            }
            node.put(args[i], args[i + 1]);
        }
        return objectMapper.writeValueAsString(node);
    }
}
