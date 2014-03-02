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

package org.apache.curator.x.rest.support;

import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.sun.jersey.api.client.Client;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.DebugUtils;
import org.apache.curator.x.rest.dropwizard.CuratorApplication;
import org.apache.curator.x.rest.dropwizard.CuratorConfiguration;
import org.apache.curator.x.rest.dropwizard.CuratorRestBundle;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.thread.ShutdownThread;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import java.io.File;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class BaseClassForTests
{
    protected TestingServer server;
    protected Application<CuratorConfiguration> application;
    protected Client restClient;

    protected static final int PORT = 8080;

    private File configFile;

    @BeforeMethod
    public void     setup() throws Exception
    {
        restClient = Client.create();

        System.setProperty(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
        server = new TestingServer();

        configFile = File.createTempFile("temp", ".tmp");
        CharStreams.write("{\"connection-string\": \"" + server.getConnectString() + "\"}", Files.newWriterSupplier(configFile, Charset.defaultCharset()));

        final CountDownLatch startedLatch = new CountDownLatch(1);
        application = new Application<CuratorConfiguration>()
        {
            @Override
            public void initialize(Bootstrap<CuratorConfiguration> bootstrap)
            {
                bootstrap.addBundle(new CuratorRestBundle());
            }

            @Override
            public void run(CuratorConfiguration configuration, Environment environment) throws Exception
            {
                LifeCycle.Listener listener = new AbstractLifeCycle.AbstractLifeCycleListener()
                {
                    @Override
                    public void lifeCycleStarted(LifeCycle event)
                    {
                        startedLatch.countDown();
                    }
                };
                environment.lifecycle().addLifeCycleListener(listener);
            }
        };

        Executors.newSingleThreadExecutor().submit
        (
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    String[] args = new String[]{"server", configFile.getPath()};
                    CuratorApplication.run(application, args);
                    return null;
                }
            }
        );

        startedLatch.await();
    }

    @AfterMethod
    public void     teardown() throws Exception
    {
        if ( configFile != null )
        {
            //noinspection ResultOfMethodCallIgnored
            configFile.delete();
        }

        if ( restClient != null )
        {
            restClient.destroy();
        }

        server.close();
        ShutdownThread.getInstance().run();
    }
}
