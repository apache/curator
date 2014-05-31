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
package org.apache.curator.x.rpc.configuration;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.airlift.units.Duration;
import io.dropwizard.logging.AppenderFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TestConfigurationBuilder
{
    @Test
    public void testSimple() throws Exception
    {
        Configuration configuration = loadTestConfiguration("configuration/simple.json");
        Assert.assertEquals(configuration.getThrift().getPort(), 1234);
        Assert.assertEquals(configuration.getPingTime(), new Duration(10, TimeUnit.SECONDS));
    }

    @Test
    public void testLogging() throws Exception
    {
        Configuration configuration = loadTestConfiguration("configuration/logging.json");
        Assert.assertEquals(configuration.getLogging().getLevel(), Level.INFO);
        Assert.assertEquals(configuration.getLogging().getAppenders().size(), 2);

        Set<String> types = Sets.newHashSet();
        for ( AppenderFactory appenderFactory : configuration.getLogging().getAppenders() )
        {
            types.add(appenderFactory.getClass().getSimpleName());
        }
        Assert.assertEquals(types, Sets.newHashSet("FileAppenderFactory", "ConsoleAppenderFactory"));
    }

    @Test
    public void testConnections() throws Exception
    {
        Configuration configuration = loadTestConfiguration("configuration/connections.json");
        Assert.assertEquals(configuration.getConnections().size(), 2);

        Assert.assertEquals(configuration.getConnections().get(0).getName(), "test");
        Assert.assertEquals(configuration.getConnections().get(0).getConnectionString(), "one:1,two:2");
        Assert.assertEquals(configuration.getConnections().get(0).getConnectionTimeout(), new Duration(20, TimeUnit.SECONDS));
        Assert.assertEquals(configuration.getConnections().get(0).getRetry().build().getClass(), ExponentialBackoffRetry.class);

        Assert.assertEquals(configuration.getConnections().get(1).getName(), "alt");
        Assert.assertEquals(configuration.getConnections().get(1).getConnectionString(), "three:3,four:4");
        Assert.assertEquals(configuration.getConnections().get(1).getConnectionTimeout(), new Duration(30, TimeUnit.SECONDS));
        Assert.assertEquals(configuration.getConnections().get(1).getRetry().build().getClass(), RetryNTimes.class);
    }

    private Configuration loadTestConfiguration(String name) throws Exception
    {
        URL resource = Resources.getResource(name);
        String source = Resources.toString(resource, Charset.defaultCharset());
        return new ConfigurationBuilder(source).build();
    }
}
