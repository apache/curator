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

package org.apache.curator.x.rest.dropwizard;

import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import io.dropwizard.Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.rest.CuratorRestClasses;
import org.apache.curator.x.rest.CuratorRestContext;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import javax.ws.rs.core.Context;

public class CuratorRestBundle implements Bundle
{
    private final CuratorRestContext context;

    public CuratorRestBundle(CuratorFramework client, int sessionLengthMs)
    {
        this.context = new CuratorRestContext(client, sessionLengthMs);
    }

    @Override
    public void initialize(Bootstrap<?> bootstrap)
    {
        // NOP
    }

    @Override
    public void run(Environment environment)
    {
        SingletonTypeInjectableProvider<Context, CuratorRestContext> injectable = new SingletonTypeInjectableProvider<Context, CuratorRestContext>(CuratorRestContext.class, context){};
        environment.jersey().register(injectable);
        for ( Class<?> clazz : CuratorRestClasses.getClasses() )
        {
            environment.jersey().register(clazz);
        }

        LifeCycle.Listener listener = new AbstractLifeCycle.AbstractLifeCycleListener()
        {
            @Override
            public void lifeCycleStarting(LifeCycle event)
            {
                context.start();
            }

            @Override
            public void lifeCycleStopping(LifeCycle event)
            {
                context.close();
            }
        };
        environment.lifecycle().addLifeCycleListener(listener);
    }
}
