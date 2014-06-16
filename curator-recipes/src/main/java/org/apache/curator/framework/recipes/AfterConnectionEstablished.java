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
package org.apache.curator.framework.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Utility class to allow execution of logic once a ZooKeeper connection becomes available.
 */
public class AfterConnectionEstablished
{
    private final static Logger log = LoggerFactory.getLogger(AfterConnectionEstablished.class);

    /**
     * Spawns a new new background thread that will block until a connection is available and
     * then execute the 'runAfterConnection' logic
     *
     * @param client             The curator client
     * @param runAfterConnection The logic to run
     */
    public static <T> T execute(final CuratorFramework client, final Callable<T> runAfterConnection) throws Exception
    {
        //Block until connected
        final ExecutorService executor = ThreadUtils.newSingleThreadExecutor(runAfterConnection.getClass().getSimpleName());
        Callable<T> internalCall = new Callable<T>()
        {
            @Override
            public T call() throws Exception
            {
                try
                {
                    client.blockUntilConnected();
                    return runAfterConnection.call();
                }
                catch ( Exception e )
                {
                    log.error("An error occurred blocking until a connection is available", e);
                    throw e;
                }
                finally
                {
                    executor.shutdown();
                }
            }
        };
        return executor.submit(internalCall).get();
    }

    private AfterConnectionEstablished()
    {
    }
}
