/*
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to allow execution of logic once a ZooKeeper connection becomes available.
 */
public class AfterConnectionEstablished {
    private static final Logger log = LoggerFactory.getLogger(AfterConnectionEstablished.class);

    /**
     * Spawns a new new background thread that will block until a connection is available and
     * then execute the 'runAfterConnection' logic
     *
     * @param client             The curator client
     * @param runAfterConnection The logic to run
     * @return future of the task so it can be canceled, etc. if needed
     */
    public static Future<?> execute(final CuratorFramework client, final Runnable runAfterConnection) throws Exception {
        // Block until connected
        final ExecutorService executor =
                ThreadUtils.newSingleThreadExecutor(ThreadUtils.getProcessName(runAfterConnection.getClass()));
        Runnable internalCall = new Runnable() {
            @Override
            public void run() {
                try {
                    client.blockUntilConnected();
                    runAfterConnection.run();
                } catch (Exception e) {
                    ThreadUtils.checkInterrupted(e);
                    log.error("An error occurred blocking until a connection is available", e);
                } finally {
                    executor.shutdown();
                }
            }
        };
        return executor.submit(internalCall);
    }

    private AfterConnectionEstablished() {}
}
