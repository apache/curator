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
package org.apache.curator.test;

import java.util.concurrent.ExecutorService;

public class ExecuteCalledWatchingExecutorService extends DelegatingExecutorService
{
    boolean executeCalled = false;

    public ExecuteCalledWatchingExecutorService(ExecutorService delegate)
    {
        super(delegate);
    }

    @Override
    public synchronized void execute(Runnable command)
    {
        executeCalled = true;
        super.execute(command);
    }

    public synchronized boolean isExecuteCalled()
    {
        return executeCalled;
    }

    public synchronized void setExecuteCalled(boolean executeCalled)
    {
        this.executeCalled = executeCalled;
    }
}
