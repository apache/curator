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

package com.netflix.curator.framework.recipes.leader;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     Utility for running a task one only one instance in a group. Uses a {@link LeaderSelector}
 *     internally. When leadership is obtained, the task is signaled to run.
 * </p>
 */
public class LeaderRunner implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Runner runner;
    private final LeaderSelector leaderSelector;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * Task interface
     */
    public interface Runner
    {
        /**
         * Run your task - do not return until done
         *
         * @throws Exception any errors
         */
        public void         run() throws Exception;

        /**
         * When this method is called, your {@link #run()} must exit asap
         *
         * @throws Exception any errors
         */
        public void         exit() throws Exception;
    }

    /**
     * @param client the client
     * @param leaderPath path to use to hold leadership nodes
     * @param runner the task
     */
    public LeaderRunner(CuratorFramework client, String leaderPath, Runner runner)
    {
        this(client, leaderPath, runner, ThreadUtils.newThreadFactory("LeaderRunner"), MoreExecutors.sameThreadExecutor());
    }

    /**
     * @param client the client
     * @param leaderPath path to use to hold leadership nodes
     * @param runner the task
     * @param threadFactory factory to use for making internal threads
     * @param executor the executor to run in
     */
    public LeaderRunner(CuratorFramework client, String leaderPath, Runner runner, ThreadFactory threadFactory, Executor executor)
    {
        this.runner = runner;

        LeaderSelectorListener listener = new LeaderSelectorListener()
        {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception
            {
                postRun();
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) )
                {
                    postExit();
                }
            }
        };
        leaderSelector = new LeaderSelector(client, leaderPath, threadFactory, executor, listener);
        leaderSelector.autoRequeue();
    }

    /**
     * The runner must be started
     *
     * @throws Exception errors
     */
    public void     start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        leaderSelector.start();
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            postExit();
            Closeables.closeQuietly(leaderSelector);
        }
    }

    private void postRun() throws Exception
    {
        runner.run();
    }

    private void postExit()
    {
        try
        {
            runner.exit();
        }
        catch ( Exception e )
        {
            log.error("Sending exit() to runner", e);
        }
    }
}
