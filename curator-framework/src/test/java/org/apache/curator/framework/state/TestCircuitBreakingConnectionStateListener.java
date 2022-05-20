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
package org.apache.curator.framework.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.Timing2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class TestCircuitBreakingConnectionStateListener
{
    private final CuratorFramework dummyClient = CuratorFrameworkFactory.newClient("foo", new RetryOneTime(1));
    private final Timing2 timing = new Timing2();
    private final Timing2 retryTiming = timing.multiple(.25);
    private volatile ScheduledThreadPoolExecutor service;

    private static class RecordingListener implements ConnectionStateListener
    {
        final BlockingQueue<ConnectionState> stateChanges = new LinkedBlockingQueue<>();

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            stateChanges.offer(newState);
        }
    }

    private class TestRetryPolicy extends RetryForever
    {
        volatile boolean isRetrying = true;

        public TestRetryPolicy()
        {
            super(retryTiming.milliseconds());
        }

        @Override
        public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper)
        {
            return isRetrying && super.allowRetry(retryCount, elapsedTimeMs, sleeper);
        }
    }

    @BeforeEach
    public void setup()
    {
        service = new ScheduledThreadPoolExecutor(1);
    }

    @AfterEach
    public void tearDown()
    {
        service.shutdownNow();
    }

    @Test
    public void testBasic() throws Exception
    {
        RecordingListener recordingListener = new RecordingListener();
        TestRetryPolicy retryPolicy = new TestRetryPolicy();
        CircuitBreakingConnectionStateListener listener = new CircuitBreakingConnectionStateListener(dummyClient, recordingListener, retryPolicy, service);

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.RECONNECTED);

        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);  // 2nd suspended is ignored
        assertTrue(recordingListener.stateChanges.isEmpty());
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);

        synchronized(listener)  // don't let retry policy run while we're pushing state changes
        {
            listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.LOST);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);   // all further events are ignored - this will be the last event
        }
        retryTiming.multiple(2).sleep();
        assertTrue(recordingListener.stateChanges.isEmpty());

        retryPolicy.isRetrying = false; // retry policy will return false
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);
    }

    @Test
    public void testResetsAfterReconnect() throws Exception
    {
        RecordingListener recordingListener = new RecordingListener();
        TestRetryPolicy retryPolicy = new TestRetryPolicy();
        CircuitBreakingConnectionStateListener listener = new CircuitBreakingConnectionStateListener(dummyClient, recordingListener, retryPolicy, service);

        synchronized(listener)  // don't let retry policy run while we're pushing state changes
        {
            listener.stateChanged(dummyClient, ConnectionState.LOST);
            listener.stateChanged(dummyClient, ConnectionState.LOST);   // second LOST ignored
        }
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        assertTrue(recordingListener.stateChanges.isEmpty());

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);   // causes circuit to close on next retry
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.RECONNECTED);
    }

    @Test
    public void testRetryNever() throws Exception
    {
        RecordingListener recordingListener = new RecordingListener();
        RetryPolicy retryNever = (retryCount, elapsedTimeMs, sleeper) -> false;
        CircuitBreakingConnectionStateListener listener = new CircuitBreakingConnectionStateListener(dummyClient, recordingListener, retryNever, service);

        listener.stateChanged(dummyClient, ConnectionState.LOST);
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        assertFalse(listener.isOpen());
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        assertFalse(listener.isOpen());
    }

    @Test
    public void testRetryOnce() throws Exception
    {
        RecordingListener recordingListener = new RecordingListener();
        RetryPolicy retryOnce = new RetryOneTime(retryTiming.milliseconds());
        CircuitBreakingConnectionStateListener listener = new CircuitBreakingConnectionStateListener(dummyClient, recordingListener, retryOnce, service);

        synchronized(listener)  // don't let retry policy run while we're pushing state changes
        {
            listener.stateChanged(dummyClient, ConnectionState.LOST);
            listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
            assertTrue(listener.isOpen());
        }
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);
        assertFalse(listener.isOpen());
    }

    @Test
    public void testSuspendedToLostRatcheting() throws Exception
    {
        RecordingListener recordingListener = new RecordingListener();
        RetryPolicy retryInfinite = new RetryForever(Integer.MAX_VALUE);
        CircuitBreakingConnectionStateListener listener = new CircuitBreakingConnectionStateListener(dummyClient, recordingListener, retryInfinite, service);

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        assertFalse(listener.isOpen());
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.RECONNECTED);

        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        assertTrue(listener.isOpen());
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        assertTrue(recordingListener.stateChanges.isEmpty());
        assertTrue(listener.isOpen());

        listener.stateChanged(dummyClient, ConnectionState.LOST);
        assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        assertTrue(listener.isOpen());

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        assertTrue(recordingListener.stateChanges.isEmpty());
        assertTrue(listener.isOpen());

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        assertTrue(recordingListener.stateChanges.isEmpty());
        assertTrue(listener.isOpen());
    }
}
