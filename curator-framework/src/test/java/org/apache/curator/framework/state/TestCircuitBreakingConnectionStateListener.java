package org.apache.curator.framework.state;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.Timing2;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
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

    @BeforeMethod
    public void setup()
    {
        service = new ScheduledThreadPoolExecutor(1);
    }

    @AfterMethod
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
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.RECONNECTED);

        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);  // 2nd suspended is ignored
        Assert.assertTrue(recordingListener.stateChanges.isEmpty());
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);

        synchronized(listener)  // don't let retry policy run while we're pushing state changes
        {
            listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.LOST);   // all further events are ignored
            listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);   // all further events are ignored - this will be the last event
        }
        retryTiming.multiple(2).sleep();
        Assert.assertTrue(recordingListener.stateChanges.isEmpty());

        retryPolicy.isRetrying = false; // retry policy will return false
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);
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
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        Assert.assertTrue(recordingListener.stateChanges.isEmpty());

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);   // causes circuit to close on next retry
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.RECONNECTED);
    }

    @Test
    public void testRetryNever() throws Exception
    {
        RecordingListener recordingListener = new RecordingListener();
        RetryPolicy retryNever = (retryCount, elapsedTimeMs, sleeper) -> false;
        CircuitBreakingConnectionStateListener listener = new CircuitBreakingConnectionStateListener(dummyClient, recordingListener, retryNever, service);

        listener.stateChanged(dummyClient, ConnectionState.LOST);
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        Assert.assertFalse(listener.isOpen());
        listener.stateChanged(dummyClient, ConnectionState.LOST);
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        Assert.assertFalse(listener.isOpen());
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
            Assert.assertTrue(listener.isOpen());
        }
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);
        Assert.assertFalse(listener.isOpen());
    }

    @Test
    public void testSuspendedToLostRatcheting() throws Exception
    {
        RecordingListener recordingListener = new RecordingListener();
        RetryPolicy retryInfinite = new RetryForever(Integer.MAX_VALUE);
        CircuitBreakingConnectionStateListener listener = new CircuitBreakingConnectionStateListener(dummyClient, recordingListener, retryInfinite, service);

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        Assert.assertFalse(listener.isOpen());
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.RECONNECTED);

        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        Assert.assertTrue(listener.isOpen());
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.SUSPENDED);

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        Assert.assertTrue(recordingListener.stateChanges.isEmpty());
        Assert.assertTrue(listener.isOpen());

        listener.stateChanged(dummyClient, ConnectionState.LOST);
        Assert.assertEquals(timing.takeFromQueue(recordingListener.stateChanges), ConnectionState.LOST);
        Assert.assertTrue(listener.isOpen());

        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        listener.stateChanged(dummyClient, ConnectionState.RECONNECTED);
        listener.stateChanged(dummyClient, ConnectionState.READ_ONLY);
        listener.stateChanged(dummyClient, ConnectionState.SUSPENDED);
        Assert.assertTrue(recordingListener.stateChanges.isEmpty());
        Assert.assertTrue(listener.isOpen());

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
        Assert.assertTrue(recordingListener.stateChanges.isEmpty());
        Assert.assertTrue(listener.isOpen());
    }
}
