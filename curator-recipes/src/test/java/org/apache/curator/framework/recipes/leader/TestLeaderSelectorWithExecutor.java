package org.apache.curator.framework.recipes.leader;

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.ThreadUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TestLeaderSelectorWithExecutor extends BaseClassForTests
{
    private static final ThreadFactory threadFactory = ThreadUtils.newThreadFactory("FeedGenerator");

    @Test
    public void test() throws Exception
    {
        Timing timing = new Timing();
        LeaderSelector leaderSelector = null;
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .retryPolicy(new ExponentialBackoffRetry(100, 3))
            .connectString(server.getConnectString())
            .sessionTimeoutMs(timing.session())
            .connectionTimeoutMs(timing.connection())
            .build();
        try
        {
            client.start();

            MyLeaderSelectorListener listener = new MyLeaderSelectorListener();
            ExecutorService executorPool = Executors.newFixedThreadPool(20);
            leaderSelector = new LeaderSelector(client, "/test", threadFactory, executorPool, listener);

            leaderSelector.autoRequeue();
            leaderSelector.start();

            timing.sleepABit();

            Assert.assertEquals(listener.getLeaderCount(), 1);
        }
        finally
        {
            Closeables.closeQuietly(leaderSelector);
            Closeables.closeQuietly(client);
        }
    }

    private class MyLeaderSelectorListener implements LeaderSelectorListener
    {
        private volatile Thread ourThread;
        private final AtomicInteger leaderCount = new AtomicInteger(0);

        public int getLeaderCount()
        {
            return leaderCount.get();
        }

        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception
        {
            ourThread = Thread.currentThread();
            try
            {
                leaderCount.incrementAndGet();
                while ( !Thread.currentThread().isInterrupted() )
                {
                    Thread.sleep(1000);
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            finally
            {
                leaderCount.decrementAndGet();
            }
        }

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState)
        {
            if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) )
            {
                if ( ourThread != null )
                {
                    ourThread.interrupt();
                }
            }
        }
    }
}
