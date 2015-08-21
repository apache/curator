package org.apache.curator.framework.imps;

import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestEnabledSessionExpiredState extends BaseClassForTests
{
    private final Timing timing = new Timing();

    private CuratorFramework client;
    private BlockingQueue<ConnectionState> states;

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        super.setup();

        client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .connectionTimeoutMs(timing.connection())
            .sessionTimeoutMs(timing.session())
            .enableSessionExpiredState()
            .retryPolicy(new RetryOneTime(1))
            .build();
        client.start();

        states = Queues.newLinkedBlockingQueue();
        ConnectionStateListener listener = new ConnectionStateListener()
        {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                states.add(newState);
            }
        };
        client.getConnectionStateListenable().addListener(listener);
    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(client);

        super.teardown();
    }

    @Test
    public void testKillSession() throws Exception
    {
        Assert.assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);

        KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());

        Assert.assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED);
        Assert.assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST);
        Assert.assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED);
    }

    @Test
    public void testReconnectWithoutExpiration() throws Exception
    {
        Assert.assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);
        server.stop();
        try
        {
            client.checkExists().forPath("/");  // any API call that will invoke the retry policy, etc.
        }
        catch ( KeeperException.ConnectionLossException ignore )
        {
        }
        Assert.assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED);
        server.restart();
        client.checkExists().forPath("/");
        Assert.assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED);
    }

    @Override
    protected boolean enabledSessionExpiredStateAware()
    {
        return true;
    }
}
