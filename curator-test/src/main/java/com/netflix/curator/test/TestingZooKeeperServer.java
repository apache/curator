package com.netflix.curator.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thanks to Jérémie BORDIER (ahfeel) for this code
 */
public class TestingZooKeeperServer extends QuorumPeerMain implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(TestingZooKeeperServer.class);

    private final QuorumConfigBuilder configBuilder;
    private final int thisInstanceIndex;
    private final ZooKeeperMainFace main;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED,
        CLOSED
    }

    public TestingZooKeeperServer(QuorumConfigBuilder configBuilder)
    {
        this(configBuilder, 0);
    }

    public TestingZooKeeperServer(QuorumConfigBuilder configBuilder, int thisInstanceIndex)
    {
        this.configBuilder = configBuilder;
        this.thisInstanceIndex = thisInstanceIndex;
        main = (configBuilder.size() > 1) ? new TestingQuorumPeerMain() : new TestingZooKeeperMain();
    }

    public void     kill()
    {
        main.kill();
        state.set(State.STOPPED);
    }

    public void     stop() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.STOPPED) )
        {
            main.close();
        }
    }

    public InstanceSpec     getInstanceSpec()
    {
        return configBuilder.getInstanceSpec(thisInstanceIndex);
    }

    @Override
    public void close() throws IOException
    {
        stop();

        if ( state.compareAndSet(State.STOPPED, State.CLOSED) )
        {
            InstanceSpec        spec = getInstanceSpec();
            if ( spec.deleteDataDirectoryOnClose() )
            {
                DirectoryUtils.deleteRecursively(spec.getDataDirectory());
            }
        }
    }

    public void start() throws Exception
    {
        if ( !state.compareAndSet(State.LATENT, State.STARTED) )
        {
            return;
        }

        new Thread
        (
            new Runnable()
            {
                public void run()
                {
                    try
                    {
                        QuorumPeerConfig config = configBuilder.buildConfig(thisInstanceIndex);
                        main.runFromConfig(config);
                    }
                    catch ( Exception e )
                    {
                        logger.error("From testing server (random state: %s)", String.valueOf(configBuilder.isFromRandom()), e);
                    }
                }
            }
        ).start();

        main.blockUntilStarted();
    }
}