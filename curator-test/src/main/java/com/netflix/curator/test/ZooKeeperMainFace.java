package com.netflix.curator.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.io.Closeable;

public interface ZooKeeperMainFace extends Closeable
{
    public void runFromConfig(QuorumPeerConfig config) throws Exception;

    public void blockUntilStarted() throws Exception;

    public void kill();
}
