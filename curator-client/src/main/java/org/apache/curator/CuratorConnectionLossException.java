package org.apache.curator;

import org.apache.zookeeper.KeeperException;

/**
 * This is needed to differentiate between ConnectionLossException thrown by ZooKeeper
 * and ConnectionLossException thrown by {@link ConnectionState#checkTimeouts()}
 */
public class CuratorConnectionLossException extends KeeperException.ConnectionLossException
{
}
