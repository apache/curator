/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.utils;

import com.netflix.curator.CuratorZookeeperClient;
import org.apache.zookeeper.ZooKeeper;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     Utility to ensure that a particular path is created.
 * </p>
 *
 * <p>
 *     The first time it is used, a synchronized call to {@link ZKPaths#mkdirs(ZooKeeper, String)} is made to
 *     ensure that the entire path has been created (with an empty byte array if needed). Subsequent
 *     calls with the instance are un-synchronized NOPs.
 * </p>
 *
 * <p>
 *     Usage:<br/>
 *     <code><pre>
 *         EnsurePath       ensurePath = new EnsurePath(aFullPathToEnsure);
 *         ...
 *         String           nodePath = aFullPathToEnsure + "/foo";
 *         ensurePath.ensure(zk);   // first time syncs and creates if needed
 *         zk.create(nodePath, ...);
 *         ...
 *         ensurePath.ensure(zk);   // subsequent times are NOPs
 *         zk.create(nodePath, ...);
 *     </pre></code>
 * </p>
 */
public class EnsurePath
{
    private final String                    path;
    private final AtomicReference<Helper>   helper = new AtomicReference<Helper>();

    private static final Helper             doNothingHelper = new Helper()
    {
        @Override
        public void ensure(CuratorZookeeperClient client, String path) throws Exception
        {
            // NOP
        }
    };

    private interface Helper
    {
        public void     ensure(CuratorZookeeperClient client, String path) throws Exception;
    }

    /**
     * @param path the full path to ensure
     */
    public EnsurePath(String path)
    {
        this.path = path;
        helper.set(new InitialHelper());
    }

    /**
     * First time, synchronizes and makes sure all nodes in the path are created. Subsequent calls
     * with this instance are NOPs.
     *
     * @param client ZK client
     * @throws Exception ZK errors
     */
    public void     ensure(CuratorZookeeperClient client) throws Exception
    {
        Helper  localHelper = helper.get();
        localHelper.ensure(client, path);
    }

    private class InitialHelper implements Helper
    {
        private boolean         isSet = false;  // guarded by synchronization

        @Override
        public synchronized void ensure(CuratorZookeeperClient client, String path) throws Exception
        {
            if ( !isSet )
            {
                client.blockUntilConnectedOrTimedOut();
                ZKPaths.mkdirs(client.getZooKeeper(), path, true);
                helper.set(doNothingHelper);

                isSet = true;
            }
        }
    }
}
