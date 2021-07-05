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
package framework;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import java.util.List;

public class CrudExamples
{
    public static void      create(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        // this will create the given ZNode with the given data
        client.create().forPath(path, payload);
    }

    public static void      createEphemeral(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        // this will create the given EPHEMERAL ZNode with the given data
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
    }

    public static String    createEphemeralSequential(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        // this will create the given EPHEMERAL-SEQUENTIAL ZNode with the given data using Curator protection.

        /*
            Protection Mode:

            It turns out there is an edge case that exists when creating sequential-ephemeral nodes. The creation
            can succeed on the server, but the server can crash before the created node name is returned to the
            client. However, the ZK session is still valid so the ephemeral node is not deleted. Thus, there is no
            way for the client to determine what node was created for them.

            Even without sequential-ephemeral, however, the create can succeed on the sever but the client (for various
            reasons) will not know it. Putting the create builder into protection mode works around this. The name of
            the node that is created is prefixed with a GUID. If node creation fails the normal retry mechanism will
            occur. On the retry, the parent path is first searched for a node that has the GUID in it. If that node is
            found, it is assumed to be the lost node that was successfully created on the first try and is returned to
            the caller.
         */
        return client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, payload);
    }

    public static void      createIdempotent(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        /*
         * This will create the given ZNode with the given data idempotently, meaning that if the initial create
         * failed transiently, it will be retried and behave as if the first create never happened, even if the
         * first create actually succeeded on the server but the client didn't know it.
         */
        client.create().idempotent().forPath(path, payload);
    }

    public static void      setData(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        // set data for the given node
        client.setData().forPath(path, payload);
    }

    public static void      setDataAsync(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        // this is one method of getting event/async notifications
        CuratorListener listener = new CuratorListener()
        {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
            {
                // examine event for details
            }
        };
        client.getCuratorListenable().addListener(listener);

        // set data for the given node asynchronously. The completion notification
        // is done via the CuratorListener.
        client.setData().inBackground().forPath(path, payload);
    }

    public static void      setDataAsyncWithCallback(CuratorFramework client, BackgroundCallback callback, String path, byte[] payload) throws Exception
    {
        // this is another method of getting notification of an async completion
        client.setData().inBackground(callback).forPath(path, payload);
    }

    public static void      setDataIdempotent(CuratorFramework client, String path, byte[] payload, int currentVersion) throws Exception
    {
        /*
         * This will set the given ZNode with the given data idempotently, meaning that if the initial setData
         * failed transiently, it will be retried and behave as if the first setData never happened, even if the
         * first setData actually succeeded on the server but the client didn't know it.
         * In other words, if currentVersion == X and payload = P, this will return success if the znode ends
         * up in the state (version == X+1 && data == P).
         * If withVersion is not specified, it will end up with success so long as the data == P, no matter the znode version.
         */
        client.setData().idempotent().withVersion(currentVersion).forPath(path, payload);
        client.setData().idempotent().forPath(path, payload);
    }



    public static void      delete(CuratorFramework client, String path) throws Exception
    {
        // delete the given node
        client.delete().forPath(path);
    }

    public static void      guaranteedDelete(CuratorFramework client, String path) throws Exception
    {
        // delete the given node and guarantee that it completes

        /*
            Guaranteed Delete

            Solves this edge case: deleting a node can fail due to connection issues. Further, if the node was
            ephemeral, the node will not get auto-deleted as the session is still valid. This can wreak havoc
            with lock implementations.


            When guaranteed is set, Curator will record failed node deletions and attempt to delete them in the
            background until successful. NOTE: you will still get an exception when the deletion fails. But, you
            can be assured that as long as the CuratorFramework instance is open attempts will be made to delete
            the node.
         */

        client.delete().guaranteed().forPath(path);
    }

    public static void      deleteIdempotent(CuratorFramework client, String path, int currentVersion) throws Exception
    {
        /* 
         * This will delete the given ZNode with the given data idempotently, meaning that if the initial delete
         * failed transiently, it will be retried and behave as if the first delete never happened, even if the
         * first delete actually succeeded on the server but the client didn't know it.
         * In other words, if currentVersion == X, this will return success if the znode ends up deleted, and will retry after
         * connection loss if the version the znode's version is still X.
         * If withVersion is not specified, it will end up successful so long as the node is deleted eventually.
         * Kind of like guaranteed but not in the background.
         * For deletes this is equivalent to the older quietly() behavior, but it is also provided under idempotent() for compatibility with Create/SetData.
         */
        client.delete().idempotent().withVersion(currentVersion).forPath(path);
        client.delete().idempotent().forPath(path);
        client.delete().quietly().withVersion(currentVersion).forPath(path);
        client.delete().quietly().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client, String path) throws Exception
    {
        /**
         * Get children and set a watcher on the node. The watcher notification will come through the
         * CuratorListener (see setDataAsync() above).
         */
        return client.getChildren().watched().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client, String path, Watcher watcher) throws Exception
    {
        /**
         * Get children and set the given watcher on the node.
         */
        return client.getChildren().usingWatcher(watcher).forPath(path);
    }
}
