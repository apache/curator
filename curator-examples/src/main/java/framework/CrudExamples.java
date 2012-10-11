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

package framework;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import java.util.List;

public class CrudExamples
{
    public static void      create(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        client.create().forPath(path, payload);
    }

    public static void      createEphemeral(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
    }

    public static String    createEphemeralSequential(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        return client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, payload);
    }

    public static void      setData(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        client.setData().forPath(path, payload);
    }

    public static void      setDataAsync(CuratorFramework client, String path, byte[] payload) throws Exception
    {
        client.setData().inBackground().forPath(path, payload);
    }

    public static void      setDataAsyncWithCallback(CuratorFramework client, BackgroundCallback callback, String path, byte[] payload) throws Exception
    {
        client.setData().inBackground(callback).forPath(path, payload);
    }

    public static void      delete(CuratorFramework client, String path) throws Exception
    {
        client.delete().forPath(path);
    }

    public static void      guaranteedDelete(CuratorFramework client, String path) throws Exception
    {
        client.delete().guaranteed().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client, String path) throws Exception
    {
        return client.getChildren().watched().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client, String path, Watcher watcher) throws Exception
    {
        return client.getChildren().usingWatcher(watcher).forPath(path);
    }
}
