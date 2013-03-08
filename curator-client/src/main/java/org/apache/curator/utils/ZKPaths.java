/*
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

package org.apache.curator.utils;

import com.google.common.collect.Lists;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import java.util.Collections;
import java.util.List;

public class ZKPaths
{
    /**
     * Apply the namespace to the given path
     *
     * @param namespace namespace (can be null)
     * @param path path
     * @return adjusted path
     */
    public static String    fixForNamespace(String namespace, String path)
    {
        if ( namespace != null )
        {
            return makePath(namespace, path);
        }
        return path;
    }

    /**
     * Given a full path, return the node name. i.e. "/one/two/three" will return "three"
     * 
     * @param path the path
     * @return the node
     */
    public static String getNodeFromPath(String path)
    {
        PathUtils.validatePath(path);
        int i = path.lastIndexOf('/');
        if ( i < 0 )
        {
            return path;
        }
        if ( (i + 1) >= path.length()  )
        {
            return "";
        }
        return path.substring(i + 1);
    }

    public static class PathAndNode
    {
        private final String        path;
        private final String        node;

        public PathAndNode(String path, String node)
        {
            this.path = path;
            this.node = node;
        }

        public String getPath()
        {
            return path;
        }

        public String getNode()
        {
            return node;
        }
    }

    /**
     * Given a full path, return the node name and its path. i.e. "/one/two/three" will return {"/one/two", "three"}
     *
     * @param path the path
     * @return the node
     */
    public static PathAndNode getPathAndNode(String path)
    {
        PathUtils.validatePath(path);
        int i = path.lastIndexOf('/');
        if ( i < 0 )
        {
            return new PathAndNode(path, "");
        }
        if ( (i + 1) >= path.length()  )
        {
            return new PathAndNode("/", "");
        }
        String node = path.substring(i + 1);
        String parentPath = (i > 0) ? path.substring(0, i) : "/";
        return new PathAndNode(parentPath, node);
    }

    /**
     * Make sure all the nodes in the path are created. NOTE: Unlike File.mkdirs(), Zookeeper doesn't distinguish
     * between directories and files. So, every node in the path is created. The data for each node is an empty blob
     *
     * @param zookeeper the client
     * @param path      path to ensure
     * @throws InterruptedException thread interruption
     * @throws org.apache.zookeeper.KeeperException
     *                              Zookeeper errors
     */
    public static void mkdirs(ZooKeeper zookeeper, String path)
        throws InterruptedException, KeeperException
    {
        mkdirs(zookeeper, path, true);
    }

    /**
     * Make sure all the nodes in the path are created. NOTE: Unlike File.mkdirs(), Zookeeper doesn't distinguish
     * between directories and files. So, every node in the path is created. The data for each node is an empty blob
     *
     * @param zookeeper the client
     * @param path      path to ensure
     * @param makeLastNode if true, all nodes are created. If false, only the parent nodes are created
     * @throws InterruptedException thread interruption
     * @throws org.apache.zookeeper.KeeperException
     *                              Zookeeper errors
     */
    public static void mkdirs(ZooKeeper zookeeper, String path, boolean makeLastNode)
        throws InterruptedException, KeeperException
    {
        PathUtils.validatePath(path);

        int pos = 1; // skip first slash, root is guaranteed to exist
        do
        {
            pos = path.indexOf('/', pos + 1);

            if ( pos == -1 )
            {
                if ( makeLastNode )
                {
                    pos = path.length();
                }
                else
                {
                    break;
                }
            }

            String subPath = path.substring(0, pos);
            if ( zookeeper.exists(subPath, false) == null )
            {
                try
                {
                    zookeeper.create(subPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                catch ( KeeperException.NodeExistsException e )
                {
                    // ignore... someone else has created it since we checked
                }
            }

        }
        while ( pos < path.length() );
    }

    /**
     * Return the children of the given path sorted by sequence number
     *
     * @param zookeeper the client
     * @param path      the path
     * @return sorted list of children
     * @throws InterruptedException thread interruption
     * @throws org.apache.zookeeper.KeeperException
     *                              zookeeper errors
     */
    public static List<String> getSortedChildren(ZooKeeper zookeeper, String path)
        throws InterruptedException, KeeperException
    {
        List<String> children = zookeeper.getChildren(path, false);
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort(sortedList);
        return sortedList;
    }

    /**
     * Given a parent path and a child node, create a combined full path
     *
     * @param parent the parent
     * @param child  the child
     * @return full path
     */
    public static String makePath(String parent, String child)
    {
        StringBuilder path = new StringBuilder();

        if ( !parent.startsWith("/") )
        {
            path.append("/");
        }
        path.append(parent);
        if ( (child == null) || (child.length() == 0) )
        {
            return path.toString();
        }

        if ( !parent.endsWith("/") )
        {
            path.append("/");
        }

        if ( child.startsWith("/") )
        {
            path.append(child.substring(1));
        }
        else
        {
            path.append(child);
        }

        return path.toString();
    }

    private ZKPaths()
    {
    }
}
