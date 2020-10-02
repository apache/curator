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
package org.apache.curator.framework.imps;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class NamespaceImpl
{
    private final CuratorFrameworkImpl client;
    private final String namespace;
    private final AtomicBoolean ensurePathNeeded;

    NamespaceImpl(CuratorFrameworkImpl client, String namespace)
    {
        if ( namespace != null )
        {
            try
            {
                PathUtils.validatePath("/" + namespace);
            }
            catch ( IllegalArgumentException e )
            {
                throw new IllegalArgumentException("Invalid namespace: " + namespace + ", " + e.getMessage());
            }
        }

        this.client = client;
        this.namespace = namespace;
        ensurePathNeeded = new AtomicBoolean(namespace != null);
    }

    String getNamespace()
    {
        return namespace;
    }

    String    unfixForNamespace(String path)
    {
        if ( (namespace != null) && (path != null) )
        {
            String      namespacePath = ZKPaths.makePath(namespace, null) + "/";
            if ( !namespacePath.equals("/") && path.startsWith(namespacePath) )
            {
                path = (path.length() > namespacePath.length()) ? path.substring(namespacePath.length()) : "/";
            }
        }
        return path;
    }

    String    fixForNamespace(String path, boolean isSequential)
    {
        if ( ensurePathNeeded.get() )
        {
            try
            {
                final CuratorZookeeperClient zookeeperClient = client.getZookeeperClient();
                RetryLoop.callWithRetry
                (
                    zookeeperClient,
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            ZKPaths.mkdirs(zookeeperClient.getZooKeeper(), ZKPaths.makePath("/", namespace), true, client.getAclProvider(), true);
                            return null;
                        }
                    }
                );
                ensurePathNeeded.set(false);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                client.logError("Ensure path threw exception", e);
            }
        }

        return ZKPaths.fixForNamespace(namespace, path, isSequential);
    }

    EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return new EnsurePath(fixForNamespace(path, false), client.getAclProvider());
    }
}
