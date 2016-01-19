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

package org.apache.curator.framework.recipes.nodes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

/**
 * <p>
 * A persistent ephemeral node is an ephemeral node that attempts to stay present in
 * ZooKeeper, even through connection and session interruptions.
 * </p>
 * <p>
 * Thanks to bbeck (https://github.com/bbeck) for the initial coding and design
 * </p>
 *
 * @deprecated This has been replaced with the more general {@link PersistentNode}
 */
@Deprecated
public class PersistentEphemeralNode extends PersistentNode
{
    /**
     * The mode for node creation
     *
     * @deprecated This has been replaced with the more general {@link PersistentNode}
     */
    @Deprecated
    public enum Mode
    {
        /**
         * Same as {@link CreateMode#EPHEMERAL}
         */
        EPHEMERAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return CreateMode.EPHEMERAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return false;
                }
            },

        /**
         * Same as {@link CreateMode#EPHEMERAL_SEQUENTIAL}
         */
        EPHEMERAL_SEQUENTIAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return pathIsSet ? CreateMode.EPHEMERAL : CreateMode.EPHEMERAL_SEQUENTIAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return false;
                }
            },

        /**
         * Same as {@link CreateMode#EPHEMERAL} with protection
         */
        PROTECTED_EPHEMERAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return CreateMode.EPHEMERAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return true;
                }
            },

        /**
         * Same as {@link CreateMode#EPHEMERAL_SEQUENTIAL} with protection
         */
        PROTECTED_EPHEMERAL_SEQUENTIAL()
            {
                @Override
                protected CreateMode getCreateMode(boolean pathIsSet)
                {
                    return pathIsSet ? CreateMode.EPHEMERAL : CreateMode.EPHEMERAL_SEQUENTIAL;
                }

                @Override
                protected boolean isProtected()
                {
                    return true;
                }
            };

        protected abstract CreateMode getCreateMode(boolean pathIsSet);

        protected abstract boolean isProtected();
    }

    /**
     * @param client   client instance
     * @param mode     creation/protection mode
     * @param basePath the base path for the node
     * @param initData     data for the node
     */
    @SuppressWarnings("deprecation")
    public PersistentEphemeralNode(CuratorFramework client, Mode mode, String basePath, byte[] initData)
    {
        super(client, mode.getCreateMode(false), mode.isProtected(), basePath, initData);
    }
}
