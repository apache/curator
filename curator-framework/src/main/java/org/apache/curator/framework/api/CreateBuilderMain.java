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
package org.apache.curator.framework.api;

import java.util.UUID;

import org.apache.zookeeper.CreateMode;

public interface CreateBuilderMain extends
    BackgroundPathAndBytesable<String>,
    CreateModable<ACLBackgroundPathAndBytesable<String>>,
    ACLCreateModeBackgroundPathAndBytesable<String>,
    Compressible<CreateBackgroundModeStatACLable>,
    Statable<CreateProtectACLCreateModePathAndBytesable<String>>
{

    /**
     * First 3 characters in the prefix on ZNode names when using {@link #withProtection()}
     */
    static final String PROTECTED_PREFIX = "_c_";

    /**
     * Last character used in the prefix on ZNode names when using {@link #withProtection()}
     */
    static final char PROTECTED_SEPARATOR = '-';

    /**
     * Prefix length on ZNode name when using {@link #withProtection()}
     */
    static final int PROTECTED_PREFIX_WITH_UUID_LENGTH = 
            PROTECTED_PREFIX.length() 
            + 36 // UUID canonical text representation produced by {@link UUID#toString()}
            + 1; // Separator length

    /**
     * Utility method to determine if a given ZNode name starts with Curator's generated protected prefix.
     * 
     * @param znodeName ZNode name
     * @return {@code true} if the given ZNode name starts with Curator's generated protected prefix
     */
    public static boolean isProtectedZNode(final String znodeName) {
        if (znodeName.length() > PROTECTED_PREFIX_WITH_UUID_LENGTH
                && znodeName.startsWith(PROTECTED_PREFIX)
                && znodeName.charAt(PROTECTED_PREFIX_WITH_UUID_LENGTH-1) == PROTECTED_SEPARATOR
           ) {
            try {
                UUID.fromString(znodeName.substring(PROTECTED_PREFIX.length(), PROTECTED_PREFIX_WITH_UUID_LENGTH-2));
                return true;
            } catch (IllegalArgumentException e) {
                // Just false
            }
        }
        return false;
    }

    /**
     * Utility method to remove Curator's generated protected prefix if present
     * 
     * @param znodeName ZNode name
     * @return string without Curator's generated protected prefix if present; original string if prefix not present
     */
    public static String removeProtectedPrefix(final String znodeName) {
        return isProtectedZNode(znodeName) ? znodeName.substring(PROTECTED_PREFIX_WITH_UUID_LENGTH) : znodeName;
    }

    /**
     * Causes any parent nodes to get created if they haven't already been
     *
     * @return this
     */
    public ProtectACLCreateModeStatPathAndBytesable<String> creatingParentsIfNeeded();

    /**
     * Causes any parent nodes to get created using {@link CreateMode#CONTAINER} if they haven't already been.
     * IMPORTANT NOTE: container creation is a new feature in recent versions of ZooKeeper.
     * If the ZooKeeper version you're using does not support containers, the parent nodes
     * are created as ordinary PERSISTENT nodes.
     *
     * @return this
     */
    public ProtectACLCreateModeStatPathAndBytesable<String> creatingParentContainersIfNeeded();

    /**
     * @deprecated this has been generalized to support all create modes. Instead, use:
     * <pre>
     *     client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)...
     * </pre>
     * @return this
     */
    @Deprecated
    public ACLPathAndBytesable<String>              withProtectedEphemeralSequential();

    /**
     * <p>
     *     Hat-tip to https://github.com/sbridges for pointing this out
     * </p>
     *
     * <p>
     *     It turns out there is an edge case that exists when creating sequential-ephemeral
     *     nodes. The creation can succeed on the server, but the server can crash before
     *     the created node name is returned to the client. However, the ZK session is still
     *     valid so the ephemeral node is not deleted. Thus, there is no way for the client to
     *     determine what node was created for them.
     * </p>
     *
     * <p>
     *     Even without sequential-ephemeral, however, the create can succeed on the sever
     *     but the client (for various reasons) will not know it.
     * </p>
     *
     * <p>
     *     Putting the create builder into protection mode works around this.
     *     The name of the node that is created is prefixed with a 40 characters string that is the concatenation of
     *     <ul>
     *         <li>{@value #PROTECTED_PREFIX}
     *         <li>Canonical text representation of a random generated UUID as produced by {@link UUID#toString()}
     *         <li>{@value #PROTECTED_SEPARATOR}
     *     </ul>
     *     If node creation fails the normal retry mechanism will occur. On the retry, the parent path is first searched
     *     for a node that has previous described prefix in it. If that node is found, it is assumed to be the lost
     *     node that was successfully created on the first try and is returned to the caller.
     * </p>
     *
     * @return this
     */
    public ACLCreateModeStatBackgroundPathAndBytesable<String>    withProtection();
}
