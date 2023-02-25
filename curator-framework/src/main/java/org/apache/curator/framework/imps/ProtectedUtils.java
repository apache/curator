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

package org.apache.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.api.CreateBuilderMain;
import org.apache.curator.utils.ZKPaths;
import java.util.Optional;
import java.util.UUID;

/**
 * Utility class to handle ZNode names when using {@link CreateBuilderMain#withProtection()}
 */
public final class ProtectedUtils
{

    private ProtectedUtils()
    {
        throw new RuntimeException("Protected Utils is a helper class");
    }

    /**
     * First 3 characters in the prefix on ZNode names when using {@link CreateBuilderMain#withProtection()}
     */
    @VisibleForTesting
    static final String PROTECTED_PREFIX = "_c_";

    /**
     * Last character used in the prefix on ZNode names when using {@link CreateBuilderMain#withProtection()}
     */
    @VisibleForTesting
    static final char PROTECTED_SEPARATOR = '-';

    /**
     * Prefix length on ZNode name when using {@link CreateBuilderMain#withProtection()}
     */
    @VisibleForTesting
    static final int PROTECTED_PREFIX_WITH_UUID_LENGTH = PROTECTED_PREFIX.length() + 36 // UUID canonical text representation produced by {@link UUID#toString()}
        + 1; // Separator length

    /**
     * Provides a prefix to be prepended to a ZNode name when protected. The method assumes that the provided string
     * adjusts to the required format
     *
     * @param protectedId canonical text representation of a UUID
     * @return string that concatenates {@value #PROTECTED_PREFIX}, the given id and {@value #PROTECTED_SEPARATOR}
     */
    public static String getProtectedPrefix(final String protectedId)
    {
        return PROTECTED_PREFIX + protectedId + PROTECTED_SEPARATOR;
    }

    /** Extracts protectedId assuming provided name has a valid protected format */
    private static String extractProtectedIdInternal(final String znodeName)
    {
        return znodeName.substring(PROTECTED_PREFIX.length(), PROTECTED_PREFIX_WITH_UUID_LENGTH - 1);
    }

    /**
     * Utility method to determine if a given ZNode name starts with Curator's generated protected prefix.
     *
     * @param znodeName ZNode name
     * @return {@code true} if the given ZNode name starts with Curator's generated protected prefix
     */
    public static boolean isProtectedZNode(final String znodeName)
    {
        if ( znodeName.length() > PROTECTED_PREFIX_WITH_UUID_LENGTH && znodeName.startsWith(PROTECTED_PREFIX) && znodeName.charAt(PROTECTED_PREFIX_WITH_UUID_LENGTH - 1) == PROTECTED_SEPARATOR )
        {
            try
            {
                //noinspection ResultOfMethodCallIgnored
                UUID.fromString(extractProtectedIdInternal(znodeName));
                return true;
            }
            catch ( IllegalArgumentException e )
            {
                // Not an UUID
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
    public static String normalize(final String znodeName)
    {
        return isProtectedZNode(znodeName) ? znodeName.substring(PROTECTED_PREFIX_WITH_UUID_LENGTH) : znodeName;
    }

    /**
     * Utility method to provide a path removing Curator's generated protected prefix if present in the ZNode name
     *
     * @param path ZNode path
     * @return string without Curator's generated protected prefix if present in ZNode name; original string if prefix not present
     */
    public static String normalizePath(final String path)
    {
        final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        final String name = pathAndNode.getNode();
        return isProtectedZNode(name) ? ZKPaths.makePath(pathAndNode.getPath(), normalize(name)) : path;
    }

    /**
     * Extracts protectedId in case the provided name is protected
     *
     * @param znodeName name of the ZNode
     * @return Optional with protectedId if the name is protected or {@code Optional#empty()}
     */
    public static Optional<String> extractProtectedId(final String znodeName)
    {
        return Optional.ofNullable(isProtectedZNode(znodeName) ? extractProtectedIdInternal(znodeName) : null);
    }

    /**
     * Converts a given ZNode name to protected format
     *
     * @param znodeName name to be converted (e.g. 'name1')
     * @param protectedId UUID canonical text representation used in protection mode (e.g. '53345f98-9423-4e0c-a7b5-9f819e3ec2e1') 
     * @return name with protected mode prefix (e.g. '_c_53345f98-9423-4e0c-a7b5-9f819e3ec2e1-name1')
     *         or the same name if protectedId is {@code null}
     */
    public static String toProtectedZNode(final String znodeName, final String protectedId)
    {
        return (protectedId == null) ? znodeName : getProtectedPrefix(protectedId) + znodeName;
    }

    /**
     * Converts a given path to protected format
     *
     * @param path complete path to be converted (e.g. '/root/path1')
     * @param protectedId UUID canonical text representation used in protection mode (e.g. '53345f98-9423-4e0c-a7b5-9f819e3ec2e1') 
     * @return path with protected mode prefix (e.g. '/root/_c_53345f98-9423-4e0c-a7b5-9f819e3ec2e1-path1')
     *         or the same path if protectedId is {@code null}
     */
    public static String toProtectedZNodePath(final String path, final String protectedId)
    {
        if ( protectedId == null )
        {
            return path;
        }
        final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        return ZKPaths.makePath(pathAndNode.getPath(), toProtectedZNode(pathAndNode.getNode(), protectedId));
    }
}
