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

import java.util.UUID;

import org.apache.curator.framework.api.CreateBuilderMain;

/**
 * Utility class to handle ZNode names when using {@link CreateBuilderMain#withProtection()}
 */
public final class ProtectedUtils {

    private ProtectedUtils() {
        throw new RuntimeException("Protected Utils is a helper class");
    }

    /**
     * First 3 characters in the prefix on ZNode names when using {@link CreateBuilderMain#withProtection()}
     */
    static final String PROTECTED_PREFIX = "_c_";

    /**
     * Last character used in the prefix on ZNode names when using {@link CreateBuilderMain#withProtection()}
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
     * Provides a prefix to be appended to a ZNode name when protected. The method assumes that the provided string
     * adjusts to the required format
     * 
     * @param protectedId canonical text representation of a UUID
     * @return string that concatenates {@value #PROTECTED_PREFIX}, the given id and {@value #PROTECTED_SEPARATOR}
     */
    static String getProtectedPrefix(final String protectedId)
    {
        return PROTECTED_PREFIX + protectedId + PROTECTED_SEPARATOR;
    }

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
    public static String normalize(final String znodeName) {
        return isProtectedZNode(znodeName) ? znodeName.substring(PROTECTED_PREFIX_WITH_UUID_LENGTH) : znodeName;
    }
}
