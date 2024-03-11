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

/**
 * Describe feature supports based on server compatibility (as opposed to
 * {@code Compatibility} which represents client compatibility.
 */
public class ZookeeperCompatibility {
    /**
     * Represent latest version with all features enabled
     */
    public static final ZookeeperCompatibility LATEST =
            builder().hasPersistentWatchers(true).build();

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        // List of features introduced by Zookeeper over time.
        // All values are set to false by default for backward compatibility
        private boolean hasPersistentWatchers = false;

        public Builder hasPersistentWatchers(boolean value) {
            this.hasPersistentWatchers = value;
            return this;
        }

        public boolean hasPersistentWatchers() {
            return this.hasPersistentWatchers;
        }

        public ZookeeperCompatibility build() {
            return new ZookeeperCompatibility(this);
        }
    }

    private final boolean hasPersistentWatchers;

    private ZookeeperCompatibility(Builder builder) {
        this.hasPersistentWatchers = builder.hasPersistentWatchers;
    }

    /**
     * Check if both client and server support persistent watchers
     *
     * @return {@code true} if both the client library and the server version
     *         support persistent watchers
     */
    public boolean hasPersistentWatchers() {
        return this.hasPersistentWatchers && Compatibility.hasPersistentWatchers();
    }
}
