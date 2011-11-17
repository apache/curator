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
package com.netflix.curator.framework;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.imps.CuratorFrameworkImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;

/**
 * Factory methods for creating framework-style clients
 */
public class CuratorFrameworkFactory
{
    private static final int        DEFAULT_SESSION_TIMEOUT_MS = 15 * 1000;     // TODO make configurable
    private static final int        DEFAULT_CONNECTION_TIMEOUT_MS = 10 * 1000;  // TODO make configurable

    /**
     * Return a new builder that builds a CuratorFramework
     *
     * @return new builder
     */
    public static Builder       builder()
    {
        return new Builder();
    }

    /**
     * Create a new client with default session timeout and default connection timeout
     *
     *
     * @param connectString list of servers to connect to
     * @param retryPolicy retry policy to use
     * @return client
     * @throws IOException ZK errors
     */
    public static CuratorFramework newClient(String connectString, RetryPolicy retryPolicy) throws IOException
    {
        return newClient(connectString, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, retryPolicy);
    }

    /**
     * Create a new client
     *
     *
     * @param connectString list of servers to connect to
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param retryPolicy retry policy to use
     * @return client
     * @throws IOException ZK errors
     */
    public static CuratorFramework newClient(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy) throws IOException
    {
        return builder().
            connectString(connectString).
            sessionTimeoutMs(sessionTimeoutMs).
            connectionTimeoutMs(connectionTimeoutMs).
            retryPolicy(retryPolicy).
            build();
    }

    public static class Builder
    {
        private static final ThreadFactory      defaultThreadFactory = new ThreadFactoryBuilder().setNameFormat("CuratorFramework-%d").build();

        private String          connectString;
        private int             sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
        private int             connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
        private RetryPolicy     retryPolicy;
        private ThreadFactory   threadFactory = defaultThreadFactory;
        private String          namespace;
        private String          authScheme = null;
        private byte[]          authValue = null;

        /**
         * Apply the current values and build a new CuratorFramework
         *
         * @return new CuratorFramework
         * @throws IOException errors
         */
        public CuratorFramework build() throws IOException
        {
            return new CuratorFrameworkImpl(this);
        }

        /**
         * Add connection authorization
         *
         * @param scheme the scheme
         * @param auth the auth bytes
         * @return this
         */
        public Builder  authorization(String scheme, byte auth[])
        {
            this.authScheme = scheme;
            this.authValue = (auth != null) ? Arrays.copyOf(auth, auth.length) : null;
            return this;
        }

        /**
         * @param connectString list of servers to connect to
         * @return this
         */
        public Builder connectString(String connectString)
        {
            this.connectString = connectString;
            return this;
        }

        /**
         * As ZooKeeper is a shared space, users of a given cluster should stay within
         * a pre-defined namespace. If a namespace is set here, all paths will get pre-pended
         * with the namespace
         *
         * @param namespace the namespace
         * @return this
         */
        public Builder namespace(String namespace)
        {
            this.namespace = namespace;
            return this;
        }

        /**
         * @param sessionTimeoutMs session timeout
         * @return this
         */
        public Builder sessionTimeoutMs(int sessionTimeoutMs)
        {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        /**
         * @param connectionTimeoutMs connection timeout
         * @return this
         */
        public Builder connectionTimeoutMs(int connectionTimeoutMs)
        {
            this.connectionTimeoutMs = connectionTimeoutMs;
            return this;
        }

        /**
         * @param retryPolicy retry policy to use
         * @return this
         */
        public Builder retryPolicy(RetryPolicy retryPolicy)
        {
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * @param threadFactory thread factory used to create Executor Services
         * @return this
         */
        public Builder threadFactory(ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        public ThreadFactory getThreadFactory()
        {
            return threadFactory;
        }

        public String getConnectString()
        {
            return connectString;
        }

        public int getSessionTimeoutMs()
        {
            return sessionTimeoutMs;
        }

        public int getConnectionTimeoutMs()
        {
            return connectionTimeoutMs;
        }

        public RetryPolicy getRetryPolicy()
        {
            return retryPolicy;
        }

        public String getNamespace()
        {
            return namespace;
        }

        public String getAuthScheme()
        {
            return authScheme;
        }

        public byte[] getAuthValue()
        {
            return (authValue != null) ? Arrays.copyOf(authValue, authValue.length) : null;
        }

        private Builder()
        {
        }
    }

    private CuratorFrameworkFactory()
    {
    }
}
