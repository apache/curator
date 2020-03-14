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

package org.apache.curator.framework;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.CompressionProvider;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.CuratorTempFrameworkImpl;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.framework.imps.GzipCompressionProvider;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.framework.state.ConnectionStateErrorPolicy;
import org.apache.curator.framework.state.ConnectionStateListenerManagerFactory;
import org.apache.curator.framework.state.StandardConnectionStateErrorPolicy;
import org.apache.curator.utils.DefaultZookeeperFactory;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.curator.CuratorZookeeperClient;

/**
 * Factory methods for creating framework-style clients
 */
public class CuratorFrameworkFactory
{
    private static final int DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

    private static final byte[] LOCAL_ADDRESS = getLocalAddress();

    private static final CompressionProvider DEFAULT_COMPRESSION_PROVIDER = new GzipCompressionProvider();
    private static final DefaultZookeeperFactory DEFAULT_ZOOKEEPER_FACTORY = new DefaultZookeeperFactory();
    private static final DefaultACLProvider DEFAULT_ACL_PROVIDER = new DefaultACLProvider();
    private static final long DEFAULT_INACTIVE_THRESHOLD_MS = (int)TimeUnit.MINUTES.toMillis(3);
    private static final int DEFAULT_CLOSE_WAIT_MS = (int)TimeUnit.SECONDS.toMillis(1);

    /**
     * Return a new builder that builds a CuratorFramework
     *
     * @return new builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Create a new client with default session timeout and default connection timeout
     *
     * @param connectString list of servers to connect to
     * @param retryPolicy   retry policy to use
     * @return client
     */
    public static CuratorFramework newClient(String connectString, RetryPolicy retryPolicy)
    {
        return newClient(connectString, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, retryPolicy);
    }

    /**
     * Create a new client
     *
     * @param connectString       list of servers to connect to
     * @param sessionTimeoutMs    session timeout
     * @param connectionTimeoutMs connection timeout
     * @param retryPolicy         retry policy to use
     * @return client
     */
    public static CuratorFramework newClient(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy)
    {
        return builder().
            connectString(connectString).
            sessionTimeoutMs(sessionTimeoutMs).
            connectionTimeoutMs(connectionTimeoutMs).
            retryPolicy(retryPolicy).
            build();
    }

    /**
     * Return the local address as bytes that can be used as a node payload
     *
     * @return local address bytes
     */
    public static byte[] getLocalAddress()
    {
        try
        {
            return InetAddress.getLocalHost().getHostAddress().getBytes();
        }
        catch ( UnknownHostException ignore )
        {
            // ignore
        }
        return new byte[0];
    }

    public static class Builder
    {
        private EnsembleProvider ensembleProvider;
        private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
        private int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
        private int maxCloseWaitMs = DEFAULT_CLOSE_WAIT_MS;
        private RetryPolicy retryPolicy;
        private ThreadFactory threadFactory = null;
        private String namespace;
        private List<AuthInfo> authInfos = null;
        private byte[] defaultData = LOCAL_ADDRESS;
        private CompressionProvider compressionProvider = DEFAULT_COMPRESSION_PROVIDER;
        private ZookeeperFactory zookeeperFactory = DEFAULT_ZOOKEEPER_FACTORY;
        private ACLProvider aclProvider = DEFAULT_ACL_PROVIDER;
        private boolean canBeReadOnly = false;
        private boolean useContainerParentsIfAvailable = true;
        private ConnectionStateErrorPolicy connectionStateErrorPolicy = new StandardConnectionStateErrorPolicy();
        private SchemaSet schemaSet = SchemaSet.getDefaultSchemaSet();
        private int waitForShutdownTimeoutMs = 0;
        private Executor runSafeService = null;
        private ConnectionStateListenerManagerFactory connectionStateListenerManagerFactory = ConnectionStateListenerManagerFactory.standard;
        private int simulatedSessionExpirationPercent = 100;

        /**
         * Apply the current values and build a new CuratorFramework
         *
         * @return new CuratorFramework
         */
        public CuratorFramework build()
        {
            return new CuratorFrameworkImpl(this);
        }

        /**
         * Apply the current values and build a new temporary CuratorFramework. Temporary
         * CuratorFramework instances are meant for single requests to ZooKeeper ensembles
         * over a failure prone network such as a WAN. The APIs available from {@link CuratorTempFramework}
         * are limited. Further, the connection will be closed after 3 minutes of inactivity.
         *
         * @return temp instance
         */
        public CuratorTempFramework buildTemp()
        {
            return buildTemp(DEFAULT_INACTIVE_THRESHOLD_MS, TimeUnit.MILLISECONDS);
        }

        /**
         * Apply the current values and build a new temporary CuratorFramework. Temporary
         * CuratorFramework instances are meant for single requests to ZooKeeper ensembles
         * over a failure prone network such as a WAN. The APIs available from {@link CuratorTempFramework}
         * are limited. Further, the connection will be closed after <code>inactiveThresholdMs</code> milliseconds of inactivity.
         *
         * @param inactiveThreshold number of milliseconds of inactivity to cause connection close
         * @param unit              threshold unit
         * @return temp instance
         */
        public CuratorTempFramework buildTemp(long inactiveThreshold, TimeUnit unit)
        {
            return new CuratorTempFrameworkImpl(this, unit.toMillis(inactiveThreshold));
        }

        /**
         * Add connection authorization
         *
         * Subsequent calls to this method overwrite the prior calls.
         *
         * @param scheme the scheme
         * @param auth   the auth bytes
         * @return this
         */
        public Builder authorization(String scheme, byte[] auth)
        {
            return authorization(ImmutableList.of(new AuthInfo(scheme, (auth != null) ? Arrays.copyOf(auth, auth.length) : null)));
        }

        /**
         * Add connection authorization. The supplied authInfos are appended to those added via call to
         * {@link #authorization(java.lang.String, byte[])} for backward compatibility.
         * <p/>
         * Subsequent calls to this method overwrite the prior calls.
         *
         * @param authInfos list of {@link AuthInfo} objects with scheme and auth
         * @return this
         */
        public Builder authorization(List<AuthInfo> authInfos)
        {
            this.authInfos = ImmutableList.copyOf(authInfos);
            return this;
        }

        /**
         * Set the list of servers to connect to. IMPORTANT: use either this or {@link #ensembleProvider(EnsembleProvider)}
         * but not both.
         *
         * @param connectString list of servers to connect to
         * @return this
         */
        public Builder connectString(String connectString)
        {
            ensembleProvider = new FixedEnsembleProvider(connectString);
            return this;
        }

        /**
         * Set the list ensemble provider. IMPORTANT: use either this or {@link #connectString(String)}
         * but not both.
         *
         * @param ensembleProvider the ensemble provider to use
         * @return this
         */
        public Builder ensembleProvider(EnsembleProvider ensembleProvider)
        {
            this.ensembleProvider = ensembleProvider;
            return this;
        }

        /**
         * Sets the data to use when {@link PathAndBytesable#forPath(String)} is used.
         * This is useful for debugging purposes. For example, you could set this to be the IP of the
         * client.
         *
         * @param defaultData new default data to use
         * @return this
         */
        public Builder defaultData(byte[] defaultData)
        {
            this.defaultData = (defaultData != null) ? Arrays.copyOf(defaultData, defaultData.length) : null;
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
         * @param maxCloseWaitMs time to wait during close to join background threads
         * @return this
         */
        public Builder maxCloseWaitMs(int maxCloseWaitMs)
        {
            this.maxCloseWaitMs = maxCloseWaitMs;
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

        /**
         * @param compressionProvider the compression provider
         * @return this
         */
        public Builder compressionProvider(CompressionProvider compressionProvider)
        {
            this.compressionProvider = compressionProvider;
            return this;
        }

        /**
         * @param zookeeperFactory the zookeeper factory to use
         * @return this
         */
        public Builder zookeeperFactory(ZookeeperFactory zookeeperFactory)
        {
            this.zookeeperFactory = zookeeperFactory;
            return this;
        }

        /**
         * @param aclProvider a provider for ACLs
         * @return this
         */
        public Builder aclProvider(ACLProvider aclProvider)
        {
            this.aclProvider = aclProvider;
            return this;
        }

        /**
         * @param canBeReadOnly if true, allow ZooKeeper client to enter
         *                      read only mode in case of a network partition. See
         *                      {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
         *                      for details
         * @return this
         */
        public Builder canBeReadOnly(boolean canBeReadOnly)
        {
            this.canBeReadOnly = canBeReadOnly;
            return this;
        }

        /**
         * By default, Curator uses {@link CreateBuilder#creatingParentContainersIfNeeded()}
         * if the ZK JAR supports {@link CreateMode#CONTAINER}. Call this method to turn off this behavior.
         *
         * @return this
         */
        public Builder dontUseContainerParents()
        {
            this.useContainerParentsIfAvailable = false;
            return this;
        }

        /**
         * Set the error policy to use. The default is {@link StandardConnectionStateErrorPolicy}
         *
         * @since 3.0.0
         * @param connectionStateErrorPolicy new error policy
         * @return this
         */
        public Builder connectionStateErrorPolicy(ConnectionStateErrorPolicy connectionStateErrorPolicy)
        {
            this.connectionStateErrorPolicy = connectionStateErrorPolicy;
            return this;
        }

        /**
         * Set a timeout for {@link CuratorZookeeperClient#close(int)}  }.
         * The default is 0, which means that this feature is disabled.
         *
         * @since 4.0.2
         * @param waitForShutdownTimeoutMs default timeout
         * @return this
         */
        public Builder waitForShutdownTimeoutMs(int waitForShutdownTimeoutMs)
        {
            this.waitForShutdownTimeoutMs = waitForShutdownTimeoutMs;
            return this;
        }

        /**
         * <p>
         *     Prior to 3.0.0, Curator did not try to manage session expiration
         *     other than the functionality provided by ZooKeeper itself. Starting with
         *     3.0.0, Curator has the option of attempting to monitor session expiration
         *     above what is provided by ZooKeeper. The percentage set by this method
         *     determines how and if Curator will check for session expiration.
         * </p>
         *
         * <p>
         *     The default percentage is 100.
         * </p>
         *
         * <p>
         *     If it is set to <tt>0</tt>, Curator does not do any additional checking
         *     for session expiration.
         * </p>
         *
         * <p>
         *     If a positive number is set, Curator will check for session expiration
         *     as follows: when ZooKeeper sends a Disconnect event, Curator will start a timer.
         *     If re-connection is not achieved before the elapsed time exceeds the negotiated
         *     session time multiplied by the session expiration percent, Curator will simulate
         *     a session expiration. Due to timing/network issues, it is <b>not possible</b> for
         *     a client to match the server's session timeout with complete accuracy. Thus, the need
         *     for a session expiration percentage.
         * </p>
         *
         * @param simulatedSessionExpirationPercent new simulated session expiration percentage
         * @return this
         * @since 5.0
         */
        public Builder simulatedSessionExpirationPercent(int simulatedSessionExpirationPercent) {
            Preconditions.checkArgument(
                    (simulatedSessionExpirationPercent > 0) && (simulatedSessionExpirationPercent <= 100),
                    "simulatedSessionExpirationPercent must be > 0 and <= 100");
            this.simulatedSessionExpirationPercent = simulatedSessionExpirationPercent;
            return this;
        }

        /**
         * Add an enforced schema set
         *
         * @param schemaSet the schema set
         * @return this
         * @since 3.2.0
         */
        public Builder schemaSet(SchemaSet schemaSet)
        {
            this.schemaSet = schemaSet;
            return this;
        }

        /**
         * Curator (and user) recipes will use this executor to call notifyAll
         * and other blocking calls that might normally block ZooKeeper's event thread.
         * By default, an executor is allocated internally using the provided (or default)
         * {@link #threadFactory(java.util.concurrent.ThreadFactory)}. Use this method
         * to set a custom executor.
         *
         * @param runSafeService executor to use for calls to notifyAll from Watcher callbacks etc
         * @return this
         * @since 4.1.0
         */
        public Builder runSafeService(Executor runSafeService)
        {
            this.runSafeService = runSafeService;
            return this;
        }

        /**
         * Sets the connection state listener manager factory. For example,
         * you can set {@link org.apache.curator.framework.state.ConnectionStateListenerManagerFactory#circuitBreaking(org.apache.curator.RetryPolicy)}
         *
         * @param connectionStateListenerManagerFactory manager factory to use
         * @return this
         * @since 4.2.0
         */
        public Builder connectionStateListenerManagerFactory(ConnectionStateListenerManagerFactory connectionStateListenerManagerFactory)
        {
            this.connectionStateListenerManagerFactory = Objects.requireNonNull(connectionStateListenerManagerFactory, "connectionStateListenerManagerFactory cannot be null");
            return this;
        }

        public Executor getRunSafeService()
        {
            return runSafeService;
        }

        public ACLProvider getAclProvider()
        {
            return aclProvider;
        }

        public ZookeeperFactory getZookeeperFactory()
        {
            return zookeeperFactory;
        }

        public CompressionProvider getCompressionProvider()
        {
            return compressionProvider;
        }

        public ThreadFactory getThreadFactory()
        {
            return threadFactory;
        }

        public EnsembleProvider getEnsembleProvider()
        {
            return ensembleProvider;
        }

        public int getSessionTimeoutMs()
        {
            return sessionTimeoutMs;
        }

        public int getConnectionTimeoutMs()
        {
            return connectionTimeoutMs;
        }

        public int getWaitForShutdownTimeoutMs()
        {
            return waitForShutdownTimeoutMs;
        }

        public int getMaxCloseWaitMs()
        {
            return maxCloseWaitMs;
        }

        public RetryPolicy getRetryPolicy()
        {
            return retryPolicy;
        }

        public String getNamespace()
        {
            return namespace;
        }

        public boolean useContainerParentsIfAvailable()
        {
            return useContainerParentsIfAvailable;
        }

        public ConnectionStateErrorPolicy getConnectionStateErrorPolicy()
        {
            return connectionStateErrorPolicy;
        }

        public int getSimulatedSessionExpirationPercent() {
            return simulatedSessionExpirationPercent;
        }

        public SchemaSet getSchemaSet()
        {
            return schemaSet;
        }

        @Deprecated
        public String getAuthScheme()
        {
            int qty = (authInfos != null) ? authInfos.size() : 0;
            switch ( qty )
            {
                case 0:
                {
                    return null;
                }

                case 1:
                {
                    return authInfos.get(0).scheme;
                }

                default:
                {
                    throw new IllegalStateException("More than 1 auth has been added");
                }
            }
        }

        @Deprecated
        public byte[] getAuthValue()
        {
            int qty = (authInfos != null) ? authInfos.size() : 0;
            switch ( qty )
            {
                case 0:
                {
                    return null;
                }

                case 1:
                {
                    byte[] bytes = authInfos.get(0).getAuth();
                    return (bytes != null) ? Arrays.copyOf(bytes, bytes.length) : null;
                }

                default:
                {
                    throw new IllegalStateException("More than 1 auth has been added");
                }
            }
        }

        public List<AuthInfo> getAuthInfos()
        {
            return authInfos;
        }

        public byte[] getDefaultData()
        {
            return defaultData;
        }

        public boolean canBeReadOnly()
        {
            return canBeReadOnly;
        }

        public ConnectionStateListenerManagerFactory getConnectionStateListenerManagerFactory()
        {
            return connectionStateListenerManagerFactory;
        }

        private Builder()
        {
        }
    }

    private CuratorFrameworkFactory()
    {
    }
}
