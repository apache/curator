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

package org.apache.curator.framework.recipes.locks;

import static com.google.common.collect.Lists.reverse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

/**
 * <p>
 *    A container that manages multiple locks as a single entity. When {@link #acquire()} is called,
 *    all the locks are acquired, in order. If that fails, any locks that were acquired are released.
 *    Similarly, when {@link #release()} is called, all locks are released in reverse order (failures are
 *    ignored). To avoid deadlocks, participants that lock overlapping paths should list them in the same
 *    order. The time passed to {@link #acquire(long, TimeUnit)} applies to each individual lock, not
 *    to the whole set.
 * </p>
 *
 * <p>
 *    <b>Mixed read/write locks</b><br>
 *    {@link #builder(CuratorFramework)} combines {@link InterProcessReadWriteLock} read and write locks,
 *    possibly on different paths and in different modes, e.g. a read lock on a parent path and a write
 *    lock on a child path. Other lock types can be added with {@link Builder#lock(InterProcessLock)}.
 * </p>
 *
 * <p>
 *    <b>Mirroring</b><br>
 *    A lock node that is already held by another participant (typically a parent task that delegated
 *    work to this one) can be <em>mirrored</em> with {@link Builder#mirror(String, String, boolean)}
 *    instead of competing for a new lock. The node paths of a held lock are available from
 *    {@link #getLockNodePaths()} and can be handed to the child. Acquiring a mirrored lock only verifies
 *    that the node still exists. Releasing it does not delete the node, which remains owned by the
 *    original holder. The holder is responsible for keeping its lock until the participants mirroring
 *    it are done.
 * </p>
 */
public class InterProcessMultiLock implements InterProcessLock {
    private final List<InterProcessLock> locks;

    /**
     * Creates a multi lock of {@link InterProcessMutex}s
     *
     * @param client the client
     * @param paths list of paths to manage in the order that they are to be locked
     */
    public InterProcessMultiLock(CuratorFramework client, List<String> paths) {
        // paths get checked in each individual InterProcessMutex, so trust them here
        this(makeLocks(client, paths));
    }

    /**
     * Creates a multi lock of any type of inter process lock
     *
     * @param locks the locks
     */
    public InterProcessMultiLock(List<InterProcessLock> locks) {
        this.locks = ImmutableList.copyOf(locks);
    }

    private static List<InterProcessLock> makeLocks(CuratorFramework client, List<String> paths) {
        ImmutableList.Builder<InterProcessLock> builder = ImmutableList.builder();
        for (String path : paths) {
            InterProcessLock lock = new InterProcessMutex(client, path);
            builder.add(lock);
        }
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acquire() throws Exception {
        acquire(-1, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception {
        Exception exception = null;
        List<InterProcessLock> acquired = Lists.newArrayList();
        boolean success = true;
        for (InterProcessLock lock : locks) {
            try {
                if (unit == null) {
                    lock.acquire();
                    acquired.add(lock);
                } else {
                    if (lock.acquire(time, unit)) {
                        acquired.add(lock);
                    } else {
                        success = false;
                        break;
                    }
                }
            } catch (Exception e) {
                ThreadUtils.checkInterrupted(e);
                success = false;
                exception = e;
                break;
            }
        }

        if (!success) {
            for (InterProcessLock lock : reverse(acquired)) {
                try {
                    lock.release();
                } catch (Exception e) {
                    ThreadUtils.checkInterrupted(e);
                    // ignore
                }
            }
        }

        if (exception != null) {
            throw exception;
        }

        return success;
    }

    /**
     * {@inheritDoc}
     *
     * <p>NOTE: locks are released in the reverse order that they were acquired.</p>
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     *
     */
    @Override
    public synchronized void release() throws Exception {
        Exception baseException = null;

        for (InterProcessLock lock : reverse(locks)) {
            try {
                lock.release();
            } catch (Exception e) {
                ThreadUtils.checkInterrupted(e);
                if (baseException == null) {
                    baseException = e;
                } else {
                    baseException = new Exception(baseException);
                }
            }
        }

        if (baseException != null) {
            throw baseException;
        }
    }

    @Override
    public synchronized boolean isAcquiredInThisProcess() {
        // it's subjective what the correct meaning is here - I choose to return true
        // only if all of the locks are acquired

        for (InterProcessLock lock : locks) {
            if (!lock.isAcquiredInThisProcess()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the {@link InterProcessLock#getLockNodePath() lock node path} of each managed lock, in
     * lock order. For thread-owned locks this is the node held by the calling thread. These paths can be
     * passed to {@link Builder#mirror(String, String, boolean)}.
     *
     * @return lock node paths, with null entries for locks that are not held or have no lock node
     */
    public List<String> getLockNodePaths() {
        List<String> paths = new ArrayList<>(locks.size());
        for (InterProcessLock lock : locks) {
            paths.add(lock.getLockNodePath());
        }
        return paths;
    }

    /**
     * @return the number of locks managed
     */
    public int size() {
        return locks.size();
    }

    /**
     * Start building a multi lock of {@link InterProcessReadWriteLock} read, write and mirrored locks,
     * and any other {@link InterProcessLock}s.
     *
     * @param client the client
     * @return builder
     */
    public static Builder builder(CuratorFramework client) {
        return new Builder(client);
    }

    public static class Builder {
        private final CuratorFramework client;
        private final List<InterProcessLock> locks = new ArrayList<>();
        private byte[] lockData = null;

        private Builder(CuratorFramework client) {
            this.client = Preconditions.checkNotNull(client, "client cannot be null");
        }

        /**
         * Data to store in the lock nodes created by subsequently added {@link #read(String)} and
         * {@link #write(String)} locks.
         *
         * @param lockData data, or null for the default
         * @return this
         */
        public Builder lockData(byte[] lockData) {
            this.lockData = lockData;
            return this;
        }

        /**
         * Add a read lock on the given path.
         *
         * @param basePath the {@link InterProcessReadWriteLock} base path
         * @return this
         */
        public Builder read(String basePath) {
            locks.add(new InterProcessReadWriteLock(client, basePath, lockData).readLock());
            return this;
        }

        /**
         * Add a write lock on the given path.
         *
         * @param basePath the {@link InterProcessReadWriteLock} base path
         * @return this
         */
        public Builder write(String basePath) {
            locks.add(new InterProcessReadWriteLock(client, basePath, lockData).writeLock());
            return this;
        }

        /**
         * Add any other lock, e.g. an {@link InterProcessMutex} or {@link InterProcessSemaphoreMutex}.
         *
         * @param lock the lock
         * @return this
         */
        public Builder lock(InterProcessLock lock) {
            locks.add(Preconditions.checkNotNull(lock, "lock cannot be null"));
            return this;
        }

        /**
         * Mirror a lock node already held by another participant, instead of acquiring a new lock.
         *
         * @param basePath the {@link InterProcessReadWriteLock} base path the node belongs to
         * @param lockNodePath full path of the held lock node, as returned by
         *                     {@link InterProcessMultiLock#getLockNodePaths()}
         * @param write true if the mirrored lock must be a write lock. A read lock can mirror either a
         *              read or a write lock node.
         * @return this
         * @throws IllegalArgumentException if the node is not directly under {@code basePath}, or a write
         *              lock is requested for a node that is not a write lock
         */
        public Builder mirror(String basePath, String lockNodePath, boolean write) {
            PathUtils.validatePath(basePath);
            PathUtils.validatePath(lockNodePath);
            ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(lockNodePath);
            Preconditions.checkArgument(
                    pathAndNode.getPath().equals(basePath),
                    "Lock node %s is not under base path %s",
                    lockNodePath,
                    basePath);
            String node = pathAndNode.getNode();
            boolean isWriteNode = node.contains(InterProcessReadWriteLock.WRITE_LOCK_NAME);
            if (write) {
                Preconditions.checkArgument(isWriteNode, "Lock node %s is not a write lock", lockNodePath);
            } else {
                Preconditions.checkArgument(
                        isWriteNode || node.contains(InterProcessReadWriteLock.READ_LOCK_NAME),
                        "Lock node %s is not a read/write lock node",
                        lockNodePath);
            }
            locks.add(new MirroredLock(client, lockNodePath));
            return this;
        }

        /**
         * @return the lock. A lock with no entries is valid and acquires immediately.
         */
        public InterProcessMultiLock build() {
            return new InterProcessMultiLock(locks);
        }
    }

    /** A lock node held by another participant, see {@link Builder#mirror(String, String, boolean)}. */
    private static class MirroredLock implements InterProcessLock {
        private final CuratorFramework client;
        private final String lockNodePath;
        private final AtomicInteger acquireCount = new AtomicInteger();

        MirroredLock(CuratorFramework client, String lockNodePath) {
            this.client = client;
            this.lockNodePath = lockNodePath;
        }

        @Override
        public void acquire() throws Exception {
            if (client.checkExists().forPath(lockNodePath) == null) {
                throw new KeeperException.NoNodeException(lockNodePath);
            }
            acquireCount.incrementAndGet();
        }

        @Override
        public boolean acquire(long time, TimeUnit unit) throws Exception {
            acquire();
            return true;
        }

        @Override
        public void release() throws Exception {
            if (acquireCount.getAndUpdate(c -> Math.max(c - 1, 0)) <= 0) {
                throw new IllegalMonitorStateException("Mirrored lock not acquired: " + lockNodePath);
            }
        }

        @Override
        public boolean isAcquiredInThisProcess() {
            return acquireCount.get() > 0;
        }

        @Override
        public String getLockNodePath() {
            return lockNodePath;
        }
    }
}
