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

package com.netflix.curator.framework.recipes.locks;

import com.netflix.curator.framework.CuratorFramework;

/**
 * Same functionality as {@link InterProcessSemaphore}. However, instead of the number
 * of leases being a convention, the number is stored in a ZNode. Thus, this version of InterProcessSemaphore
 * provides some assurance that all clients use the same number of leases. Of course, there is a performance
 * impact of reading the number of leases. This only occurs when an instance of this class is allocated.
 */
public class CoopInterProcessSemaphore extends InterProcessSemaphore
{
    private final String leaseStorePath;

    /**
     * @param client client
     * @param lockPath the path to lock
     * @param leaseStorePath path to store number of leases in
     * @param defaultNumberOfLeases the seed number of leases allowed by this semaphore. If the number of leases has not been written yet, this value is used
     * @throws Exception errors reading/writing the number of leases
     */
    public CoopInterProcessSemaphore(CuratorFramework client, String lockPath, String leaseStorePath, int defaultNumberOfLeases) throws Exception
    {
        this(client, lockPath, leaseStorePath, defaultNumberOfLeases, null);
    }

    /**
     * @param client client
     * @param lockPath the path to lock
     * @param leaseStorePath path to store number of leases in
     * @param defaultNumberOfLeases the seed number of leases allowed by this semaphore. If the number of leases has not been written yet, this value is used
     * @param clientClosingListener if not null, will get called if client connection unexpectedly closes
     * @throws Exception errors reading/writing the number of leases
     */
    public CoopInterProcessSemaphore(CuratorFramework client, String lockPath, String leaseStorePath, int defaultNumberOfLeases, ClientClosingListener<InterProcessSemaphore> clientClosingListener) throws Exception
    {
        super(client, lockPath, defaultNumberOfLeases, clientClosingListener);
        this.leaseStorePath = leaseStorePath;
        initLeaseStorePath(leaseStorePath);
    }

    /**
     * Change the stored value of number of leases. This instance will updated immediately, other
     * instances will update as their watchers are notified.
     *
     * @param newNumberOfLeases the new number of leases to use
     * @throws Exception errors reading/writing the number of leases
     */
    public void changeNumberOfLeases(int newNumberOfLeases) throws Exception
    {
        changeNumberOfLeases(newNumberOfLeases, leaseStorePath);
    }
}
