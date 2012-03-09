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

package com.netflix.curator.ensemble.exhibitor;

import com.google.common.collect.ImmutableList;
import java.util.Collection;

/**
 * POJO for specifying the cluster of Exhibitor instances
 */
public class Exhibitors
{
    private final Collection<String> hostnames;
    private final int restPort;
    private final String backupConnectionString;

    /**
     * @param hostnames set of Exhibitor instance host names
     * @param restPort the REST port used to connect to Exhibitor
     */
    public Exhibitors(Collection<String> hostnames, int restPort)
    {
        this(hostnames, restPort, null);
    }

    /**
     * @param hostnames set of Exhibitor instance host names
     * @param restPort the REST port used to connect to Exhibitor
     * @param backupConnectionString in case an Exhibitor instance can't be contacted, the fixed
     *                               connection string to use as a backup
     */
    public Exhibitors(Collection<String> hostnames, int restPort, String backupConnectionString)
    {
        this.backupConnectionString = backupConnectionString;
        this.hostnames = ImmutableList.copyOf(hostnames);
        this.restPort = restPort;
    }

    public Collection<String> getHostnames()
    {
        return hostnames;
    }

    public int getRestPort()
    {
        return restPort;
    }

    public String getBackupConnectionString()
    {
        return backupConnectionString;
    }
}
