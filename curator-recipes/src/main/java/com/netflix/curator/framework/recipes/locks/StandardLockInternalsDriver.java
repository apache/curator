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
import org.apache.zookeeper.KeeperException;
import java.util.List;

class StandardLockInternalsDriver implements LockInternalsDriver
{
    @Override
    public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
    {
        int             ourIndex = children.indexOf(sequenceNodeName);
        validateOurIndex(client, sequenceNodeName, ourIndex);

        boolean         getsTheLock = ourIndex < maxLeases;
        String          pathToWatch = getsTheLock ? null : children.get(ourIndex - 1);

        return new PredicateResults(pathToWatch, getsTheLock);
    }

    @Override
    public String fixForSorting(String str, String lockName)
    {
        int index = str.lastIndexOf(lockName);
        if ( index >= 0 )
        {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    static void validateOurIndex(CuratorFramework client, String sequenceNodeName, int ourIndex) throws KeeperException.ConnectionLossException
    {
        if ( ourIndex < 0 )
        {
            client.getZookeeperClient().getLog().error("Sequential path not found: " + sequenceNodeName);
            throw new KeeperException.ConnectionLossException(); // treat it as a kind of disconnection and just try again according to the retry policy
        }
    }
}
