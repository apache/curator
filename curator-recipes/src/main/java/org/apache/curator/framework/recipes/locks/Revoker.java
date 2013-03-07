/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.curator.framework.recipes.locks;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

public class Revoker
{
    /**
     * Utility to mark a lock for revocation. Assuming that the lock has been registered with
     * a {@link RevocationListener}, it will get called and the lock should be released. Note,
     * however, that revocation is cooperative.
     *
     * @param client the client
     * @param path the path of the lock - usually from something like
     * {@link InterProcessMutex#getParticipantNodes()}
     * @throws Exception errors
     */
    public static void  attemptRevoke(CuratorFramework client, String path) throws Exception
    {
        try
        {
            client.setData().forPath(path, LockInternals.REVOKE_MESSAGE);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
    }

    private Revoker()
    {
    }
}
