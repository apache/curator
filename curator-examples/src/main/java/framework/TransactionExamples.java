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

package framework;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.transaction.CuratorTransactionResult;
import java.util.Collection;

public class TransactionExamples
{
    public static Collection<CuratorTransactionResult>      transaction(CuratorFramework client) throws Exception
    {
        // this example shows how to use ZooKeeper's new transactions

        Collection<CuratorTransactionResult>    results = client.inTransaction()
                .create().forPath("/a/path", "some data".getBytes())
            .and()
                .setData().forPath("/another/path", "other data".getBytes())
            .and()
                .delete().forPath("/yet/another/path")
            .and()
                .commit();  // IMPORTANT! The transaction is not submitted until commit() is called

        for ( CuratorTransactionResult result : results )
        {
            System.out.println(result.getForPath() + " - " + result.getType());
        }

        return results;
    }
}
