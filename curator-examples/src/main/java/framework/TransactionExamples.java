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
package framework;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import java.util.Collection;

public class TransactionExamples
{
    public static Collection<CuratorTransactionResult>      transaction(CuratorFramework client) throws Exception
    {
        // this example shows how to use ZooKeeper's transactions

        CuratorOp createOp = client.transactionOp().create().forPath("/a/path", "some data".getBytes());
        CuratorOp setDataOp = client.transactionOp().setData().forPath("/another/path", "other data".getBytes());
        CuratorOp deleteOp = client.transactionOp().delete().forPath("/yet/another/path");

        Collection<CuratorTransactionResult>    results = client.transaction().forOperations(createOp, setDataOp, deleteOp);

        for ( CuratorTransactionResult result : results )
        {
            System.out.println(result.getForPath() + " - " + result.getType());
        }

        return results;
    }
}
