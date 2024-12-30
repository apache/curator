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

package org.apache.curator.framework.imps;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionCheckBuilder;
import org.apache.curator.framework.api.transaction.TransactionCreateBuilder;
import org.apache.curator.framework.api.transaction.TransactionDeleteBuilder;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.api.transaction.TransactionSetDataBuilder;

public class TransactionOpImpl implements TransactionOp {
    private final InternalCuratorFramework client;

    public TransactionOpImpl(InternalCuratorFramework client) {
        this.client = client;
    }

    @Override
    public TransactionCreateBuilder<CuratorOp> create() {
        ExtractingCuratorOp op = new ExtractingCuratorOp();
        return new CreateBuilderImpl(client).<CuratorOp>asTransactionCreateBuilder(op, op.getRecord());
    }

    @Override
    public TransactionDeleteBuilder<CuratorOp> delete() {
        ExtractingCuratorOp op = new ExtractingCuratorOp();
        return new DeleteBuilderImpl(client).<CuratorOp>asTransactionDeleteBuilder(op, op.getRecord());
    }

    @Override
    public TransactionSetDataBuilder<CuratorOp> setData() {
        ExtractingCuratorOp op = new ExtractingCuratorOp();
        return new SetDataBuilderImpl(client).<CuratorOp>asTransactionSetDataBuilder(op, op.getRecord());
    }

    @Override
    public TransactionCheckBuilder<CuratorOp> check() {
        ExtractingCuratorOp op = new ExtractingCuratorOp();
        return CuratorTransactionImpl.<CuratorOp>makeTransactionCheckBuilder(client, op, op.getRecord());
    }
}
