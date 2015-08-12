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
package org.apache.curator.framework.imps;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TypeAndPath;
import org.apache.zookeeper.Op;

class ExtractingCuratorOp implements CuratorOp
{
    private final CuratorMultiTransactionRecord record = new CuratorMultiTransactionRecord();

    CuratorMultiTransactionRecord getRecord()
    {
        return record;
    }

    @Override
    public TypeAndPath getTypeAndPath()
    {
        validate();
        return record.getMetadata(0);
    }

    @Override
    public Op get()
    {
        validate();
        return record.iterator().next();
    }

    private void validate()
    {
        Preconditions.checkArgument(record.size() > 0, "No operation has been added");
        Preconditions.checkArgument(record.size() == 1, "Multiple operations added");
    }
}
