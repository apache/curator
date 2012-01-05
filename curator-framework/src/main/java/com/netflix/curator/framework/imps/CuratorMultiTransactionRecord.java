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

package com.netflix.curator.framework.imps;

import com.google.common.collect.Lists;
import com.netflix.curator.framework.api.transaction.OperationType;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import java.util.List;

class CuratorMultiTransactionRecord extends MultiTransactionRecord
{
    private final List<TypeAndPath>     metadata = Lists.newArrayList();

    static class TypeAndPath
    {
        final OperationType type;
        final String forPath;

        TypeAndPath(OperationType type, String forPath)
        {
            this.type = type;
            this.forPath = forPath;
        }
    }

    @Override
    public final void add(Op op)
    {
        throw new UnsupportedOperationException();
    }
    
    void add(Op op, OperationType type, String forPath)
    {
        super.add(op);
        metadata.add(new TypeAndPath(type, forPath));
    }
    
    TypeAndPath     getMetadata(int index)
    {
        return metadata.get(index);
    }

    int             metadataSize()
    {
        return metadata.size();
    }
}
