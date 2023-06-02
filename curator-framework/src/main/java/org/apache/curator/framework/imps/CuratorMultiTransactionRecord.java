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

import com.google.common.collect.Lists;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.framework.api.transaction.TypeAndPath;
import org.apache.zookeeper.Op;

class CuratorMultiTransactionRecord implements Iterable<Op> {
    private final List<TypeAndPath> metadata = Lists.newArrayList();
    private final List<Op> ops = new ArrayList<>();

    void add(Op op, OperationType type, String forPath) {
        ops.add(op);
        metadata.add(new TypeAndPath(type, forPath));
    }

    TypeAndPath getMetadata(int index) {
        return metadata.get(index);
    }

    int metadataSize() {
        return metadata.size();
    }

    void addToDigest(MessageDigest digest) {
        for (Op op : ops) {
            digest.update(op.getPath().getBytes());
            digest.update(Integer.toString(op.getType()).getBytes());
            digest.update(op.toRequestRecord().toString().getBytes());
        }
    }

    @Override
    public Iterator<Op> iterator() {
        return ops.iterator();
    }

    int size() {
        return ops.size();
    }
}
