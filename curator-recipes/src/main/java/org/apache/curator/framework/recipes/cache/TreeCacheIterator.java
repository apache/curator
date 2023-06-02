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

package org.apache.curator.framework.recipes.cache;

import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

// depth first iterator over tree cache nodes
class TreeCacheIterator implements Iterator<ChildData> {
    private final LinkedList<Current> stack = new LinkedList<>();
    private Current current;

    private static class Current {
        final Iterator<TreeCache.TreeNode> iterator;
        TreeCache.TreeNode node;

        Current(Iterator<TreeCache.TreeNode> iterator) {
            this.iterator = iterator;
            node = iterator.next();
        }
    }

    TreeCacheIterator(TreeCache.TreeNode root) {
        current = new Current(Iterators.forArray(root));
        stack.push(current);
    }

    @Override
    public boolean hasNext() {
        return (current != null) && TreeCache.isLive(current.node.childData);
    }

    @Override
    public ChildData next() {
        if (current == null) {
            throw new NoSuchElementException();
        }

        ChildData result = current.node.childData; // result of next iteration is current node's data

        // set the next node for the next iteration (or note completion)

        do {
            setNext();
        } while ((current != null) && !TreeCache.isLive(current.node.childData));

        return result;
    }

    private void setNext() {
        if (current.node.children != null) {
            stack.push(current);
            current = new Current(current.node.children.values().iterator());
        } else
            while (true) {
                if (current.iterator.hasNext()) {
                    current.node = current.iterator.next();
                    break;
                } else if (stack.size() > 0) {
                    current = stack.pop();
                } else {
                    current = null; // done
                    break;
                }
            }
    }
}
