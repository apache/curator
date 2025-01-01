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

import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.listen.Listenable;

class NamespaceFacade extends DelegatingCuratorFramework {
    private final NamespaceImpl namespace;
    private final FailedDeleteManager failedDeleteManager = new FailedDeleteManager(this);

    NamespaceFacade(CuratorFrameworkImpl client, String namespace) {
        super(client);
        this.namespace = new NamespaceImpl(client, namespace);
    }

    @Override
    NamespaceImpl getNamespaceImpl() {
        return namespace;
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable() {
        throw new UnsupportedOperationException(
                "getCuratorListenable() is only available from a non-namespaced CuratorFramework instance");
    }

    @Override
    FailedDeleteManager getFailedDeleteManager() {
        return failedDeleteManager;
    }
}
