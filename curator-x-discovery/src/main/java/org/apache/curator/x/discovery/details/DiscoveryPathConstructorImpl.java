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

package org.apache.curator.x.discovery.details;

import com.google.common.base.Preconditions;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.DiscoveryPathConstructor;

/**
 * A standard constructor, it uses standard path constructing strategy by applying name to the base path.
 */
public class DiscoveryPathConstructorImpl implements DiscoveryPathConstructor {
    private final String basePath;

    public DiscoveryPathConstructorImpl(String basePath) {
        Preconditions.checkArgument(basePath != null, "basePath cannot be null");
        PathUtils.validatePath(basePath);
        this.basePath = basePath;
    }

    @Override
    public String getBasePath() {
        return basePath;
    }

    @Override
    public String getPathForInstances(String serviceName) {
        return ZKPaths.makePath(basePath, serviceName);
    }

    @Override
    public String getPathForInstance(String serviceName, String instanceId) {
        return ZKPaths.makePath(getPathForInstances(serviceName), instanceId);
    }
}
