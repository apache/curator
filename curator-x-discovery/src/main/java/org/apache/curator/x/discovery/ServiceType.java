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
package org.apache.curator.x.discovery;

/**
 * The type of the service registration.
 *
 * STATIC and PERMANENT registrations are created when the x-discovery-server rest service registers another service.
 * Where as DYNAMIC registrations are typically used for everything else
 * (including the definition of the x-discovery-server rest service itself).
 */
public enum ServiceType
{
    /**
     * DYNAMIC registrations (default) are tied to the lifecycle of the creating process.
     *
     * ServiceDiscoveryImpl.internalRegisterService() maps DYNAMIC registrations to EPHEMERAL nodes as a result they
     * don't need to be manually cleaned up.
     */
    DYNAMIC,

    /**
     * STATIC registrations require the caller to regularly re-register the service before a timeout expires.
     *
     * The timeout is not defined in the curator code (the caller must define the timeout).
     *
     * However InstanceCleanup.checkService implements the timeout process to expire them after the timeout.
     * Even though STATIC registrations are mapped to PERSISTENT zookeeper nodes, they are also cleaned up
     * during a clean shutdown of the x-discovery-server rest service ServiceDiscoveryImpl.close().
     */
    STATIC,

    /**
     * PERMANENT registrations are not tied to the existence of any particular process.
     *
     * They can be (un)registered through the x-discovery-server rest service, however the process immediately discards
     * all knowledge of the registration ServiceDiscoveryImpl.registerService(...)
     */
    PERMANENT
}
