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

package org.apache.curator.test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("UnusedDeclaration")
public class QuorumConfigBuilder
{
    private final ImmutableList<InstanceSpec> instanceSpecs;
    private final boolean fromRandom;

    public QuorumConfigBuilder(Collection<InstanceSpec> specs)
    {
        this(specs.toArray(new InstanceSpec[specs.size()]));
    }

    public QuorumConfigBuilder(InstanceSpec... specs)
    {
        fromRandom = (specs == null) || (specs.length == 0);
        instanceSpecs = fromRandom ? ImmutableList.of(InstanceSpec.newInstanceSpec()) : ImmutableList.copyOf(specs);
    }

    public boolean isFromRandom()
    {
        return fromRandom;
    }

    public QuorumPeerConfig buildConfig() throws Exception
    {
        return buildConfig(0);
    }

    public InstanceSpec     getInstanceSpec(int index)
    {
        return instanceSpecs.get(index);
    }

    public List<InstanceSpec> getInstanceSpecs()
    {
        return instanceSpecs;
    }

    public int  size()
    {
        return instanceSpecs.size();
    }

    public QuorumPeerConfig buildConfig(int instanceIndex) throws Exception
    {
        boolean       isCluster = (instanceSpecs.size() > 1);
        InstanceSpec  spec = instanceSpecs.get(instanceIndex);

        if ( isCluster )
        {
            Files.write(Integer.toString(spec.getServerId()).getBytes(), new File(spec.getDataDirectory(), "myid"));
        }

        Properties properties = new Properties();
        properties.setProperty("initLimit", "10");
        properties.setProperty("syncLimit", "5");
        properties.setProperty("dataDir", spec.getDataDirectory().getCanonicalPath());
        properties.setProperty("clientPort", Integer.toString(spec.getPort()));
        if ( isCluster )
        {
            for ( InstanceSpec thisSpec : instanceSpecs )
            {
                properties.setProperty("server." + thisSpec.getServerId(), String.format("localhost:%d:%d", thisSpec.getQuorumPort(), thisSpec.getElectionPort()));
            }
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(properties);
        return config;
    }
}
