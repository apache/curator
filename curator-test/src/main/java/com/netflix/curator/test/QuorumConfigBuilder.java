package com.netflix.curator.test;

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
