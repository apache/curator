/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package discovery;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.UriSpec;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;
import java.io.Closeable;
import java.io.IOException;

public class ExampleServer implements Closeable
{
    private final ServiceDiscovery<InstanceDetails> serviceDiscovery;
    private final ServiceInstance<InstanceDetails> thisInstance;

    public ExampleServer(CuratorFramework client, String path, String serviceName, String description) throws Exception
    {
        UriSpec     uriSpec = new UriSpec("{scheme}://foo.com:{port}");
        thisInstance = ServiceInstance.<InstanceDetails>builder()
            .name(serviceName)
            .payload(new InstanceDetails(description))
            .port((int)(65535 * Math.random()))
            .uriSpec(uriSpec)
            .build();
        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class);
        serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
            .client(client)
            .basePath(path)
            .serializer(serializer)
            .thisInstance(thisInstance)
            .build();
    }

    public ServiceInstance<InstanceDetails> getThisInstance()
    {
        return thisInstance;
    }

    public void start() throws Exception
    {
        serviceDiscovery.start();
    }

    @Override
    public void close() throws IOException
    {
        Closeables.closeQuietly(serviceDiscovery);
    }
}
