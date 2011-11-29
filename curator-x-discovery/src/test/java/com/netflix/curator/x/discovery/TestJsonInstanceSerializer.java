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

package com.netflix.curator.x.discovery;

import junit.framework.Assert;
import org.testng.annotations.Test;

public class TestJsonInstanceSerializer
{
    @Test
    public void     testBasic() throws Exception
    {
        JsonInstanceSerializer<String>  serializer = new JsonInstanceSerializer<String>(String.class);
        ServiceInstance<String>         instance = new ServiceInstance<String>("name", "id", "address", 10, 20, "payload");
        byte[]                          bytes = serializer.serialize(instance);

        ServiceInstance<String>         rhs = serializer.deserialize(bytes);
        Assert.assertEquals(instance, rhs);
        Assert.assertEquals(instance.getId(), rhs.getId());
        Assert.assertEquals(instance.getName(), rhs.getName());
        Assert.assertEquals(instance.getPayload(), rhs.getPayload());
        Assert.assertEquals(instance.getAddress(), rhs.getAddress());
        Assert.assertEquals(instance.getPort(), rhs.getPort());
        Assert.assertEquals(instance.getSslPort(), rhs.getSslPort());
    }

    @Test
    public void     testWrongPayloadType() throws Exception
    {
        JsonInstanceSerializer<String>  stringSerializer = new JsonInstanceSerializer<String>(String.class);
        JsonInstanceSerializer<Double>  doubleSerializer = new JsonInstanceSerializer<Double>(Double.class);

        byte[]                          bytes = stringSerializer.serialize(new ServiceInstance<String>("name", "id", "address", 10, 20, "payload"));
        try
        {
            doubleSerializer.deserialize(bytes);
            Assert.fail();
        }
        catch ( ClassCastException e )
        {
            // correct
        }
    }

    @Test
    public void     testNoPayload() throws Exception
    {
        JsonInstanceSerializer<Void>    serializer = new JsonInstanceSerializer<Void>(Void.class);
        ServiceInstance<Void>           instance = new ServiceInstance<Void>("name", "id", "address", 10, 20, null);
        byte[]                          bytes = serializer.serialize(instance);

        ServiceInstance<Void>           rhs = serializer.deserialize(bytes);
        Assert.assertEquals(instance, rhs);
        Assert.assertEquals(instance.getId(), rhs.getId());
        Assert.assertEquals(instance.getName(), rhs.getName());
        Assert.assertEquals(instance.getPayload(), rhs.getPayload());
        Assert.assertEquals(instance.getAddress(), rhs.getAddress());
        Assert.assertEquals(instance.getPort(), rhs.getPort());
        Assert.assertEquals(instance.getSslPort(), rhs.getSslPort());
    }
}
