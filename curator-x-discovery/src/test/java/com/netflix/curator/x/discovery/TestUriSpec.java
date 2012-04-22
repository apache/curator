package com.netflix.curator.x.discovery;

import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.util.Iterator;
import java.util.Map;

public class TestUriSpec
{
    @Test
    public void     testScheme()
    {
        UriSpec                             spec = new UriSpec("{scheme}://foo.com");

        ServiceInstanceBuilder<Void>        builder = new ServiceInstanceBuilder<Void>();
        builder.id("x");
        builder.name("foo");
        builder.port(5);
        ServiceInstance<Void>               instance = builder.build();
        Assert.assertEquals(spec.build(instance), "http://foo.com");

        builder.sslPort(5);
        instance = builder.build();
        Assert.assertEquals(spec.build(instance), "https://foo.com");
    }

    @Test
    public void     testFromInstance()
    {
        ServiceInstanceBuilder<Void>        builder = new ServiceInstanceBuilder<Void>();
        builder.address("1.2.3.4");
        builder.name("foo");
        builder.id("bar");
        builder.port(5);
        builder.sslPort(6);
        builder.registrationTimeUTC(789);
        builder.serviceType(ServiceType.PERMANENT);
        ServiceInstance<Void>               instance = builder.build();

        UriSpec                             spec = new UriSpec("{scheme}://{address}:{port}:{ssl-port}/{name}/{id}/{registration-time-utc}/{service-type}");

        Map<String, Object>     m = Maps.newHashMap();
        m.put("scheme", "test");
        Assert.assertEquals(spec.build(instance, m), "test://1.2.3.4:5:6/foo/bar/789/permanent");
    }

    @Test
    public void     testEscapes()
    {
        UriSpec                 spec = new UriSpec("{one}two-three-{[}four{]}-five{six}");
        Iterator<UriSpec.Part>  iterator = spec.iterator();
        checkPart(iterator.next(), "one", true);
        checkPart(iterator.next(), "two-three-", false);
        checkPart(iterator.next(), "[", true);
        checkPart(iterator.next(), "four", false);
        checkPart(iterator.next(), "]", true);
        checkPart(iterator.next(), "-five", false);
        checkPart(iterator.next(), "six", true);

        Map<String, Object>     m = Maps.newHashMap();
        m.put("one", 1);
        m.put("six", 6);
        Assert.assertEquals(spec.build(m), "1two-three-{four}-five6");
    }

    @Test
    public void     testBasic()
    {
        UriSpec                 spec = new UriSpec("{one}{two}three-four-five{six}seven{eight}");
        Iterator<UriSpec.Part>  iterator = spec.iterator();
        checkPart(iterator.next(), "one", true);
        checkPart(iterator.next(), "two", true);
        checkPart(iterator.next(), "three-four-five", false);
        checkPart(iterator.next(), "six", true);
        checkPart(iterator.next(), "seven", false);
        checkPart(iterator.next(), "eight", true);
    }

    private void    checkPart(UriSpec.Part p, String value, boolean isVariable)
    {
        Assert.assertEquals(p.getValue(), value);
        Assert.assertEquals(p.isVariable(), isVariable);
    }
}
