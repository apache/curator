package org.apache.curator.x.rpc.configuration;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.airlift.units.Duration;
import io.dropwizard.logging.AppenderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TestConfigurationBuilder
{
    @Test
    public void testSimple() throws Exception
    {
        ConfigurationX configuration = loadTestConfiguration("configuration/simple.json");
        Assert.assertEquals(configuration.getThrift().getPort(), 1234);
        Assert.assertEquals(configuration.getPingTime(), new Duration(10, TimeUnit.SECONDS));
    }

    @Test
    public void testLogging() throws Exception
    {
        ConfigurationX configuration = loadTestConfiguration("configuration/logging.json");
        Assert.assertEquals(configuration.getLogging().getLevel(), Level.INFO);
        Assert.assertEquals(configuration.getLogging().getAppenders().size(), 2);

        Set<String> types = Sets.newHashSet();
        for ( AppenderFactory appenderFactory : configuration.getLogging().getAppenders() )
        {
            types.add(appenderFactory.getClass().getSimpleName());
        }
        Assert.assertEquals(types, Sets.newHashSet("FileAppenderFactory", "ConsoleAppenderFactory"));
    }

    private ConfigurationX loadTestConfiguration(String name) throws Exception
    {
        URL resource = Resources.getResource(name);
        String source = Resources.toString(resource, Charset.defaultCharset());
        return new ConfigurationBuilder(source).build();
    }
}
