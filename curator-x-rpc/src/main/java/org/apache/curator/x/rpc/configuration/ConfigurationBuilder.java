package org.apache.curator.x.rpc.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationFactoryFactory;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.DefaultConfigurationFactoryFactory;
import io.dropwizard.jackson.AnnotationSensitivePropertyNamingStrategy;
import io.dropwizard.jackson.LogbackModule;
import io.dropwizard.logging.ConsoleAppenderFactory;
import io.dropwizard.logging.FileAppenderFactory;
import io.dropwizard.logging.LoggingFactory;
import io.dropwizard.logging.SyslogAppenderFactory;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

class ConfigurationBuilder
{
    private final String configurationSource;

    static
    {
        LoggingFactory.bootstrap();
    }

    ConfigurationBuilder(String configurationSource)
    {
        this.configurationSource = configurationSource;
    }

    ConfigurationX build() throws IOException, ConfigurationException
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new LogbackModule());
        mapper.setPropertyNamingStrategy(new AnnotationSensitivePropertyNamingStrategy());
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        subtypeResolver.registerSubtypes(ConsoleAppenderFactory.class, FileAppenderFactory.class, SyslogAppenderFactory.class);
        mapper.setSubtypeResolver(subtypeResolver);

        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        ConfigurationFactoryFactory<ConfigurationX> factoryFactory = new DefaultConfigurationFactoryFactory<ConfigurationX>();
        ConfigurationFactory<ConfigurationX> configurationFactory = factoryFactory.create(ConfigurationX.class, validatorFactory.getValidator(), mapper, "curator");
        ConfigurationSourceProvider provider = new ConfigurationSourceProvider()
        {
            @Override
            public InputStream open(String path) throws IOException
            {
                return new ByteArrayInputStream(configurationSource.getBytes(Charset.defaultCharset()));
            }
        };
        return configurationFactory.build(provider, "");
    }
}
