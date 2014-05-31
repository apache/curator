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
package org.apache.curator.x.rpc.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
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
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class ConfigurationBuilder
{
    private final String configurationSource;

    static
    {
        LoggingFactory.bootstrap();
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public ConfigurationBuilder(String configurationSource)
    {
        this.configurationSource = configurationSource;
    }

    public Configuration build() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new LogbackModule());
        mapper.setPropertyNamingStrategy(new AnnotationSensitivePropertyNamingStrategy());
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        subtypeResolver.registerSubtypes
        (
            ConsoleAppenderFactory.class,
            FileAppenderFactory.class,
            SyslogAppenderFactory.class,
            ExponentialBackoffRetryConfiguration.class,
            RetryNTimesConfiguration.class
        );
        mapper.setSubtypeResolver(subtypeResolver);

        ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();
        ConfigurationFactoryFactory<Configuration> factoryFactory = new DefaultConfigurationFactoryFactory<Configuration>();
        ConfigurationFactory<Configuration> configurationFactory = factoryFactory.create(Configuration.class, validatorFactory.getValidator(), mapper, "curator");
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
