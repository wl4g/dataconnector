/*
 *  Copyright (C) 2023 ~ 2035 the original authors WL4G (James Wong).
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.wl4g.streamconnect.config;

import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.facade.StreamConnectEngineFacade;
import com.wl4g.streamconnect.meter.MeterEventHandler;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.StreamConnectEngineBootstrap;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * The {@link StreamConnectAutoConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
public class StreamConnectAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "stream-connect")
    public StreamConnectProperties streamConnectProperties() {
        return new StreamConnectProperties();
    }

    @Bean
    public StreamConnectConfiguration streamConnectConfiguration(Environment environment,
                                                                 StreamConnectProperties properties,
                                                                 ApplicationEventPublisher eventPublisher,
                                                                 StreamConnectMeter meter) {
        return new StreamConnectConfiguration(environment, properties, eventPublisher, meter);
    }

    @Bean
    public StreamConnectEngineFacade streamConnectEngineFacade(StreamConnectConfiguration config,
                                                               StreamConnectEngineBootstrap engineManager) {
        return new StreamConnectEngineFacade(config, engineManager);
    }

    @Bean
    public StreamConnectEngineBootstrap streamConnectEngineBootstrap(Environment environment,
                                                                     StreamConnectConfiguration config,
                                                                     CachingChannelRegistry registry) {
        return new StreamConnectEngineBootstrap(environment, config, registry);
    }

    @Bean
    public StreamConnectMeter streamConnectMeter(PrometheusMeterRegistry meterRegistry,
                                                 @Value("${spring.application.name}") String appName,
                                                 @Value("${server.port}") Integer port) {
        return new StreamConnectMeter(meterRegistry, appName, port);
    }

    @Bean
    public MeterEventHandler meterEventHandler(StreamConnectMeter meter) {
        return new MeterEventHandler(meter);
    }

    @Bean
    public CachingChannelRegistry cachingChannelRegistry(StreamConnectConfiguration config) {
        return new CachingChannelRegistry(config);
    }

}

