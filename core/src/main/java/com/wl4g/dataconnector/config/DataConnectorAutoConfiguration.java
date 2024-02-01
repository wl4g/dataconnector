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

package com.wl4g.dataconnector.config;

import com.wl4g.dataconnector.coordinator.CachingChannelRegistry;
import com.wl4g.dataconnector.facade.DataConnectorEngineFacade;
import com.wl4g.dataconnector.meter.MeterEventHandler;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * The {@link DataConnectorAutoConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@SuppressWarnings("unused")
public class DataConnectorAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "data-connector")
    public DataConnectorProperties dataConnectorProperties() {
        return new DataConnectorProperties();
    }

    @Bean
    public DataConnectorConfiguration dataConnectorConfiguration(Environment environment,
                                                                 DataConnectorProperties properties,
                                                                 ApplicationEventPublisher eventPublisher,
                                                                 DataConnectorMeter meter) {
        return new DataConnectorConfiguration(environment, properties, eventPublisher, meter);
    }

    @Bean
    public DataConnectorEngineFacade dataConnectorEngineFacade(DataConnectorConfiguration config,
                                                               DataConnectorEngineBootstrap engine) {
        return new DataConnectorEngineFacade(config, engine);
    }

    @Bean
    public DataConnectorEngineBootstrap dataConnectorEngineBootstrap(Environment environment,
                                                                     DataConnectorConfiguration config,
                                                                     CachingChannelRegistry registry) {
        return new DataConnectorEngineBootstrap(environment, config, registry);
    }

    @Bean
    public DataConnectorMeter dataConnectorMeter(PrometheusMeterRegistry meterRegistry,
                                                 @Value("${spring.application.name}") String appName,
                                                 @Value("${server.port}") Integer port) {
        return new DataConnectorMeter(meterRegistry, appName, port);
    }

    @Bean
    public MeterEventHandler meterEventHandler(DataConnectorMeter meter) {
        return new MeterEventHandler(meter);
    }

    @Bean
    public CachingChannelRegistry cachingChannelRegistry(DataConnectorConfiguration config) {
        return new CachingChannelRegistry(config);
    }

}
