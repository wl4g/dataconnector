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

import com.wl4g.streamconnect.coordinator.CachingSubscriberRegistry;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.coordinator.KafkaStreamConnectCoordinator;
import com.wl4g.streamconnect.coordinator.StreamConnectEventPublisher;
import com.wl4g.streamconnect.custom.DefaultStreamConnectEngineCustomizer;
import com.wl4g.streamconnect.custom.StreamConnectEngineCustomizer;
import com.wl4g.streamconnect.dispatch.CheckpointTopicManager;
import com.wl4g.streamconnect.dispatch.StreamConnectEngineScheduler;
import com.wl4g.streamconnect.facade.StreamConnectEngineFacade;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.meter.StreamConnectMeterEventHandler;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
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
    public StreamConnectConfiguration streamConnectConfiguration(StreamConnectProperties properties) {
        return new StreamConnectConfiguration(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamConnectEngineCustomizer defaultStreamConnectEngineCustomizer(StreamConnectConfiguration config) {
        return new DefaultStreamConnectEngineCustomizer(config);
    }

    @Bean
    public StreamConnectEventPublisher streamConnectEventPublisher(StreamConnectConfiguration config) {
        return new StreamConnectEventPublisher(config);
    }

    @Bean
    public StreamConnectEngineFacade streamConnectEngineFacade(StreamConnectConfiguration config,
                                                               StreamConnectEngineScheduler engineManager,
                                                               StreamConnectEventPublisher eventPublisher) {
        return new StreamConnectEngineFacade(config, engineManager, eventPublisher);
    }

    @Bean
    public StreamConnectEngineScheduler streamConnectEngineScheduler(StreamConnectConfiguration config,
                                                                     StreamConnectEngineCustomizer customizer,
                                                                     CheckpointTopicManager topicManager,
                                                                     ApplicationEventPublisher eventPublisher,
                                                                     CachingSubscriberRegistry registry) {
        return new StreamConnectEngineScheduler(config, topicManager, customizer, eventPublisher, registry);
    }

    @Bean
    public StreamConnectMeter streamConnectMeter(ApplicationContext context,
                                                 PrometheusMeterRegistry meterRegistry,
                                                 @Value("${spring.application.name}") String appName,
                                                 @Value("${server.port}") Integer port) {
        return new StreamConnectMeter(context, meterRegistry, appName, port);
    }

    @Bean
    public StreamConnectMeterEventHandler streamConnectMeterEventHandler(StreamConnectMeter meter) {
        return new StreamConnectMeterEventHandler(meter);
    }

    @Bean
    public CachingSubscriberRegistry cachingSubscriberRegistry(StreamConnectConfiguration config,
                                                               StreamConnectEngineCustomizer facade) {
        return new CachingSubscriberRegistry(config, facade);
    }

    @Bean
    @ConditionalOnMissingBean
    public IStreamConnectCoordinator kafkaStreamConnectCoordinator(Environment environment,
                                                                   StreamConnectProperties config,
                                                                   StreamConnectEngineCustomizer customizer,
                                                                   CachingSubscriberRegistry registry) {
        return new KafkaStreamConnectCoordinator(environment, config, customizer, registry);
    }

    @Bean
    public CheckpointTopicManager checkpointTopicManager(StreamConnectConfiguration config,
                                                         StreamConnectEngineCustomizer customizer,
                                                         CachingSubscriberRegistry registry) {
        return new CheckpointTopicManager(config, customizer, registry);
    }

}

