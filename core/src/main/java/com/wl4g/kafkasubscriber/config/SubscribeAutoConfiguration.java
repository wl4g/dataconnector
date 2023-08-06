/*
 * Copyright 2017 ~ 2025 the original authors James Wong.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g.kafkasubscriber.config;

import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.coordinator.ISubscribeCoordinator;
import com.wl4g.kafkasubscriber.coordinator.KafkaSubscribeCoordinator;
import com.wl4g.kafkasubscriber.custom.DefaultSubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.dispatch.CheckpointTopicManager;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineManager;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineFacade;
import com.wl4g.kafkasubscriber.coordinator.SubscribeEventPublisher;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.meter.SubscribeMeterEventHandler;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The {@link SubscribeAutoConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@Configuration
public class SubscribeAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "kafka-subscriber")
    public SubscribeConfiguration kafkaSubscriberConfiguration(ApplicationContext context) {
        return new SubscribeConfiguration(context);
    }

    @Bean
    @ConditionalOnMissingBean
    public SubscribeEngineCustomizer defaultSubscribeEngineCustomizer(SubscribeConfiguration config) {
        return new DefaultSubscribeEngineCustomizer(config);
    }

    @Bean
    public SubscribeEventPublisher subscribeEventPublisher(SubscribeConfiguration config) {
        return new SubscribeEventPublisher(config);
    }

    @Bean
    public SubscribeEngineFacade subscribeEngineFacade(SubscribeConfiguration config,
                                                       SubscribeEngineManager engineManager,
                                                       SubscribeEventPublisher eventPublisher) {
        return new SubscribeEngineFacade(config, engineManager, eventPublisher);
    }

    @Bean
    public SubscribeEngineManager subscribeEngineManager(SubscribeConfiguration config,
                                                         SubscribeEngineCustomizer customizer,
                                                         CheckpointTopicManager topicManager,
                                                         ApplicationEventPublisher eventPublisher,
                                                         CachingSubscriberRegistry registry) {
        return new SubscribeEngineManager(config, topicManager, customizer, eventPublisher, registry);
    }

    @Bean
    public SubscribeMeter subscriberMeter(ApplicationContext context,
                                          PrometheusMeterRegistry meterRegistry,
                                          @Value("${spring.application.name}") String appName,
                                          @Value("${server.port}") Integer port) {
        return new SubscribeMeter(context, meterRegistry, appName, port);
    }

    @Bean
    public SubscribeMeterEventHandler subscribeMeterEventHandler(SubscribeMeter meter) {
        return new SubscribeMeterEventHandler(meter);
    }

    @Bean
    public CachingSubscriberRegistry cachingSubscriberRegistry(SubscribeConfiguration config,
                                                               SubscribeEngineCustomizer facade) {
        return new CachingSubscriberRegistry(config, facade);
    }

    @Bean
    @ConditionalOnMissingBean
    public ISubscribeCoordinator kafkaSubscriberCoordinator() {
        return new KafkaSubscribeCoordinator();
    }

    @Bean
    public CheckpointTopicManager filteredTopicManager(SubscribeConfiguration config,
                                                       SubscribeEngineCustomizer customizer,
                                                       CachingSubscriberRegistry registry) {
        return new CheckpointTopicManager(config, customizer, registry);
    }

}

