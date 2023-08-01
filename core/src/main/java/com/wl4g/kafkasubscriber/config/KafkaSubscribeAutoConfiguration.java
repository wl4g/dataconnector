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
import com.wl4g.kafkasubscriber.coordinator.ShardingSubscriberCoordinator;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.dispatch.CheckpointTopicManager;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineManager;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineFacade;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * The {@link KafkaSubscribeAutoConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@Configuration
public class KafkaSubscribeAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "kafka-subscriber")
    public KafkaSubscribeConfiguration kafkaSubscriberConfiguration(ApplicationContext context) {
        return new KafkaSubscribeConfiguration(context);
    }

    @Bean
    @ConditionalOnMissingBean
    public SubscribeEngineCustomizer defaultSubscribeEngineCustomizer(KafkaSubscribeConfiguration config) {
        return new SubscribeEngineCustomizer() {
            @Override
            public List<SubscriberInfo> loadSubscribers(String pipelineName, SubscriberInfo query) {
                return config.getDefinitions().getSubscribers();
            }
        };
    }

    @Bean
    public SubscribeEngineFacade subscribeEngineFacade(KafkaSubscribeConfiguration config, SubscribeEngineManager engine) {
        return new SubscribeEngineFacade(config, engine);
    }

    @Bean
    public SubscribeEngineManager kafkaSubscribeManager(ApplicationContext context,
                                                        KafkaSubscribeConfiguration config,
                                                        SubscribeEngineCustomizer customizer,
                                                        CachingSubscriberRegistry registry) {
        return new SubscribeEngineManager(context, config, customizer, registry);
    }

    @Bean
    public SubscribeMeter subscriberMeter(ApplicationContext context,
                                          PrometheusMeterRegistry meterRegistry,
                                          @Value("${spring.application.name}") String appName,
                                          @Value("${server.port}") Integer port) {
        return new SubscribeMeter(context, meterRegistry, appName, port);
    }

    @Bean
    public CachingSubscriberRegistry cachingSubscriberRegistry(KafkaSubscribeConfiguration config,
                                                               SubscribeEngineCustomizer facade) {
        return new CachingSubscriberRegistry(config, facade);
    }

    @Bean
    @ConditionalOnMissingBean
    public ShardingSubscriberCoordinator shardingSubscriberCoordinator() {
        return new ShardingSubscriberCoordinator() {
        };
    }

    @Bean
    public CheckpointTopicManager filteredTopicManager(KafkaSubscribeConfiguration config,
                                                       SubscribeEngineCustomizer customizer,
                                                       CachingSubscriberRegistry registry) {
        return new CheckpointTopicManager(config, customizer, registry);
    }

}

