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

import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.coordinator.ShardingSubscriberCoordinator;
import com.wl4g.kafkasubscriber.dispatch.CheckpointTopicManager;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineManager;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineFacade;
import com.wl4g.kafkasubscriber.filter.DefaultRecordMatchSubscribeFilter;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.sink.DefaultPrintSubscribeSink;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * The {@link KafkaSubscriberAutoConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@Configuration
public class KafkaSubscriberAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "kafka-subscriber")
    public KafkaSubscriberProperties kafkaSubscriberProperties() {
        return new KafkaSubscriberProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public SubscribeEngineCustomizer defaultSubscriberEngineCustomizer(KafkaSubscriberProperties config) {
        return new SubscribeEngineCustomizer() {
            // TODO rename sourceProvider ??
            @Override
            public List<KafkaSubscriberProperties.SourceProperties> loadSources(String pipelineName, String sourceProvider) {
                return config.getDefinitions().getSources();
            }

            @Override
            public List<SubscriberInfo> loadSubscribers(String pipelineName, SubscriberInfo query) {
                return config.getDefinitions().getSubscribers();
            }
        };
    }

    @Bean
    public SubscribeEngineFacade subscribeEngineFacade(KafkaSubscriberProperties config, SubscribeEngineManager engine) {
        return new SubscribeEngineFacade(config, engine);
    }

    @Bean
    public SubscribeEngineManager kafkaSubscribeManager(ApplicationContext context,
                                                        KafkaSubscriberProperties config,
                                                        SubscribeEngineCustomizer facade,
                                                        CachingSubscriberRegistry registry) {
        return new SubscribeEngineManager(context, config, facade, registry);
    }

    @Bean
    public SubscribeMeter subscriberMeter(ApplicationContext context,
                                          PrometheusMeterRegistry meterRegistry,
                                          @Value("${spring.application.name}") String appName,
                                          @Value("${server.port}") Integer port) {
        return new SubscribeMeter(context, meterRegistry, appName, port);
    }

    @Bean(DefaultRecordMatchSubscribeFilter.BEAN_NAME)
    @ConditionalOnMissingBean
    public ISubscribeFilter defaultRecordMatchSubscribeFilter() {
        return new DefaultRecordMatchSubscribeFilter();
    }

    @Bean(DefaultPrintSubscribeSink.BEAN_NAME)
    @ConditionalOnMissingBean
    public ISubscribeSink defaultSubscriberSink() {
        return new DefaultPrintSubscribeSink();
    }

    @Bean
    public CachingSubscriberRegistry cachingSubscriberRegistry(KafkaSubscriberProperties config,
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
    public CheckpointTopicManager filteredTopicManager(KafkaSubscriberProperties config,
                                                       SubscribeEngineCustomizer customizer,
                                                       CachingSubscriberRegistry registry) {
        return new CheckpointTopicManager(config, customizer, registry);
    }

}





