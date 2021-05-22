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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.dispatch.KafkaSubscriberBootstrap;
import com.wl4g.kafkasubscriber.sink.ShardingSubscriberCoordinator;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import com.wl4g.kafkasubscriber.filter.DefaultRecordMatchSubscribeFilter;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.sink.DefaultPrintSubscribeSink;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    public SubscribeFacade defaultSubscriberFacade(KafkaSubscriberProperties config) {
        return new SubscribeFacade() {
            @Override
            public List<SubscriberInfo> findSubscribers(SubscriberInfo query) {
                return config.getSubscribers();
            }

            @Override
            public boolean matchSubscriberRecord(SubscriberInfo subscriber, ConsumerRecord<String, ObjectNode> record) {
                // Notice: By default, the $subscriberId field of the source message match, which should be customized
                // to match the subscriber relationship corresponding to each record in the source consume topic.
                return record.value().get("$subscriberId").asLong(-1L) == subscriber.getId();
            }
        };
    }

    @Bean
    public KafkaSubscriberBootstrap kafkaSubscribeManager(ApplicationContext context,
                                                          KafkaSubscriberProperties config,
                                                          SubscribeFacade facade,
                                                          SubscriberRegistry registry) {
        return new KafkaSubscriberBootstrap(context, config, facade, registry);
    }

    @Bean
    public SubscribeMeter subscriberMeter(ApplicationContext context,
                                          PrometheusMeterRegistry meterRegistry,
                                          @Value("${spring.application.name}") String appName,
                                          @Value("${server.port}") Integer port) {
        return new SubscribeMeter(context, meterRegistry, appName, port);
    }

    @Bean
    public SubscriberRegistry subscriberRegistry(KafkaSubscriberProperties config) {
        return new SubscriberRegistry(config);
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
    @ConditionalOnMissingBean
    public ShardingSubscriberCoordinator shardingSubscriberCoordinator() {
        return new ShardingSubscriberCoordinator() {
        };
    }

}





