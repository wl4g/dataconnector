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

package com.wl4g.kafkasubscriber.dispatch;

import com.wl4g.kafkasubscriber.config.KafkaConsumerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineCustomizer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.lang.Assert2.notNullOf;

/**
 * The {@link SubscribeEngineBootstrap}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class SubscribeEngineBootstrap implements ApplicationRunner, Closeable {

    private final ApplicationContext context;
    private final KafkaSubscriberProperties config;
    private final SubscribeEngineCustomizer facade;
    private final CachingSubscriberRegistry registry;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> filterSubscribers;
    private final Map<Long, ConcurrentMessageListenerContainer<String, String>> sinkSubscribers;

    public SubscribeEngineBootstrap(@NotNull ApplicationContext context,
                                    @NotNull KafkaSubscriberProperties config,
                                    @NotNull SubscribeEngineCustomizer facade,
                                    @NotNull CachingSubscriberRegistry registry) {
        this.context = notNullOf(context, "context");
        this.config = notNullOf(config, "config");
        this.facade = notNullOf(facade, "facade");
        this.registry = notNullOf(registry, "registry");
        this.filterSubscribers = new ConcurrentHashMap<>(config.getPipelines().size());
        this.sinkSubscribers = new ConcurrentHashMap<>(config.getPipelines().size());
    }

    public Map<String, ConcurrentMessageListenerContainer<String, String>> getFilterSubscribers() {
        return Collections.unmodifiableMap(filterSubscribers);
    }

    public Map<Long, ConcurrentMessageListenerContainer<String, String>> getSinkSubscribers() {
        return Collections.unmodifiableMap(sinkSubscribers);
    }

    @Override
    public void close() {
        filterSubscribers.forEach((groupId, subscriber) -> {
            try {
                log.info("Stopping filter subscriber group id: {}", groupId);
                subscriber.stop();
                log.info("Stopped filter subscriber group id: {}", groupId);
            } catch (Throwable ex) {
                log.error("Failed to stop filter subscriber group id: {}", groupId, ex);
            }
        });
        sinkSubscribers.forEach((subscriberId, subscriber) -> {
            try {
                log.info("Stopping sink subscriber id: {}", subscriberId);
                subscriber.stop();
                log.info("Stopped sink subscriber id: {}", subscriberId);
            } catch (Throwable ex) {
                log.error("Failed to stop sink subscriber id: {}", subscriberId, ex);
            }
        });
    }

    @Override
    public void run(ApplicationArguments args) {
        registerFilteringAndSenderSubscriber();
        startAllSubscriber();
    }

    private void registerFilteringAndSenderSubscriber() {
        config.getPipelines().forEach(pipeline -> {
            final KafkaSubscriberProperties.SourceProperties source = pipeline.getSource();

            // Create acknowledge producer.
            final Producer<String, String> acknowledgeProducer = KafkaProducerBuilder.buildDefaultAcknowledgedKafkaProducer(
                    source.getConsumerProps().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

            // Register filter dispatcher.
            filterSubscribers.computeIfAbsent(source.getGroupId(), sourceGroupId -> {
                final FilterBatchMessageDispatcher filterDispatcher = new FilterBatchMessageDispatcher(
                        context, pipeline, facade, registry, source.getGroupId(), acknowledgeProducer);
                filterDispatcher.init();
                return new KafkaConsumerBuilder(source.getConsumerProps())
                        .buildSubscriber(source.getTopicPattern(), sourceGroupId, source.getParallelism(), filterDispatcher);
            });

            // Register sink dispatchers If necessary. (per subscribers a sink dispatcher instance)
            final KafkaSubscriberProperties.SinkProperties sink = pipeline.getSink();
            if (!sink.isEnable()) {
                log.info("Pipeline sink is disabled, skip register sink dispatcher, pipeline: {}", pipeline);
                return;
            }
            safeList(registry.getAll()).forEach(subscriber -> {
                subscriber.validate();
                final String sinkFromTopic = facade.generateCheckpointTopic(pipeline.getFilter(), subscriber.getId());
                final String sinkGroupId = facade.generateSinkGroupId(sink, subscriber.getId());

                sinkSubscribers.computeIfAbsent(subscriber.getId(), subscriberId -> {
                    final SinkSubscriberBatchMessageDispatcher sinkDispatcher = new SinkSubscriberBatchMessageDispatcher(
                            context, pipeline, facade, registry, sinkGroupId, acknowledgeProducer, subscriber);
                    sinkDispatcher.init();
                    return new KafkaConsumerBuilder(sink.getConsumerProps())
                            .buildSubscriber(Pattern.compile(sinkFromTopic), sinkGroupId, sink.getParallelism(), sinkDispatcher);
                });
            });
        });
    }

    public void stopSinkSubscriber() {

    }

    private void startAllSubscriber() {
        log.info("Starting all pipeline filter subscribers for {}...", filterSubscribers.size());
        filterSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);

        log.info("Starting all pipeline sink subscribers for {}...", sinkSubscribers.size());
        sinkSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);
    }

}
