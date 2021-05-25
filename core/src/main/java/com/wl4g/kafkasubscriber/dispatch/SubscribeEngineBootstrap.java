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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaConsumerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    private final SubscribeEngineCustomizer customizer;
    private final CachingSubscriberRegistry registry;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> filterSubscribers;
    private final Map<Long, ConcurrentMessageListenerContainer<String, String>> sinkSubscribers;

    public SubscribeEngineBootstrap(@NotNull ApplicationContext context,
                                    @NotNull KafkaSubscriberProperties config,
                                    @NotNull SubscribeEngineCustomizer customizer,
                                    @NotNull CachingSubscriberRegistry registry) {
        this.context = notNullOf(context, "context");
        this.config = notNullOf(config, "config");
        this.customizer = notNullOf(customizer, "customizer");
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
        registerAllPipelines();
        startAllPipelines();
    }

    private void registerAllPipelines() {
        config.getPipelines().forEach(pipeline -> {
            // Register filter dispatcher.
            safeList(pipeline.getInternalSources()).forEach(source -> {
                // Build acknowledge producer.
                final Producer<String, String> acknowledgeProducer = KafkaProducerBuilder.buildDefaultAcknowledgedKafkaProducer(
                        source.getConsumerProps().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
                final ISubscribeFilter filter = obtainSubscribeFilter(source.getGroupId(), pipeline.getFilter());

                filterSubscribers.computeIfAbsent(source.getGroupId(), sourceGroupId -> {
                    final FilterBatchMessageDispatcher filterDispatcher = new FilterBatchMessageDispatcher(
                            context, pipeline, source, customizer, registry, sourceGroupId,
                            source.getTopicPattern().toString(), filter, acknowledgeProducer);
                    filterDispatcher.init();
                    return new KafkaConsumerBuilder(source.getConsumerProps())
                            .buildSubscriber(source.getTopicPattern(), sourceGroupId, source.getParallelism(), filterDispatcher);
                });
            });

            // Register sinkConfig dispatcher If necessary. (per subscriber a sinkConfig dispatcher instance)
            final KafkaSubscriberProperties.SinkProperties sinkConfig = pipeline.getInternalSink();
            if (Objects.isNull(sinkConfig)) {
                log.info("Pipeline sinkConfig is disabled, skip register sinkConfig dispatcher, pipeline: {}", pipeline);
                return;
            }
            safeList(registry.getAll()).forEach(subscriber -> {
                subscriber.validate();
                final String sinkFromTopic = customizer.generateCheckpointTopic(pipeline.getInternalFilter(), subscriber.getId());
                final String sinkGroupId = customizer.generateSinkGroupId(sinkConfig, subscriber.getId());
                final ISubscribeSink sink = obtainSubscribeSink(sinkGroupId, sinkConfig.getName(), subscriber);

                sinkSubscribers.computeIfAbsent(subscriber.getId(), subscriberId -> {
                    final SinkSubscriberBatchMessageDispatcher sinkDispatcher = new SinkSubscriberBatchMessageDispatcher(
                            context, pipeline, sinkFromTopic, customizer, registry, sinkGroupId, subscriber, sink);
                    sinkDispatcher.init();
                    return new KafkaConsumerBuilder(sinkConfig.getConsumerProps())
                            .buildSubscriber(Pattern.compile(sinkFromTopic), sinkGroupId, sinkConfig.getParallelism(), sinkDispatcher);
                });
            });
        });
    }

    private void startAllPipelines() {
        log.info("Starting all pipeline filter subscribers for {}...", filterSubscribers.size());
        filterSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);

        log.info("Starting all pipeline sink subscribers for {}...", sinkSubscribers.size());
        sinkSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);
    }

    /**
     * Obtain custom subscribe filter. (Each pipeline custom filter a instance)
     *
     * @param groupId          groupId
     * @param customFilterName customFilterName
     * @return {@link ISubscribeFilter}
     */
    private ISubscribeFilter obtainSubscribeFilter(String groupId, String customFilterName) {
        try {
            log.info("{} :: Obtaining custom subscriber filter...", customFilterName);
            return context.getBean(customFilterName, ISubscribeFilter.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new IllegalStateException(String.format("%s :: Could not obtain custom subscribe filter of bean %s",
                    groupId, customFilterName));
        }
    }

    /**
     * Obtain custom subscribe sinker. (Each pipeline custom sink instances)
     *
     * @param groupId          groupId
     * @param customFilterName customFilterName
     * @param subscriber       subscriber
     * @return {@link ISubscribeSink}
     */
    private ISubscribeSink obtainSubscribeSink(String groupId,
                                               String customFilterName,
                                               SubscriberInfo subscriber) {
        try {
            log.info("{} :: {} :: Creating custom subscriber sink of bean {}",
                    groupId, subscriber.getId(), customFilterName);
            return context.getBean(customFilterName, ISubscribeSink.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new IllegalStateException(String.format("%s :: %s :: Could not getting custom subscriber sink of bean %s",
                    groupId, subscriber.getId(), customFilterName));
        }
    }

    public @Null Boolean stopFilter(@NotBlank String sharedConsumerGroupId,
                                    long shutdownTimeout) throws InterruptedException {
        @Null Boolean result = true;
        Assert2.hasTextOf(sharedConsumerGroupId, "sharedConsumerGroupId");
        // Check for has it stopped
        if (filterSubscribers.containsKey(sharedConsumerGroupId)) {// force shutdown
            if (shutdownTimeout <= 0) {
                filterSubscribers.get(sharedConsumerGroupId).stopAbnormally(() -> {
                    filterSubscribers.remove(sharedConsumerGroupId);
                });
                result = null;
            } else { // graceful shutdown
                final CountDownLatch latch = new CountDownLatch(1);
                filterSubscribers.get(sharedConsumerGroupId).stop(latch::countDown);
                if (latch.await(shutdownTimeout, TimeUnit.MILLISECONDS)) {
                    filterSubscribers.remove(sharedConsumerGroupId);
                } else {
                    result = false;
                }
            }
        }
        return result;
    }

    public @Null Boolean stopSinker(@NotNull Long subscriberId,
                                    long shutdownTimeout) throws InterruptedException {
        Assert2.notNullOf(subscriberId, "subscriberId");
        // Check for has it stopped
        if (!sinkSubscribers.containsKey(subscriberId)) {
            return true;
        }
        // force shutdown
        if (shutdownTimeout <= 0) {
            sinkSubscribers.get(subscriberId).stopAbnormally(() -> {
                sinkSubscribers.remove(subscriberId);
            });
            return null;
        } else { // graceful shutdown
            final CountDownLatch latch = new CountDownLatch(1);
            sinkSubscribers.get(subscriberId).stop(latch::countDown);
            if (latch.await(shutdownTimeout, TimeUnit.MILLISECONDS)) {
                sinkSubscribers.remove(subscriberId);
                return true;
            }
            return false;
        }
    }

}
