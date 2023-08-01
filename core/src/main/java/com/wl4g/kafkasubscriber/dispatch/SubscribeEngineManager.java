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

import com.wl4g.infra.common.collection.CollectionUtils2;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaConsumerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties.SourceProperties;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.facade.SubscribeSourceProvider;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.lang.Assert2.notNullOf;
import static com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties.EnginePipelineProperties;
import static com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties.SinkProperties;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * The {@link SubscribeEngineManager}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class SubscribeEngineManager implements ApplicationRunner, Closeable {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ApplicationContext context;
    private final KafkaSubscriberProperties config;
    private final SubscribeEngineCustomizer customizer;
    private final SubscribeSourceProvider sourceProvider;
    private final CachingSubscriberRegistry registry;
    private final Map<String, SubscribePipelineBootstrap> pipelineRegistry;

    public SubscribeEngineManager(@NotNull ApplicationContext context,
                                  @NotNull KafkaSubscriberProperties config,
                                  @NotNull SubscribeEngineCustomizer customizer,
                                  @NotNull SubscribeSourceProvider sourceProvider,
                                  @NotNull CachingSubscriberRegistry registry) {
        this.context = notNullOf(context, "context");
        this.config = notNullOf(config, "config");
        this.customizer = notNullOf(customizer, "customizer");
        this.sourceProvider = notNullOf(sourceProvider, "sourceProvider");
        this.registry = notNullOf(registry, "registry");
        this.pipelineRegistry = new ConcurrentHashMap<>(config.getPipelines().size());
    }

    public Map<String, SubscribePipelineBootstrap> getPipelineRegistry() {
        return unmodifiableMap(pipelineRegistry.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey,
                        e -> new SubscribePipelineBootstrap(e.getKey(),
                                unmodifiableMap(e.getValue().getFilterBootstraps()),
                                unmodifiableMap(e.getValue().getSinkBootstraps())))));
    }

    @Override
    public void close() {
        pipelineRegistry.forEach((pipelineName, pipelineBootstrap) -> {
            try {
                log.info("Stopping subscribe pipeline tenantId: {}", pipelineName);
                pipelineBootstrap.stopFilters(15_000);
                log.info("Stopped subscribe pipeline tenantId: {}", pipelineName);
            } catch (Throwable ex) {
                log.error("Failed to stop subscriber pipeline tenantId: {}", pipelineName, ex);
            }
        });
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!running.compareAndSet(false, true)) {
            log.warn("Already started, ignore again.");
            return;
        }
        registerAllPipelines();
        startAllPipelines();
    }

    private void registerAllPipelines() {
        safeList(config.getPipelines()).forEach(this::registerPipeline);

        log.info("---------------- ::: [Begin] Registered all pipeline subscribers details ::: ----------------");
        safeMap(pipelineRegistry)
                .forEach((pipelineName, pipelineBootstrap) -> {
                    safeMap(pipelineBootstrap.getFilterBootstraps()) // source=>bootstrap
                            .forEach((sourceName, value) -> log.info("Registered filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                    pipelineName, value.getDispatcher().getSourceConfig().getName(),
                                    value.getDispatcher().getTopicDesc(),
                                    value.getDispatcher().getGroupId()));
                    safeMap(pipelineBootstrap.getSinkBootstraps()) // subscriberId=>bootstrap
                            .forEach((subscriberId, value) -> log.info("Registered sink subscribe bootstrap of pipeline: {}, subscriber: {}, topic: {}, groupId: {}",
                                    pipelineName, value.getDispatcher().getSubscriber().getId(),
                                    value.getDispatcher().getTopicDesc(),
                                    value.getDispatcher().getGroupId()));
                });
        log.info("---------------- ::: Registered all pipeline subscribers details [End] ::: ----------------");
    }

    private void startAllPipelines() {
        log.info("Starting all pipeline filter subscribers for {}...", pipelineRegistry.size());
        pipelineRegistry.values().forEach(SubscribePipelineBootstrap::startFilters);

        log.info("Starting all pipeline sink subscribers for {}...", pipelineRegistry.size());
        pipelineRegistry.values().forEach(SubscribePipelineBootstrap::startSinks);
    }

    /**
     * Register a new pipeline, ignore if already exists.
     *
     * @param pipelineConfig pipeline properties.
     * @return If it does not exist, it will be newly registered and return the pipeline bootstrap object, otherwise it will return null.
     */
    public SubscribePipelineBootstrap registerPipeline(EnginePipelineProperties pipelineConfig) {
        Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        pipelineConfig.validate();

        if (!pipelineConfig.isEnable()) {
            log.info("Disabled to register subscribe pipeline: {}", pipelineConfig.getName());
            return null;
        }
        if (pipelineRegistry.containsKey(pipelineConfig.getName())) {
            log.info("Already to registered subscribe pipeline: {}", pipelineConfig.getName());
            return null;
        }

        return pipelineRegistry.computeIfAbsent(pipelineConfig.getName(), pipelineName -> {
            log.info("Registering to pipeline {} => {} ...", pipelineName, pipelineConfig);

            // Build filter dispatchers.
            final Map<String, SubscribeContainerBootstrap<FilterBatchMessageDispatcher>> filterBootstraps =
                    safeList(pipelineConfig.getInternalSourceProvider().loadSources(pipelineName))
                            .stream()
                            .collect(Collectors.toMap(
                                    KafkaSubscriberProperties.BaseConsumerProperties::getName,
                                    source -> registerPipelineFilter(pipelineConfig, source)));

            Map<String, SubscribeContainerBootstrap<SinkBatchMessageDispatcher>> sinkBootstraps = emptyMap();
            // Register sink config If necessary. (per subscriber a sink dispatcher instance)
            final KafkaSubscriberProperties.SinkProperties sinkConfig = pipelineConfig.getInternalSink();
            if (Objects.isNull(sinkConfig)) {
                log.info("Pipeline sinkConfig is disabled, skip register sinkConfig dispatcher, pipeline: {}",
                        pipelineConfig);
            } else {
                // Build sink dispatchers.
                sinkBootstraps = safeList(registry.getShardingAll())
                        .stream()
                        .collect(Collectors.toMap(
                                SubscriberInfo::getId,
                                subscriber -> registerPipelineSink(pipelineConfig, sinkConfig, subscriber)));
            }

            return new SubscribePipelineBootstrap(pipelineName, filterBootstraps, sinkBootstraps);
        });
    }

    public SubscribeContainerBootstrap<FilterBatchMessageDispatcher> registerPipelineFilter(EnginePipelineProperties pipelineConfig,
                                                                                            SourceProperties sourceConfig) {
        Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        Assert2.notNullOf(sourceConfig, "sourceConfig");
        pipelineConfig.validate();
        sourceConfig.validate();

        // Obtain custom filter.
        final ISubscribeFilter filter = obtainSubscribeFilter(sourceConfig.getGroupId(), pipelineConfig.getFilter());

        // Build acknowledge producer.
        final Producer<String, String> acknowledgeProducer = KafkaProducerBuilder
                .buildDefaultAcknowledgedKafkaProducer(sourceConfig.getConsumerProps()
                        .get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        // Build filter dispatcher.
        final FilterBatchMessageDispatcher dispatcher = new FilterBatchMessageDispatcher(
                context, pipelineConfig, sourceConfig, customizer, registry, sourceConfig.getTopicPattern().toString(),
                sourceConfig.getGroupId(), filter, acknowledgeProducer);

        return new SubscribeContainerBootstrap<>(dispatcher,
                new KafkaConsumerBuilder(sourceConfig.getConsumerProps())
                        .buildSubscriber(sourceConfig.getTopicPattern(), sourceConfig.getGroupId(),
                                sourceConfig.getParallelism(), dispatcher));
    }

    public SubscribeContainerBootstrap<SinkBatchMessageDispatcher> registerPipelineSink(EnginePipelineProperties pipelineConfig,
                                                                                        SinkProperties sinkConfig,
                                                                                        SubscriberInfo subscriber) {
        Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        Assert2.notNullOf(sinkConfig, "sinkConfig");
        Assert2.notNullOf(subscriber, "subscriber");
        pipelineConfig.validate();
        sinkConfig.validate();
        subscriber.validate();

        final String sinkFromTopic = customizer.generateCheckpointTopic(pipelineConfig.getName(),
                pipelineConfig.getInternalFilter().getTopicPrefix(), subscriber.getId());
        final String sinkGroupId = customizer.generateSinkGroupId(pipelineConfig.getName(), sinkConfig, subscriber.getId());

        // Obtain custom sink.
        final ISubscribeSink sink = obtainSubscribeSink(sinkGroupId, sinkConfig.getName(), subscriber);

        // Build sink dispatcher.
        final SinkBatchMessageDispatcher dispatcher = new SinkBatchMessageDispatcher(
                context, pipelineConfig, customizer, registry, sinkFromTopic, sinkGroupId, subscriber, sink);

        return new SubscribeContainerBootstrap<>(dispatcher,
                new KafkaConsumerBuilder(sinkConfig.getConsumerProps())
                        .buildSubscriber(Pattern.compile(sinkFromTopic), sinkGroupId,
                                sinkConfig.getParallelism(), dispatcher));
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

    @Slf4j
    @AllArgsConstructor
    @Getter
    public static class SubscribePipelineBootstrap {
        private final String name; // pipeline name
        private final Map<String, SubscribeContainerBootstrap<FilterBatchMessageDispatcher>> filterBootstraps; // source=>bootstrap
        private final Map<String, SubscribeContainerBootstrap<SinkBatchMessageDispatcher>> sinkBootstraps; // subscriberId=>bootstrap

        public Map<String, Boolean> startFilters(String... sourceNames) {
            return safeMap(filterBootstraps).entrySet().stream()
                    .filter(e -> CollectionUtils2.isEmptyArray(sourceNames) || StringUtils.equalsAny(e.getKey(), sourceNames))
                    .collect(Collectors.toMap(
                            entry -> {
                                try {
                                    final SubscribeContainerBootstrap<FilterBatchMessageDispatcher> bootstrap = entry.getValue();

                                    log.info("Starting filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                            name, bootstrap.getDispatcher().getSourceConfig().getName(),
                                            bootstrap.getDispatcher().getTopicDesc(),
                                            bootstrap.getDispatcher().getGroupId());

                                    bootstrap.start();

                                    log.info("Started filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                            name, bootstrap.getDispatcher().getSourceConfig().getName(),
                                            bootstrap.getDispatcher().getTopicDesc(),
                                            bootstrap.getDispatcher().getGroupId());

                                    return entry.getKey();
                                } catch (Throwable th) {
                                    log.error("Failed to start filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                            name, entry.getValue().getDispatcher().getSourceConfig().getName(),
                                            entry.getValue().getDispatcher().getTopicDesc(),
                                            entry.getValue().getDispatcher().getGroupId(), th);
                                    return entry.getKey();
                                }
                            }, entry -> entry.getValue().getContainer().isRunning()));
        }

        public Map<String, Boolean> startSinks(String... subscriberIds) {
            return safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> CollectionUtils2.isEmptyArray(subscriberIds) || StringUtils.equalsAny(e.getKey(), subscriberIds))
                    .collect(Collectors.toMap(entry -> {
                        try {
                            final SubscribeContainerBootstrap<SinkBatchMessageDispatcher> bootstrap = entry.getValue();

                            log.info("Starting sink subscribe bootstrap of pipeline: {}, subscriber: {}, topic: {}",
                                    name, bootstrap.getDispatcher().getSubscriber().getId(),
                                    bootstrap.getDispatcher().getTopicDesc());

                            bootstrap.start();

                            log.info("Started sink subscribe bootstrap: of pipeline {}, subscriber: {}, topic: {}",
                                    name,
                                    bootstrap.getDispatcher().getSubscriber().getId(),
                                    bootstrap.getDispatcher().getTopicDesc());
                            return entry.getKey();
                        } catch (Throwable th) {
                            log.error("Failed to start sink subscribe bootstrap of pipeline: {}, subscriber: {}, topic: {}",
                                    name, entry.getValue().getDispatcher().getSubscriber().getId(),
                                    entry.getValue().getDispatcher().getTopicDesc(), th);
                            return entry.getKey();
                        }
                    }, entry -> entry.getValue().getContainer().isRunning()));
        }

        public Map<String, Boolean> stopFilters(long perFilterTimeout, String... sourceNames) {
            // Stopping filter.
            final Map<String, Boolean> result = safeMap(filterBootstraps).entrySet().stream()
                    .filter(e -> CollectionUtils2.isEmptyArray(sourceNames) || StringUtils.equalsAny(e.getKey(), sourceNames))
                    .collect(Collectors.toMap(
                            entry -> entry.getValue().getDispatcher().getSourceConfig().getName(),
                            entry -> {
                                final SubscribeContainerBootstrap<FilterBatchMessageDispatcher> bootstrap = entry.getValue();
                                final String sourceName = bootstrap.getDispatcher().getSourceConfig().getName();
                                try {
                                    log.info("Stopping subscribe for source: {}", sourceName);
                                    final boolean r = bootstrap.stop(perFilterTimeout);
                                    log.info("Stopped subscribe for source: {}", sourceName);
                                    return r;
                                } catch (Throwable ex) {
                                    log.error("Failed to stop subscribe for source: {}", sourceName, ex);
                                    return bootstrap.getContainer().isRunning();
                                }
                            }));

            // Remove stopped filter.
            safeMap(result).entrySet().iterator().forEachRemaining(entry -> {
                if (entry.getValue()) {
                    log.debug("Removing filter for source: {}", entry.getKey());
                    filterBootstraps.remove(entry.getKey());
                    log.debug("Removed filter for source: {}", entry.getKey());
                }
            });

            return result;
        }

        public Map<String, Boolean> stopSinks(long perSinkTimeout, String... subscriberIds) {
            // Stopping sink.
            final Map<String, Boolean> result = safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> CollectionUtils2.isEmptyArray(subscriberIds) || StringUtils.equalsAny(e.getKey(), subscriberIds))
                    .collect(Collectors.toMap(
                            entry -> entry.getValue().getDispatcher().getSubscriber().getId(),
                            entry -> {
                                final SubscribeContainerBootstrap<SinkBatchMessageDispatcher> bootstrap = entry.getValue();
                                final String subscriberId = bootstrap.getDispatcher().getSubscriber().getId();
                                try {
                                    log.info("Stopping sink for subscriber id: {}", subscriberId);
                                    final boolean r = bootstrap.stop(perSinkTimeout);
                                    log.info("Stopped sink for subscriber id: {}", subscriberId);
                                    return r;
                                } catch (Throwable ex) {
                                    log.error("Failed to stop sink subscriber id: {}", subscriberId, ex);
                                    return bootstrap.getContainer().isRunning();
                                }
                            }));

            // Remove stopped sink.
            safeMap(result).entrySet().iterator().forEachRemaining(entry -> {
                if (entry.getValue()) {
                    log.debug("Removing sink for subscriber id: {}", entry.getKey());
                    sinkBootstraps.remove(entry.getKey());
                    log.debug("Removed sink for subscriber id: {}", entry.getKey());
                }
            });

            return result;
        }
    }

    @Getter
    @AllArgsConstructor
    public static class SubscribeContainerBootstrap<D extends AbstractBatchMessageDispatcher> {
        private final D dispatcher;
        private final ConcurrentMessageListenerContainer<String, String> container;

        public void start() {
            if (!isRunning()) {
                container.start();
            }
        }

        public boolean stop(long shutdownTimeout) throws InterruptedException {
            return stop(true, shutdownTimeout);
        }

        public boolean stop(boolean gracefulShutdown, long shutdownTimeout) throws InterruptedException {
            if (!isRunning()) {
                return true;
            }
            final CountDownLatch latch = new CountDownLatch(1);
            // graceful shutdown
            if (gracefulShutdown) {
                container.stop(latch::countDown);
                latch.await(shutdownTimeout, TimeUnit.MILLISECONDS);
            } else {
                container.stopAbnormally(latch::countDown);
                latch.await(shutdownTimeout, TimeUnit.MILLISECONDS);
            }
            return container.isRunning();
        }

        public boolean isRunning() {
            return container.isRunning();
        }

    }

}
