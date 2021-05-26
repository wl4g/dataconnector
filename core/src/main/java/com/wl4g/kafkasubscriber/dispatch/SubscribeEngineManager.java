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
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.BaseConsumerConfig;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeFilterConfig;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeSourceConfig;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
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

import static com.wl4g.infra.common.collection.CollectionUtils2.isEmptyArray;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.lang.Assert2.notNullOf;
import static com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeEnginePipelineConfig;
import static com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeSinkConfig;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.apache.commons.lang3.StringUtils.equalsAny;

/**
 * The {@link SubscribeEngineManager}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Order(Ordered.LOWEST_PRECEDENCE - 100)
public class SubscribeEngineManager implements ApplicationRunner, Closeable {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final KafkaSubscribeConfiguration config;
    private final CheckpointTopicManager topicManager;
    private final SubscribeEngineCustomizer customizer;
    private final CachingSubscriberRegistry registry;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<String, SubscribePipelineBootstrap> pipelineRegistry;

    public SubscribeEngineManager(@NotNull KafkaSubscribeConfiguration config,
                                  @NotNull CheckpointTopicManager topicManager,
                                  @NotNull SubscribeEngineCustomizer customizer,
                                  @NotNull ApplicationEventPublisher eventPublisher,
                                  @NotNull CachingSubscriberRegistry registry) {
        this.config = notNullOf(config, "config");
        this.topicManager = notNullOf(topicManager, "topicManager");
        this.customizer = notNullOf(customizer, "customizer");
        this.registry = notNullOf(registry, "registry");
        this.eventPublisher = notNullOf(eventPublisher, "eventPublisher");
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

        // TODO
        // topicManager.initPipelinesTopicIfNecessary(15_000);
        registerAllPipelines();
        startAllPipelines();
    }

    private void registerAllPipelines() {
        log.info("Registering to all pipelines ...");
        safeList(config.getPipelines()).forEach(this::registerPipeline);

        log.info("---------------- ::: [Begin] Registered all pipeline subscribers details ::: ----------------");
        safeMap(pipelineRegistry)
                .forEach((pipelineName, pipelineBootstrap) -> {
                    // source=>bootstrap
                    safeMap(pipelineBootstrap.getFilterBootstraps())
                            .forEach((sourceName, value) -> log.info("Registered filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                    pipelineName, value.getDispatcher().getSubscribeSourceConfig().getName(),
                                    value.getDispatcher().getTopicDesc(),
                                    value.getDispatcher().getGroupId()));
                    // subscriberId=>bootstrap
                    safeMap(pipelineBootstrap.getSinkBootstraps())
                            .forEach((subscriberId, value) -> log.info("Registered sink subscribe bootstrap of pipeline: {}, subscriber: {}, topic: {}, groupId: {}",
                                    pipelineName, value.getDispatcher().getSubscriber().getId(),
                                    value.getDispatcher().getTopicDesc(),
                                    value.getDispatcher().getGroupId()));
                });
        log.info("---------------- ::: Registered all pipeline subscribers details [End] ::: ----------------");
    }

    private void startAllPipelines() {
        log.info("Starting to all pipelines filter subscribers for {}...", pipelineRegistry.size());
        pipelineRegistry.values().forEach(SubscribePipelineBootstrap::startFilters);

        log.info("Starting to all pipelines sink subscribers for {}...", pipelineRegistry.size());
        pipelineRegistry.values().forEach(SubscribePipelineBootstrap::startSinks);
    }

    /**
     * Register a new pipeline, ignore if already exists.
     *
     * @param pipelineConfig pipeline properties.
     * @return If it does not exist, it will be newly registered and return the pipeline bootstrap object, otherwise it will return null.
     */
    public SubscribePipelineBootstrap registerPipeline(SubscribeEnginePipelineConfig pipelineConfig) {
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
                    safeList(pipelineConfig.getParsedSourceProvider().loadSources(pipelineName))
                            .stream()
                            .map(SubscribeSourceConfig::optimizeProperties)
                            .collect(Collectors.toMap(
                                    BaseConsumerConfig::getName,
                                    source -> registerPipelineFilter(pipelineConfig, (SubscribeSourceConfig) source)));

            Map<String, SubscribeContainerBootstrap<SinkBatchMessageDispatcher>> sinkBootstraps = emptyMap();
            // Register sink config If necessary. (per subscriber a sink dispatcher instance)
            final ISubscribeSink subscribeSink = pipelineConfig.getParsedSink();
            if (Objects.isNull(subscribeSink)) {
                log.info("Pipeline sinkConfig is disabled, skip register sinkConfig dispatcher, pipeline: {}",
                        pipelineConfig);
            } else {
                // Build sink dispatchers.
                sinkBootstraps = safeList(registry.getSubscribers(pipelineConfig.getName()))
                        .stream()
                        .collect(Collectors.toMap(
                                SubscriberInfo::getId,
                                subscriber -> registerPipelineSink(pipelineConfig, subscriber)));
            }

            return new SubscribePipelineBootstrap(pipelineName, filterBootstraps, sinkBootstraps);
        });
    }

    public SubscribeContainerBootstrap<FilterBatchMessageDispatcher> registerPipelineFilter(
            SubscribeEnginePipelineConfig pipelineConfig,
            SubscribeSourceConfig subscribeSourceConfig) {
        Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        Assert2.notNullOf(subscribeSourceConfig, "sourceConfig");
        pipelineConfig.validate();
        subscribeSourceConfig.validate();
        subscribeSourceConfig.optimizeProperties();

        // Build acknowledge producer.
        final Producer<String, String> acknowledgeProducer = KafkaProducerBuilder
                .buildDefaultAcknowledgedKafkaProducer(subscribeSourceConfig.getConsumerProps()
                        .get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        // Build filter dispatcher.
        final FilterBatchMessageDispatcher dispatcher = new FilterBatchMessageDispatcher(
                config,
                pipelineConfig,
                subscribeSourceConfig,
                customizer,
                registry,
                eventPublisher,
                subscribeSourceConfig.getTopicPattern(),
                subscribeSourceConfig.getGroupId(),
                pipelineConfig.getParsedFilter(),
                acknowledgeProducer);

        return new SubscribeContainerBootstrap<>(dispatcher,
                new KafkaConsumerBuilder(subscribeSourceConfig.getConsumerProps())
                        .buildSubscriber(Pattern.compile(subscribeSourceConfig.getTopicPattern()),
                                subscribeSourceConfig.getGroupId(),
                                subscribeSourceConfig.getParallelism(), dispatcher));
    }

    public SubscribeContainerBootstrap<SinkBatchMessageDispatcher> registerPipelineSink(
            SubscribeEnginePipelineConfig pipelineConfig,
            SubscriberInfo subscriber) {
        Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        Assert2.notNullOf(subscriber, "subscriber");
        pipelineConfig.validate();
        subscriber.validate();

        final SubscribeFilterConfig filterConfig = pipelineConfig.getParsedFilter().getFilterConfig();
        final SubscribeSinkConfig sinkConfig = (SubscribeSinkConfig) pipelineConfig.getParsedSink()
                .getSinkConfig().optimizeProperties();

        final String sinkFromTopic = customizer.generateCheckpointTopic(pipelineConfig.getName(),
                filterConfig.getTopicPrefix(), subscriber.getId());
        final String sinkGroupId = customizer.generateSinkGroupId(pipelineConfig.getName(), sinkConfig, subscriber.getId());

        // Build sink dispatcher.
        final SinkBatchMessageDispatcher dispatcher = new SinkBatchMessageDispatcher(
                config,
                pipelineConfig,
                customizer,
                registry,
                eventPublisher,
                sinkFromTopic,
                sinkGroupId,
                subscriber,
                pipelineConfig.getParsedSink());

        return new SubscribeContainerBootstrap<>(dispatcher,
                new KafkaConsumerBuilder(sinkConfig.getConsumerProps())
                        .buildSubscriber(Pattern.compile(sinkFromTopic), sinkGroupId,
                                sinkConfig.getParallelism(), dispatcher));
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
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(Collectors.toMap(
                            entry -> {
                                try {
                                    final SubscribeContainerBootstrap<FilterBatchMessageDispatcher> bootstrap = entry.getValue();

                                    log.info("Starting filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                            name, bootstrap.getDispatcher().getSubscribeSourceConfig().getName(),
                                            bootstrap.getDispatcher().getTopicDesc(),
                                            bootstrap.getDispatcher().getGroupId());

                                    bootstrap.start();

                                    log.info("Started filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                            name, bootstrap.getDispatcher().getSubscribeSourceConfig().getName(),
                                            bootstrap.getDispatcher().getTopicDesc(),
                                            bootstrap.getDispatcher().getGroupId());

                                    return entry.getKey();
                                } catch (Throwable th) {
                                    log.error("Failed to start filter subscribe bootstrap of pipeline: {}, source: {}, topic: {}, groupId: {}",
                                            name, entry.getValue().getDispatcher().getSubscribeSourceConfig().getName(),
                                            entry.getValue().getDispatcher().getTopicDesc(),
                                            entry.getValue().getDispatcher().getGroupId(), th);
                                    return entry.getKey();
                                }
                            }, entry -> entry.getValue().getContainer().isRunning()));
        }

        public Map<String, Boolean> startSinks(String... subscriberIds) {
            return safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(subscriberIds) || equalsAny(e.getKey(), subscriberIds))
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
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(Collectors.toMap(
                            //entry -> entry.getValue().getDispatcher().getSubscribeSourceConfig().getName(),
                            Entry::getKey,
                            entry -> {
                                final SubscribeContainerBootstrap<FilterBatchMessageDispatcher> bootstrap = entry.getValue();
                                final String sourceName = bootstrap.getDispatcher().getSubscribeSourceConfig().getName();
                                try {
                                    log.info("Stopping filter subscribe for source: {}", sourceName);
                                    final boolean r = bootstrap.stop(perFilterTimeout);
                                    log.info("Stopped filter subscribe for source: {}", sourceName);
                                    return r;
                                } catch (Throwable ex) {
                                    log.error("Failed to stop subscribe filter for source: {}", sourceName, ex);
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
                    .filter(e -> isEmptyArray(subscriberIds) || equalsAny(e.getKey(), subscriberIds))
                    .collect(Collectors.toMap(
                            //entry -> entry.getValue().getDispatcher().getSubscriber().getId(),
                            Entry::getKey,
                            entry -> {
                                final SubscribeContainerBootstrap<SinkBatchMessageDispatcher> bootstrap = entry.getValue();
                                final String subscriberId = bootstrap.getDispatcher().getSubscriber().getId();
                                try {
                                    log.info("Stopping sink subscribe for id: {}", subscriberId);
                                    final boolean r = bootstrap.stop(perSinkTimeout);
                                    log.info("Stopped sink subscribe for id: {}", subscriberId);
                                    return r;
                                } catch (Throwable ex) {
                                    log.error("Failed to stop sink subscribe id: {}", subscriberId, ex);
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

        public Map<String, ContainerStatus> statusFilters(String... sourceNames) {
            return safeMap(filterBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(Collectors.toMap(Entry::getKey, entry -> new ContainerStatus(entry.getValue().getContainer().isRunning(),
                            entry.getValue().getContainer().getContainers().size())));
        }

        public Map<String, ContainerStatus> statusSinks(String... subscriberIds) {
            return safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(subscriberIds) || equalsAny(e.getKey(), subscriberIds))
                    .collect(Collectors.toMap(Entry::getKey,
                            entry -> new ContainerStatus(entry.getValue().getContainer().isRunning(),
                                    entry.getValue().getContainer().getContainers().size())));
        }

        public Map<String, Integer> scalingFilters(int perConcurrency,
                                                   boolean restart,
                                                   long perRestartTimeout,
                                                   String... sourceNames) {
            return safeMap(filterBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(Collectors.toMap(
                            Entry::getKey,
                            entry -> {
                                final SubscribeContainerBootstrap<FilterBatchMessageDispatcher> bootstrap = entry.getValue();
                                final String sourceName = bootstrap.getDispatcher().getSubscribeSourceConfig().getName();
                                try {
                                    log.info("Scaling filter subscribe for source: {}", sourceName);
                                    bootstrap.scaling(perConcurrency, restart, perRestartTimeout);
                                    log.info("Scaled filter subscribe for source: {}", sourceName);
                                } catch (Throwable ex) {
                                    log.error("Failed to stop filter subscribe for source: {}", sourceName, ex);
                                }
                                return bootstrap.getContainer().getContainers().size();
                            }));
        }

        public Map<String, Integer> scalingSinks(int perConcurrency,
                                                 boolean restart,
                                                 long perRestartTimeout,
                                                 String... subscriberIds) {
            return safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(subscriberIds) || equalsAny(e.getKey(), subscriberIds))
                    .collect(Collectors.toMap(
                            Entry::getKey,
                            entry -> {
                                final SubscribeContainerBootstrap<SinkBatchMessageDispatcher> bootstrap = entry.getValue();
                                final String subscriberId = bootstrap.getDispatcher().getSubscriber().getId();
                                try {
                                    log.info("Scaling sink subscribe for id: {}", subscriberId);
                                    bootstrap.scaling(perConcurrency, restart, perRestartTimeout);
                                    log.info("Scaled sink subscribe for id: {}", subscriberId);
                                } catch (Throwable ex) {
                                    log.error("Failed to scaling sink subscribe id: {}", subscriberId, ex);
                                }
                                return bootstrap.getContainer().getContainers().size();
                            }));
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

        public boolean scaling(int concurrency,
                               boolean restart,
                               long restartTimeout) throws InterruptedException {
            container.setConcurrency(concurrency);
            if (restart) {
                if (stop(restartTimeout)) {
                    start();
                    return true;
                } else {
                    return false;
                }
            }
            // It is bound not to immediately scale the number of concurrent containers.
            return false;
        }

        public void pause() {
            container.pause();
        }

        public void resume() {
            container.resume();
        }

        public boolean isRunning() {
            return container.isRunning();
        }

        public boolean isHealthy() {
            return container.isInExpectedState();
        }

        public Map<String, Map<MetricName, ? extends Metric>> metrics() {
            return container.metrics();
        }

    }

    @Getter
    @AllArgsConstructor
    public static class ContainerStatus {
        private boolean running;
        private int actuateConcurrency;
    }

}
