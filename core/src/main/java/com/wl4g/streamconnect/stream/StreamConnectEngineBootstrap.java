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

package com.wl4g.streamconnect.stream;

import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.framework.StreamConnectSpiFactory;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import com.wl4g.streamconnect.stream.source.SourceStream;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsTag;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.wl4g.infra.common.collection.CollectionUtils2.isEmptyArray;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.lang.Assert2.notNullOf;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.equalsAny;

/**
 * The {@link StreamConnectEngineBootstrap}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Order(Ordered.LOWEST_PRECEDENCE - 100)
public class StreamConnectEngineBootstrap extends Collector implements ApplicationRunner, Closeable {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Environment environment;
    private final StreamConnectConfiguration config;
    private final CachingChannelRegistry registry;
    private final Map<String, StreamConnectorBootstrap> connectorRegistry;
    private final IStreamConnectCoordinator coordinator;

    public StreamConnectEngineBootstrap(@NotNull Environment environment,
                                        @NotNull StreamConnectConfiguration config,
                                        @NotNull CachingChannelRegistry registry) {
        this.environment = notNullOf(environment, "environment");
        this.config = notNullOf(config, "config");
        this.registry = notNullOf(registry, "registry");
        this.connectorRegistry = new ConcurrentHashMap<>(config.getConnectorMap().size());
        this.coordinator = config.getCoordinatorProvider().obtain(environment,
                config,
                config.getConfigurator(),
                registry,
                config.getMeter());
    }

    public Map<String, StreamConnectorBootstrap> getConnectorRegistry() {
        return unmodifiableMap(connectorRegistry.entrySet().stream()
                .collect(toMap(Entry::getKey,
                        e -> new StreamConnectorBootstrap(e.getKey(),
                                unmodifiableMap(e.getValue().getSourceBootstraps()),
                                unmodifiableMap(e.getValue().getSinkBootstraps())))));
    }

    @Override
    public void close() {
        this.connectorRegistry.forEach((connectorName, connectorBootstrap) -> {
            try {
                log.info("Stopping to connector: {}", connectorName);
                connectorBootstrap.stopSources(15_000);
                log.info("Stopped to connector: {}", connectorName);
            } catch (Throwable ex) {
                log.error("Failed to stop connector: {}", connectorName, ex);
            }
        });
        this.coordinator.close();
        this.registry.clear();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        startup();
    }

    private void startup() throws TimeoutException, InterruptedException {
        if (!running.compareAndSet(false, true)) {
            log.warn("Already started, ignore again.");
            return;
        }
        startCoordinatorAndWaitingReady();
        registerAllConnectors();
        startAllConnectors();
    }

    private void startCoordinatorAndWaitingReady() throws TimeoutException, InterruptedException {
        if (log.isInfoEnabled()) {
            log.info("Starting to coordinator ...");
        }
        this.coordinator.start();
        if (log.isInfoEnabled()) {
            log.info("Coordinator is started.");
        }
        this.coordinator.waitForReady();
        if (log.isInfoEnabled()) {
            log.info("Coordinator is ready.");
        }
    }

    private void registerAllConnectors() {
        if (log.isInfoEnabled()) {
            log.info("Registering to all connectors ...");
        }
        safeMap(config.getConnectorMap()).values().forEach(this::registerConnector);

        if (log.isInfoEnabled()) {
            final StringBuffer message = new StringBuffer("----- ::: [Begin] Registered all connectors details ::: -----\n");
            safeMap(connectorRegistry)
                    .forEach((connectorName, connectorBootstrap) -> {
                        // sourceName -> bootstrap
                        safeMap(connectorBootstrap.getSourceBootstraps())
                                .forEach((sourceName, value) -> {
                                    message.append(String.format("Registered channel source bootstrap of connector: %s, source: %s\n",
                                            connectorName, value.getStream().getDescription()));
                                });
                        // channelId -> bootstrap
                        safeMap(connectorBootstrap.getSinkBootstraps())
                                .forEach((channelId, value) -> {
                                    message.append(String.format("Registered channel sink bootstrap of connector: %s, sink: %s\n",
                                            connectorName, value.getStream().getDescription()));
                                });
                    });
            message.append("----- ::: Registered all connectors details [End] ::: -----\n");
            log.info(message.toString());
        }
    }

    private void startAllConnectors() {
        log.info("Starting to all connectors source for {}...", connectorRegistry.size());
        connectorRegistry.values().forEach(StreamConnectorBootstrap::startSources);

        log.info("Starting to all connectors sink for {}...", connectorRegistry.size());
        connectorRegistry.values().forEach(StreamConnectorBootstrap::startSinks);
    }

    /**
     * Register a new connector, ignore if already exists.
     *
     * @param connectorConfig connector configuration.
     * @return If it does not exist, it will be newly registered and return the connector bootstrap object, otherwise it will return null.
     */
    public StreamConnectorBootstrap registerConnector(ConnectorConfig connectorConfig) {
        requireNonNull(connectorConfig, "connectorConfig must not be null");
        connectorConfig.validate();

        if (!connectorConfig.isEnable()) {
            if (log.isInfoEnabled()) {
                log.info("Disabled to register channel connector: {}", connectorConfig.getName());
            }
            return null;
        }
        if (connectorRegistry.containsKey(connectorConfig.getName())) {
            if (log.isInfoEnabled()) {
                log.info("Already to registered channel connector: {}", connectorConfig.getName());
            }
            return null;
        }

        return connectorRegistry.computeIfAbsent(connectorConfig.getName(), connectorName -> {
            if (log.isInfoEnabled()) {
                log.info("Registering to connector {} => {} ...", connectorName, connectorConfig);
            }

            final AbstractStream.StreamContext context = new AbstractStream.StreamContext(environment,
                    config, connectorConfig, registry, this);

            // Register to source streams.
            final Map<String, StreamBootstrap<? extends SourceStream>> sourceBootstraps =
                    safeList(config.getConfigurator().loadSourceConfigs(connectorName))
                            .stream()
                            .collect(toMap(SourceStream.SourceStreamConfig::getName,
                                    sourceStreamConfig -> createConnectorSource(context, sourceStreamConfig)));

            // Register to sink streams. (per channel a sink stream instance)
            final Map<String, StreamBootstrap<? extends SinkStream>> sinkBootstraps =
                    safeList(registry.getAssignedChannels(connectorConfig.getName()))
                            .stream()
                            .collect(toMap(ChannelInfo::getId,
                                    channel -> createConnectorSink(context, channel)));

            return new StreamConnectorBootstrap(connectorName, sourceBootstraps, sinkBootstraps);
        });
    }

    public StreamBootstrap<? extends SourceStream> createConnectorSource(
            AbstractStream.StreamContext context,
            SourceStream.SourceStreamConfig sourceStreamConfig) {
        requireNonNull(context, "context must not be null");
        requireNonNull(sourceStreamConfig, "sourceStreamConfig must not be null");
        sourceStreamConfig.validate();

        return StreamConnectSpiFactory
                .get(SourceStream.SourceStreamProvider.class, AbstractStream.BaseStreamConfig.getStreamProviderTypeName(sourceStreamConfig.getType()))
                .create(context, sourceStreamConfig, registry);
    }

    public StreamBootstrap<? extends SinkStream> createConnectorSink(
            AbstractStream.StreamContext context,
            ChannelInfo channel) {
        requireNonNull(context, "context must not be null");
        requireNonNull(channel, "channel must not be null");
        channel.validate();

        final SinkStream.SinkStreamConfig sinkStreamConfig = channel.getSettingsSpec().getSinkSpec();
        return StreamConnectSpiFactory
                .get(SinkStream.SinkStreamProvider.class, AbstractStream.BaseStreamConfig.getStreamProviderTypeName(sinkStreamConfig.getType()))
                .create(context, sinkStreamConfig, channel);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        final List<MetricFamilySamples> result = new LinkedList<>();

        // Statistics of connectors count.
        result.add(new GaugeMetricFamily(
                MetricsName.connector_total.getName(),
                MetricsName.connector_total.getHelp(),
                connectorRegistry.keySet().size()));

        // Statistics of connector channels count.
        registry.getRegistry().forEach((connectorName, channels) -> {
            final GaugeMetricFamily gauge = new GaugeMetricFamily(
                    MetricsName.coordinator_sharding_channels_total.getName(),
                    MetricsName.coordinator_sharding_channels_total.getHelp(),
                    singletonList(MetricsTag.CONNECTOR));
            gauge.addMetric(singletonList(connectorName), channels.size());
            result.add(gauge);
        });

        return result;
    }

    @Slf4j
    @AllArgsConstructor
    @Getter
    public static class StreamConnectorBootstrap {
        private final String name; // connector name
        private final Map<String, StreamBootstrap<? extends SourceStream>> sourceBootstraps; // sourceName->bootstrap
        private final Map<String, StreamBootstrap<? extends SinkStream>> sinkBootstraps; // channelId->bootstrap

        public Map<String, Boolean> startSources(String... sourceNames) {
            return safeMap(sourceBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(toMap(entry -> {
                        try {
                            final StreamBootstrap<? extends SourceStream> bootstrap = entry.getValue();
                            if (log.isInfoEnabled()) {
                                log.info("Starting source stream bootstrap of connector: {}, source: {}",
                                        name, bootstrap.getStream().toString());
                            }
                            bootstrap.start();
                            if (log.isInfoEnabled()) {
                                log.info("Started source stream bootstrap of connector: {}, source: {}",
                                        name, bootstrap.getStream().toString());

                            }
                            return entry.getKey();
                        } catch (Throwable th) {
                            log.error(String.format("Failed to start source stream bootstrap of connector: %s, source: %s",
                                    name, entry.getValue().getStream()), th);
                            return entry.getKey();
                        }
                    }, entry -> entry.getValue().isRunning()));
        }

        public Map<String, Boolean> startSinks(String... channelIds) {
            return safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(channelIds) || equalsAny(e.getKey(), channelIds))
                    .collect(toMap(entry -> {
                        try {
                            final StreamBootstrap<? extends SinkStream> bootstrap = entry.getValue();
                            if (log.isInfoEnabled()) {
                                log.info("Starting sink channel bootstrap of connector: {}, channel: {}, topic: {}",
                                        name, bootstrap.getStream().getChannel().getId(),
                                        "bootstrap.getDispatcher().getTopicDesc()"); // TODO
                            }
                            bootstrap.start();
                            if (log.isInfoEnabled()) {
                                log.info("Started sink channel bootstrap: of connector {}, channel: {}, topic: {}",
                                        name,
                                        bootstrap.getStream().getChannel().getId(),
                                        "bootstrap.getDispatcher().getTopicDesc()"); // TODO
                            }
                            return entry.getKey();
                        } catch (Throwable th) {
                            log.error("Failed to start sink channel bootstrap of connector: {}, channel: {}, topic: {}",
                                    name, entry.getValue().getStream().getChannel().getId(),
                                    "entry.getValue().getDispatcher().getTopicDesc()", th); // TODO
                            return entry.getKey();
                        }
                    }, entry -> entry.getValue().isRunning()));
        }

        public Map<String, Boolean> stopSources(long perSourceTimeout, String... sourceNames) {
            // Stopping source.
            final Map<String, Boolean> result = safeMap(sourceBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(toMap(Entry::getKey,
                            entry -> {
                                final StreamBootstrap<? extends SourceStream> bootstrap = entry.getValue();
                                final String sourceName = bootstrap.getStream().getSourceStreamConfig().getName();
                                try {
                                    if (log.isInfoEnabled()) {
                                        log.info("Stopping source channel for source: {}", sourceName);
                                    }
                                    final boolean r = bootstrap.stop(perSourceTimeout);
                                    if (log.isInfoEnabled()) {
                                        log.info("Stopped source channel for source: {}", sourceName);
                                    }
                                    return r;
                                } catch (Throwable ex) {
                                    log.error("Failed to stop channel source for source: {}", sourceName, ex);
                                    return bootstrap.isRunning();
                                }
                            }));

            // Remove stopped source.
            safeMap(result).entrySet().iterator().forEachRemaining(entry -> {
                if (entry.getValue()) {
                    if (log.isInfoEnabled()) {
                        log.info("Removing source for source: {}", entry.getKey());
                    }
                    sourceBootstraps.remove(entry.getKey());
                    if (log.isInfoEnabled()) {
                        log.info("Removed source for source: {}", entry.getKey());
                    }
                }
            });

            return result;
        }

        public Map<String, Boolean> stopSinks(long perSinkTimeout, String... channelIds) {
            // Stopping sink.
            final Map<String, Boolean> result = safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(channelIds) || equalsAny(e.getKey(), channelIds))
                    .collect(toMap(Entry::getKey,
                            entry -> {
                                final StreamBootstrap<? extends SinkStream> bootstrap = entry.getValue();
                                final String channelId = bootstrap.getStream().getChannel().getId();
                                try {
                                    if (log.isInfoEnabled()) {
                                        log.info("Stopping sink channel for id: {}", channelId);
                                    }
                                    final boolean r = bootstrap.stop(perSinkTimeout);
                                    if (log.isInfoEnabled()) {
                                        log.info("Stopped sink channel for id: {}", channelId);
                                    }
                                    return r;
                                } catch (Throwable ex) {
                                    log.error("Failed to stop sink channel id: {}", channelId, ex);
                                    return bootstrap.isRunning();
                                }
                            }));

            // Remove stopped sink.
            safeMap(result).entrySet().iterator().forEachRemaining(entry -> {
                if (entry.getValue()) {
                    if (log.isInfoEnabled()) {
                        log.info("Removing sink for channel id: {}", entry.getKey());
                    }
                    sinkBootstraps.remove(entry.getKey());
                    if (log.isInfoEnabled()) {
                        log.info("Removed sink for channel id: {}", entry.getKey());
                    }
                }
            });

            return result;
        }

        public Map<String, ContainerTaskStatus> statusSources(String... sourceNames) {
            return safeMap(sourceBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(toMap(Entry::getKey, entry -> new ContainerTaskStatus(entry.getValue().isRunning(),
                            entry.getValue().getSubTaskCount())));
        }

        public Map<String, ContainerTaskStatus> statusSinks(String... channelIds) {
            return safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(channelIds) || equalsAny(e.getKey(), channelIds))
                    .collect(toMap(Entry::getKey,
                            entry -> new ContainerTaskStatus(entry.getValue().isRunning(),
                                    entry.getValue().getSubTaskCount())));
        }

        public Map<String, Integer> scalingSources(int perConcurrency,
                                                   boolean restart,
                                                   long perRestartTimeout,
                                                   String... sourceNames) {
            return safeMap(sourceBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(sourceNames) || equalsAny(e.getKey(), sourceNames))
                    .collect(toMap(
                            Entry::getKey,
                            entry -> {
                                final StreamBootstrap<? extends SourceStream> bootstrap = entry.getValue();
                                final String sourceName = bootstrap.getStream().getSourceStreamConfig().getName();
                                try {
                                    if (log.isInfoEnabled()) {
                                        log.info("Scaling source channel for source: {}", sourceName);
                                    }
                                    bootstrap.scaling(perConcurrency, restart, perRestartTimeout);
                                    if (log.isInfoEnabled()) {
                                        log.info("Scaled source channel for source: {}", sourceName);
                                    }
                                } catch (Throwable ex) {
                                    log.error("Failed to stop source channel for source: {}", sourceName, ex);
                                }
                                return bootstrap.getSubTaskCount();
                            }));
        }

        public Map<String, Integer> scalingSinks(int perConcurrency,
                                                 boolean restart,
                                                 long perRestartTimeout,
                                                 String... channelIds) {
            return safeMap(sinkBootstraps).entrySet().stream()
                    .filter(e -> isEmptyArray(channelIds) || equalsAny(e.getKey(), channelIds))
                    .collect(toMap(
                            Entry::getKey,
                            entry -> {
                                final StreamBootstrap<? extends SinkStream> bootstrap = entry.getValue();
                                final String channelId = bootstrap.getStream().getChannel().getId();
                                try {
                                    if (log.isInfoEnabled()) {
                                        log.info("Scaling sink channel for id: {}", channelId);
                                    }
                                    bootstrap.scaling(perConcurrency, restart, perRestartTimeout);
                                    if (log.isInfoEnabled()) {
                                        log.info("Scaled sink channel for id: {}", channelId);
                                    }
                                } catch (Throwable ex) {
                                    log.error("Failed to scaling sink channel id: {}", channelId, ex);
                                }
                                return bootstrap.getSubTaskCount();
                            }));
        }

    }

    @Getter
    @AllArgsConstructor
    public static abstract class StreamBootstrap<S extends AbstractStream> {
        private final S stream;
        private final Object internalTask;

        public abstract void start();

        public boolean stop(long timeoutMs) throws Exception {
            return stop(timeoutMs, false);
        }

        public abstract boolean stop(long timeoutMs,
                                     boolean force) throws Exception;

        public abstract boolean scaling(int concurrency,
                                        boolean restart,
                                        long restartTimeout) throws Exception;

        public abstract void pause();

        public abstract void resume();

        public abstract boolean isRunning();

        public abstract boolean isHealthy();

        public abstract int getSubTaskCount();

    }

    @Getter
    @AllArgsConstructor
    public static class ContainerTaskStatus {
        private boolean running;
        private int actuateConcurrency;
    }

}
