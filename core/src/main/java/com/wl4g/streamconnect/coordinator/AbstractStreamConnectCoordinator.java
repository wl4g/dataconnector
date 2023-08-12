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

package com.wl4g.streamconnect.coordinator;

import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.coordinator.strategy.IShardingStrategy;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.exception.NoFoundChannelException;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.util.concurrent.NamedThreadFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * The {@link AbstractStreamConnectCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
public abstract class AbstractStreamConnectCoordinator implements IStreamConnectCoordinator {
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final Environment environment;
    private final StreamConnectConfiguration config;
    private final IStreamConnectConfigurator configurator;
    private final CachingChannelRegistry registry;
    private final StreamConnectMeter meter;

    private Thread daemon;
    private ExecutorService discoveryExecutor;
    private ExecutorService busEventExecutor;

    protected AbstractStreamConnectCoordinator(@NotNull Environment environment,
                                               @NotNull StreamConnectConfiguration config,
                                               @NotNull IStreamConnectConfigurator configurator,
                                               @NotNull CachingChannelRegistry registry,
                                               @NotNull StreamConnectMeter meter) {
        this.environment = requireNonNull(environment, "environment must not be null");
        this.config = requireNonNull(config, "config must not be null");
        this.configurator = requireNonNull(configurator, "configurator must not be null");
        this.registry = requireNonNull(registry, "registry must not be null");
        this.meter = requireNonNull(meter, "meter must not be null");
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            if (log.isInfoEnabled()) {
                log.info("Starting coordinator ...");
            }
            this.daemon = new Thread(this, getClass().getSimpleName());

            final int poolSize = config.getConnectorMap().size();
            this.discoveryExecutor = new ThreadPoolExecutor(poolSize, poolSize,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(4),
                    new NamedThreadFactory("discovery-executor"),
                    new ThreadPoolExecutor.AbortPolicy());

            this.busEventExecutor = new ThreadPoolExecutor(poolSize, poolSize,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(4),
                    new NamedThreadFactory("busEvent-executor"),
                    new ThreadPoolExecutor.AbortPolicy());

            this.daemon.start();

            if (log.isInfoEnabled()) {
                log.info("Started coordinator.");
            }
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Coordinator stopping ...");
            this.discoveryExecutor.shutdown();
            this.discoveryExecutor = null;
            this.busEventExecutor.shutdown();
            this.busEventExecutor = null;
            this.daemon.interrupt();
            this.daemon = null;
            log.info("Stopped coordinator.");
        }
    }

    @Override
    public void run() {
        if (running.get()) {
            try {
                if (log.isInfoEnabled()) {
                    log.info("Starting coordinator ...");
                }
                doRun();
            } catch (Throwable ex) {
                running.set(false);
                final String errmsg = "Could not start coordinator.";
                log.error(errmsg, ex);
                throw new StreamConnectException(errmsg, ex);
            }
        }
    }

    protected abstract void doRun();

    @Override
    public void onDiscovery(List<ServerInstance> instances) {
        log.info("Discovery of instances: {}", instances);

        meter.counter(MetricsName.coordinator_eventbus_total.getName(),
                MetricsName.coordinator_eventbus_total.getHelp()).increment();

        discoveryExecutor.execute(() -> doUpdateChannels(instances));
    }

    protected void doUpdateChannels(List<ServerInstance> instances) {
        Collections.sort(instances);

        // Find the self service instance.
        final ServerInstance self = instances.stream()
                .filter(ServerInstance::getSelfInstance)
                .findFirst()
                .orElseThrow(() -> new NoFoundChannelException("No self service instance found."));
        if (log.isInfoEnabled()) {
            log.info("Found self server instance: {}", self);
        }

        final IShardingStrategy strategy = ofNullable(config.getDefinitions()
                .getShardingStrategyMap()
                .get(getCoordinatorConfig().getShardingStrategy()))
                .orElseThrow(() -> new StreamConnectException("No sharding strategy found."));

        final int shardingTotal = instances.size();
        final Map<ServerInstance, List<Integer>> sharding = strategy.getShardingItem(shardingTotal, instances);
        final List<Integer> shardingItems = requireNonNull(sharding.get(self), "No sharding items found.");
        if (log.isInfoEnabled()) {
            log.info("Re-balancing channels of sharding items: {} ...", shardingItems);
        }

        safeMap(config.getConnectorMap()).values().forEach(connector -> {
            final List<ChannelInfo> assignedChannels = configurator.loadChannels(connector.getName(),
                    ShardingInfo.builder()
                            .total(shardingTotal)
                            .items(shardingItems)
                            .build());
            if (log.isInfoEnabled()) {
                log.info("Re-balanced channels of connector: {}, sharding: {}, {}, assigned channels: {}",
                        connector.getName(), sharding, assignedChannels.size(), assignedChannels);
            }
            registry.unAssign(connector.getName());
            registry.assign(connector.getName(), assignedChannels);
        });
    }

    @Override
    public void onBusEvent(BusEvent event) {
        meter.counter(MetricsName.coordinator_discovery_total.getName(),
                MetricsName.coordinator_discovery_total.getHelp()).increment();

        busEventExecutor.execute(() -> doUpdateBusEvent(event));
    }

    protected void doUpdateBusEvent(BusEvent event) {
        if (event instanceof AddChannelEvent) {
            if (log.isInfoEnabled()) {
                log.info("Adding channel event: {}", event);
            }
            getRegistry().assign(event.getConnectorName(), safeList(((AddChannelEvent) event)
                    .getChannels()));
        } else if (event instanceof UpdateChannelEvent) {
            if (log.isInfoEnabled()) {
                log.info("Updating channel event: {}", event);
            }
            getRegistry().assign(event.getConnectorName(), safeList(((UpdateChannelEvent) event)
                    .getChannels()));
        } else if (event instanceof RemoveChannelEvent) {
            if (log.isInfoEnabled()) {
                log.info("Removing channel event: {}", event);
            }
            safeList(((RemoveChannelEvent) event).getChannelIds())
                    .forEach(channelId -> {
                        getRegistry().unAssign(event.getConnectorName(), channelId);
                    });
        } else {
            log.warn("Unsupported channel event type of: {}", event);
        }
    }

}
