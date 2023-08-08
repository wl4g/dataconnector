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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.config.StreamConnectProperties;
import com.wl4g.streamconnect.coordinator.StreamConnectEventPublisher.AddSubscribeEvent;
import com.wl4g.streamconnect.coordinator.StreamConnectEventPublisher.RemoveSubscribeEvent;
import com.wl4g.streamconnect.coordinator.StreamConnectEventPublisher.SubscribeEvent;
import com.wl4g.streamconnect.coordinator.StreamConnectEventPublisher.UpdateSubscribeEvent;
import com.wl4g.streamconnect.coordinator.strategy.IShardingStrategy;
import com.wl4g.streamconnect.coordinator.strategy.ShardingStrategyFactory;
import com.wl4g.streamconnect.custom.StreamConnectEngineCustomizer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;

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

    protected final Environment environment;
    protected final StreamConnectProperties config;
    protected final StreamConnectEngineCustomizer customizer;
    protected final CachingSubscriberRegistry registry;

    private Thread daemon;

    public AbstractStreamConnectCoordinator(Environment environment,
                                            StreamConnectProperties config,
                                            StreamConnectEngineCustomizer customizer,
                                            CachingSubscriberRegistry registry) {
        this.environment = Assert2.notNullOf(environment, "environment");
        this.config = Assert2.notNullOf(config, "config");
        this.customizer = Assert2.notNullOf(customizer, "customizer");
        this.registry = Assert2.notNullOf(registry, "registry");
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Subscribe coordinator initializing ...");
            init();
            log.info("Subscribe coordinator starting ...");
            this.daemon = new Thread(this, getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Subscribe coordinator stopping ...");
            this.daemon.interrupt();
            this.daemon = null;
        }
    }

    @Override
    public void run() {
        if (running.get()) {
            doRun();
        }
    }

    protected abstract void init();

    protected abstract void doRun();

    @Override
    public void onDiscovery(List<ServiceInstance> instances) {
        Collections.sort(instances);
        log.info("Discovery of instances: {}", instances);

        updateSubscribers(instances);
    }

    @Override
    public void onBusEvent(SubscribeEvent event) {
        if (event instanceof AddSubscribeEvent) {
            log.info("Adding subscribe event: {}", event);
            getRegistry().putAll(event.getPipelineName(), safeList(((AddSubscribeEvent) event).getSubscribers()));
        } else if (event instanceof UpdateSubscribeEvent) {
            log.info("Updating subscribe event: {}", event);
            getRegistry().putAll(event.getPipelineName(), safeList(((UpdateSubscribeEvent) event).getSubscribers()));
        } else if (event instanceof RemoveSubscribeEvent) {
            log.info("Removing subscribe event: {}", event);
            safeList(((RemoveSubscribeEvent) event).getSubscriberIds()).forEach(subscriberId -> {
                getRegistry().remove(event.getPipelineName(), subscriberId);
            });
        } else {
            log.warn("Unsupported subscribe event type of: {}", event);
        }
    }

    protected void updateSubscribers(List<ServiceInstance> instances) {
        // Find the self service instance.
        final ServiceInstance self = instances.stream()
                .filter(ServiceInstance::getSelfInstance)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No self service instance found."));

        final IShardingStrategy strategy = ShardingStrategyFactory.getStrategy(config.getCoordinator()
                .getShardingStrategy());

        final int shardingTotal = instances.size();
        final Map<ServiceInstance, List<Integer>> sharding = strategy.getShardingItem(shardingTotal, instances);
        final List<Integer> shardingItems = Objects.requireNonNull(sharding.get(self), "No sharding items found.");
        log.info("Re-balancing subscribers of sharding items: {} ...", shardingItems);

        safeList(config.getPipelines()).forEach(pipeline -> {
            final List<SubscriberInfo> assignedSubscribers = customizer.loadSubscribers(pipeline.getName(),
                    ShardingInfo.builder().total(shardingTotal).items(shardingItems).build());

            log.info("Re-balanced subscribers of pipeline: {}, sharding: {}, {}, assigned subscribers: {}",
                    pipeline.getName(), sharding, assignedSubscribers.size(), assignedSubscribers);

            registry.remove(pipeline.getName());
            registry.putAll(pipeline.getName(), assignedSubscribers);
        });

    }

}
