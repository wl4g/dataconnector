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

package com.wl4g.kafkasubscriber.coordinator;

import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.coordinator.strategy.IShardingStrategy;
import com.wl4g.kafkasubscriber.coordinator.strategy.ShardingStrategyFactory;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineFacade;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link AbstractSubscribeCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public abstract class AbstractSubscribeCoordinator implements ISubscribeCoordinator {

    private final AtomicBoolean running = new AtomicBoolean(false);

    private @Autowired KafkaSubscribeConfiguration config;
    private @Autowired SubscribeEngineCustomizer customizer;
    private @Autowired CachingSubscriberRegistry registry;

    private Thread thread;

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Subscribe coordinator initializing ...");
            init();
            log.info("Subscribe coordinator starting ...");
            this.thread = new Thread(this, getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Subscribe coordinator stopping ...");
            this.thread.interrupt();
            this.thread = null;
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

    protected void updateSubscribers(List<ServiceInstance> instances) {
        // Find the self service instance.
        final ServiceInstance self = instances.stream()
                .filter(ServiceInstance::getSelfInstance)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No self service instance found."));

        // TODO using configurable sharding type
        final IShardingStrategy strategy = ShardingStrategyFactory.getStrategy("AVG_SHARDING");

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
            registry.putAll(pipeline.getName(), assignedSubscribers
                    .stream()
                    .collect(toMap(SubscriberInfo::getId, s -> s)));
        });

    }

}
