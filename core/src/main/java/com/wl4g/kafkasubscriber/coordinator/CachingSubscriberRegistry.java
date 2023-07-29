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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineCustomizer;
import lombok.Getter;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link CachingSubscriberRegistry}
 *
 * @author James Wong
 * @since v1.0
 **/
public class CachingSubscriberRegistry {

    private final @Getter KafkaSubscriberProperties config;
    private final @Getter SubscribeEngineCustomizer customizer;
    private final Map<String, SubscriberInfo> registry;

    public CachingSubscriberRegistry(KafkaSubscriberProperties config,
                                     SubscribeEngineCustomizer customizer) {
        this.config = Assert2.notNullOf(config, "config");
        this.customizer = Assert2.notNullOf(customizer, "customizer");
        this.registry = new ConcurrentHashMap<>(16);
    }

    public SubscriberInfo get(String id) {
        return registry.get(id);
    }

    public Collection<SubscriberInfo> getShardingAll() {
        // TODO
//        return registry.values();
        return customizer.loadSubscribers(new SubscriberInfo());
    }

    public void put(String id, SubscriberInfo subscriber) {
        registry.put(id, subscriber);
    }

    public void putAll(Map<String, SubscriberInfo> subscribers) {
        registry.putAll(subscribers);
    }

    public void putAllIfAbsent(Map<String, SubscriberInfo> subscribers) {
        subscribers.forEach(registry::putIfAbsent);
    }

    public void remove(String id) {
        registry.remove(id);
    }

    public void clear() {
        registry.clear();
    }

    public int size() {
        return registry.size();
    }

    public boolean isEmpty() {
        return registry.isEmpty();
    }


}
