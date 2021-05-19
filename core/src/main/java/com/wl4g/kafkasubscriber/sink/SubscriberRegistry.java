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

package com.wl4g.kafkasubscriber.sink;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link SubscriberRegistry}
 *
 * @author James Wong
 * @since v1.0
 **/
public class SubscriberRegistry {

    private final @Getter KafkaSubscriberProperties config;
    private final Map<Long, SubscriberInfo> registry;

    public SubscriberRegistry(KafkaSubscriberProperties config) {
        this.config = Assert2.notNullOf(config, "config");
        this.registry = new ConcurrentHashMap<>(16);
    }

    public SubscriberInfo get(Long id) {
        return registry.get(id);
    }

    public List<SubscriberInfo> getAll() {
        return (List<SubscriberInfo>) registry.values();
    }

    public void put(Long id, SubscriberInfo subscriber) {
        registry.put(id, subscriber);
    }

    public void putAll(Map<Long, SubscriberInfo> subscribers) {
        registry.putAll(subscribers);
    }

    public void putAllIfAbsent(Map<Long, SubscriberInfo> subscribers) {
        subscribers.forEach(registry::putIfAbsent);
    }

    public void remove(Long id) {
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
