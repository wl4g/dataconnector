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
import com.wl4g.kafkasubscriber.config.SubscribeConfiguration;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import lombok.Getter;

import javax.validation.constraints.NotBlank;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link CachingSubscriberRegistry}
 *
 * @author James Wong
 * @since v1.0
 **/
public class CachingSubscriberRegistry {

    private final @Getter SubscribeConfiguration config;
    private final @Getter SubscribeEngineCustomizer customizer;
    private final Map<String, Map<String, SubscriberInfo>> registry;

    public CachingSubscriberRegistry(SubscribeConfiguration config,
                                     SubscribeEngineCustomizer customizer) {
        this.config = Assert2.notNullOf(config, "config");
        this.customizer = Assert2.notNullOf(customizer, "customizer");
        this.registry = new ConcurrentHashMap<>(2);
    }

    public SubscriberInfo get(@NotBlank String pipelineName,
                              String subscriberId) {
        return obtainWithPipeline(pipelineName).get(subscriberId);
    }

    public Collection<SubscriberInfo> getSubscribers(@NotBlank String pipelineName) {
        return obtainWithPipeline(pipelineName).values();
    }

    public void putAll(@NotBlank String pipelineName, Collection<SubscriberInfo> subscribers) {
        obtainWithPipeline(pipelineName).putAll(safeList(subscribers)
                .stream()
                .map(SubscriberInfo::validate)
                .collect(toMap(SubscriberInfo::getId, s -> s)));
    }

    public boolean remove(@NotBlank String pipelineName) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        return Objects.nonNull(registry.remove(pipelineName));
    }

    public boolean remove(@NotBlank String pipelineName, @NotBlank String subscriberId) {
        Assert2.hasTextOf(subscriberId, "subscriberId");
        return Objects.nonNull(obtainWithPipeline(pipelineName).remove(subscriberId));
    }

    public void clear(@NotBlank String pipelineName) {
        obtainWithPipeline(pipelineName).clear();
    }

    public void clear() {
        registry.clear();
    }

    public int size(@NotBlank String pipelineName) {
        return obtainWithPipeline(pipelineName).size();
    }

    public int size() {
        return registry.size();
    }

    private Map<String, SubscriberInfo> obtainWithPipeline(@NotBlank String pipelineName) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        return registry.computeIfAbsent(pipelineName, k -> new ConcurrentHashMap<>(16));
    }

}
