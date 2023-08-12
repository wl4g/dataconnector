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
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import lombok.Getter;

import javax.validation.constraints.NotBlank;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link CachingChannelRegistry}
 *
 * @author James Wong
 * @since v1.0
 **/
public class CachingChannelRegistry {

    private final @Getter StreamConnectConfiguration config;
    private final Map<String, Map<String, ChannelInfo>> registry;

    public CachingChannelRegistry(StreamConnectConfiguration config) {
        this.config = Assert2.notNullOf(config, "config");
        this.registry = new ConcurrentHashMap<>(2);
    }

    public Map<String, Map<String, ChannelInfo>> getRegistry() {
        return unmodifiableMap(registry)
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, e -> unmodifiableMap(e.getValue())));
    }

    public ChannelInfo get(@NotBlank String connectorName,
                           @NotBlank String channelId) {
        Assert2.hasTextOf(channelId, "channelId");
        return obtainConnector(connectorName).get(channelId);
    }

    public Collection<ChannelInfo> getAssignedChannels(@NotBlank String connectorName) {
        return obtainConnector(connectorName).values();
    }

    public void assign(@NotBlank String connectorName, Collection<ChannelInfo> channels) {
        obtainConnector(connectorName).putAll(safeList(channels)
                .stream()
                .map(ChannelInfo::validate)
                .collect(toMap(ChannelInfo::getId, s -> s)));
    }

    public void unAssign(@NotBlank String connectorName) {
        Assert2.hasTextOf(connectorName, "connectorName");
        registry.remove(connectorName);
    }

    public void unAssign(@NotBlank String connectorName, @NotBlank String channelId) {
        Assert2.hasTextOf(channelId, "channelId");
        obtainConnector(connectorName).remove(channelId);
    }

    public void clear(@NotBlank String connectorName) {
        obtainConnector(connectorName).clear();
    }

    public void clear() {
        registry.clear();
    }

    public int size(@NotBlank String connectorName) {
        return obtainConnector(connectorName).size();
    }

    public int size() {
        return registry.size();
    }

    private Map<String, ChannelInfo> obtainConnector(@NotBlank String connectorName) {
        Assert2.hasTextOf(connectorName, "connectorName");
        return registry.computeIfAbsent(connectorName, k -> new ConcurrentHashMap<>(16));
    }

}
