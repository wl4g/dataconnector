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

package com.wl4g.dataconnector.coordinator;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.stream.dispatch.ComplexProcessChain;
import com.wl4g.dataconnector.stream.dispatch.ComplexProcessHandler;
import com.wl4g.dataconnector.stream.dispatch.filter.IProcessFilter.ProcessFilterProvider;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper.ProcessMapperProvider;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link CachingChannelRegistry}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class CachingChannelRegistry {

    private final @Getter DataConnectorConfiguration config;
    private final Map<String, Map<String, ChannelInfoWrapper>> registry;

    public CachingChannelRegistry(DataConnectorConfiguration config) {
        this.config = Assert2.notNullOf(config, "config");
        this.registry = new ConcurrentHashMap<>(2);
    }

    public Map<String, Map<String, ChannelInfoWrapper>> getRegistry() {
        return unmodifiableMap(registry)
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, e -> unmodifiableMap(e.getValue())));
    }

    public ChannelInfoWrapper get(@NotBlank String connectorName,
                                  @NotBlank String channelId) {
        Assert2.hasTextOf(channelId, "channelId");
        return obtainChannels(connectorName).get(channelId);
    }

    public Collection<ChannelInfoWrapper> getAssignedChannels(@NotBlank String connectorName) {
        return obtainChannels(connectorName).values();
    }

    public void assign(@NotBlank String connectorName, Collection<ChannelInfo> channels) {
        obtainChannels(connectorName).putAll(safeList(channels)
                .stream()
                .map(ChannelInfo::validate)
                .map(c -> new ChannelInfoWrapper(createChannelProcessChains(connectorName, c), c))
                .collect(toMap(ChannelInfo::getId, s -> s)));
    }

    public void unAssign(@NotBlank String connectorName) {
        Assert2.hasTextOf(connectorName, "connectorName");
        final Map<String, ChannelInfoWrapper> remove = registry.remove(connectorName);
        if (nonNull(remove)) {
            remove.clear();
        }
    }

    public void unAssign(@NotBlank String connectorName, @NotBlank String channelId) {
        Assert2.hasTextOf(channelId, "channelId");
        obtainChannels(connectorName).remove(channelId);
    }

    @SuppressWarnings("unused")
    public void clear(@NotBlank String connectorName) {
        obtainChannels(connectorName).clear();
    }

    public void clear() {
        registry.clear();
    }

    public int size(@NotBlank String connectorName) {
        return obtainChannels(connectorName).size();
    }

    public int size() {
        return registry.size();
    }

    private List<ComplexProcessChain> createChannelProcessChains(@NotBlank String connectorName, @NotNull ChannelInfo channel) {
        Assert2.hasTextOf(connectorName, "connectorName");
        requireNonNull(channel, "channel must not be null");

        final ConnectorConfig connectorConfig = config.getConnectorMap().get(connectorName);
        if (isNull(connectorConfig)) {
            throw new DataConnectorException(String.format("Could not create channel process chain, because not found connector config of '%s'",
                    connectorName));
        }

        // Construct to connector process chains(merge to filters/mappers).
        return safeList(channel.getSettingsSpec().getPolicySpec().getRules())
                .stream()
                .map(r -> new ComplexProcessChain(safeList(r.getChain())
                        .stream()
                        .map(ri -> {
                            final ProcessFilterProvider pfp = config.getDefinitions().getFilterMap().get(ri.getName());
                            if (nonNull(pfp)) {
                                return pfp.create(config, channel, ri);
                            }
                            final ProcessMapperProvider pmp = config.getDefinitions().getMapperMap().get(ri.getName());
                            if (nonNull(pmp)) {
                                return (ComplexProcessHandler) pmp.create(config, channel, ri);
                            }
                            log.warn("Unable to getting filter/mapper process provider definition '{}' of channelId '{}'",
                                    ri.getName(), channel.getId());
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .toArray(ComplexProcessHandler[]::new)))
                .collect(toList());
    }

    private Map<String, ChannelInfoWrapper> obtainChannels(@NotBlank String connectorName) {
        Assert2.hasTextOf(connectorName, "connectorName");
        return registry.computeIfAbsent(connectorName, k -> new ConcurrentHashMap<>(16));
    }

    @Getter
    public static class ChannelInfoWrapper extends ChannelInfo {
        private final List<ComplexProcessChain> chains;

        public ChannelInfoWrapper(@NotNull List<ComplexProcessChain> chains,
                                  @NotNull ChannelInfo channel) {
            requireNonNull(channel, "channel must not be null");
            this.chains = requireNonNull(chains, "chain must not be null");
            BeanUtils.copyProperties(channel, this);
        }
    }

}
