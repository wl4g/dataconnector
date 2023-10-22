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

package com.wl4g.dataconnector.config.configurator;

import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.ShardingInfo;
import com.wl4g.dataconnector.stream.source.SourceStream;
import com.wl4g.dataconnector.stream.source.SourceStream.SourceStreamConfig;
import com.wl4g.dataconnector.util.Crc32Util;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

/**
 * The {@link DefaultDataConnectorConfigurator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class DefaultDataConnectorConfigurator extends AbstractDataConnectorConfigurator {

    private final List<? extends SourceStreamConfig> staticSources;
    private final List<ChannelInfo> staticChannels;

    public DefaultDataConnectorConfigurator(Environment environment,
                                            DataConnectorConfiguration config,
                                            DataConnectorMeter meter,
                                            List<SourceStreamConfig> staticSources,
                                            List<ChannelInfo> staticChannels) {
        super(environment, config, meter);
        this.staticSources = Assert2.notEmptyOf(staticSources, "staticSources");
        this.staticChannels = Assert2.notEmptyOf(staticChannels, "staticChannels");
    }

    @Override
    public List<? extends SourceStreamConfig> loadSourceConfigs(String connectorName) {
        return safeList(getStaticSources())
                .stream()
                .filter(SourceStreamConfig::isEnable)
                .filter(s -> {
                    final List<String> labels = safeList(s.getLabels());
                    return labels.isEmpty() || labels.stream().anyMatch(l -> StringUtils.equals(l, connectorName));
                })
                .collect(toList());
    }

    @Override
    public List<ChannelInfo> loadChannels(@NotBlank String connectorName,
                                          @Nullable ShardingInfo sharding) {
        Predicate<ChannelInfo> predicate = s -> true;
        if (nonNull(sharding)) {
            predicate = s -> safeList(sharding.getItems())
                    .contains(sharding.getTotal() % (int) Crc32Util.compute(s.getId()));
        }
        return safeList(getStaticChannels())
                .stream()
                .filter(ChannelInfo::isEnable)
                .filter(predicate)
                .collect(toList());
    }

    @Getter
    @Setter
    public static class DefaultConfiguratorProvider extends ConfiguratorProvider {
        public static final String TYPE_NAME = "DEFAULT_CONFIGURATOR";
        private volatile IDataConnectorConfigurator SINGLETON;

        private @NotEmpty List<SourceStreamConfig> staticSources = new ArrayList<>(2);
        private @NotEmpty List<ChannelInfo> staticChannels = new ArrayList<>(2);

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public void validate() {
            Assert2.notEmptyOf(staticSources, "staticSources");
            Assert2.notEmptyOf(staticChannels, "staticChannels");
            this.staticSources.forEach(SourceStream.SourceStreamConfig::validate);
            this.staticChannels.forEach(ChannelInfo::validate);
        }

        @Override
        public synchronized IDataConnectorConfigurator obtain(
                Environment environment,
                DataConnectorConfiguration config,
                DataConnectorMeter meter) {
            return isNull(SINGLETON) ? (SINGLETON = new DefaultDataConnectorConfigurator(
                    environment, config, meter, staticSources, staticChannels))
                    : SINGLETON;
        }

    }

}
