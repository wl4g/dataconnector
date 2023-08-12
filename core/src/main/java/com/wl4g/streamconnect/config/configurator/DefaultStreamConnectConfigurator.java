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

package com.wl4g.streamconnect.config.configurator;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.source.SourceStream.SourceStreamConfig;
import com.wl4g.streamconnect.util.Crc32Util;
import lombok.Getter;
import lombok.Setter;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Null;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

/**
 * The {@link DefaultStreamConnectConfigurator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class DefaultStreamConnectConfigurator extends AbstractStreamConnectConfigurator {

    private final List<? extends SourceStreamConfig> staticSources;
    private final List<ChannelInfo> staticChannels;

    public DefaultStreamConnectConfigurator(Environment environment,
                                            StreamConnectConfiguration config,
                                            StreamConnectMeter meter,
                                            List<SourceStreamConfig> staticSources,
                                            List<ChannelInfo> staticChannels) {
        super(environment, config, meter);
        this.staticSources = Assert2.notEmptyOf(staticSources, "staticSources");
        this.staticChannels = Assert2.notEmptyOf(staticChannels, "staticChannels");
    }

    @Override
    public List<? extends SourceStreamConfig> loadSourceConfigs(String connectorName) {
        return getStaticSources();
    }

    @Override
    public List<ChannelInfo> loadChannels(@NotBlank String connectorName,
                                          @Null IStreamConnectCoordinator.ShardingInfo sharding) {
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
        private static IStreamConnectConfigurator SINGLETON;

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
            this.staticSources.forEach(SourceStreamConfig::validate);
            this.staticChannels.forEach(ChannelInfo::validate);
        }

        @Override
        public synchronized IStreamConnectConfigurator obtain(
                Environment environment,
                StreamConnectConfiguration config,
                StreamConnectMeter meter) {
            return isNull(SINGLETON) ? (SINGLETON = new DefaultStreamConnectConfigurator(
                    environment, config, meter, staticSources, staticChannels))
                    : SINGLETON;
        }

    }

}
