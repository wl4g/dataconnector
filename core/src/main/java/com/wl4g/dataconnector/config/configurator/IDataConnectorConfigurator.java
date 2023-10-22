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

import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.ShardingInfo;
import com.wl4g.dataconnector.framework.IDataConnectorSpi;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import com.wl4g.dataconnector.stream.source.SourceStream.SourceStreamConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotBlank;
import javax.annotation.Nullable;
import java.util.List;

/**
 * The {@link IDataConnectorConfigurator}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface IDataConnectorConfigurator {

    List<? extends SourceStreamConfig> loadSourceConfigs(@NotBlank String connectorName);

    List<ChannelInfo> loadChannels(@NotBlank String connectorName,
                                   @Nullable ShardingInfo sharding);

    // ----- Configurator provider. -----

    @Getter
    @Setter
    @NoArgsConstructor
    abstract class ConfiguratorProvider implements IDataConnectorSpi {
        public abstract IDataConnectorConfigurator obtain(Environment environment,
                                                          DataConnectorConfiguration config,
                                                          DataConnectorMeter meter);
    }

}
