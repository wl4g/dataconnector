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

package com.wl4g.dataconnector.config;

import com.wl4g.dataconnector.checkpoint.ICheckpoint;
import com.wl4g.dataconnector.config.configurator.IDataConnectorConfigurator.ConfiguratorProvider;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.CoordinatorProvider;
import com.wl4g.dataconnector.coordinator.strategy.IShardingStrategy;
import com.wl4g.dataconnector.framework.DataConnectorSpiFactory;
import com.wl4g.dataconnector.qos.IQoS;
import com.wl4g.dataconnector.stream.dispatch.filter.IProcessFilter.ProcessFilterProvider;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper.ProcessMapperProvider;
import com.wl4g.dataconnector.stream.sink.SinkStream.SinkStreamConfig;
import com.wl4g.dataconnector.stream.sink.SinkStream.SinkStreamProvider;
import com.wl4g.dataconnector.stream.source.SourceStream.SourceStreamConfig;
import com.wl4g.dataconnector.stream.source.SourceStream.SourceStreamProvider;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.Constructor;

import static com.wl4g.infra.common.lang.Assert2.notNullOf;

/**
 * {@link DataConnectorYamlConstructor}
 *
 * @author James Wong
 * @since v1.0
 */
public class DataConnectorYamlConstructor extends Constructor {

    public static void configure(BaseConstructor constructor) {
        notNullOf(constructor, "constructor");

        // Register to configurator providers.
        for (ConfiguratorProvider provider : DataConnectorSpiFactory.load(ConfiguratorProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to source stream configs.
        for (SourceStreamConfig config : DataConnectorSpiFactory.load(SourceStreamConfig.class)) {
            constructor.addTypeDescription(new TypeDescription(config.getClass(), "!".concat(config.getType())));
        }

        // Register to source stream providers.
        for (SourceStreamProvider provider : DataConnectorSpiFactory.load(SourceStreamProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to process filter providers.
        for (ProcessFilterProvider provider : DataConnectorSpiFactory.load(ProcessFilterProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to process mapper providers.
        for (ProcessMapperProvider provider : DataConnectorSpiFactory.load(ProcessMapperProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to sink stream configs.
        for (SinkStreamConfig config : DataConnectorSpiFactory.load(SinkStreamConfig.class)) {
            constructor.addTypeDescription(new TypeDescription(config.getClass(), "!".concat(config.getType())));
        }

        // Register to sink stream providers.
        for (SinkStreamProvider provider : DataConnectorSpiFactory.load(SinkStreamProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to qos.
        for (IQoS qos : DataConnectorSpiFactory.load(IQoS.class)) {
            constructor.addTypeDescription(new TypeDescription(qos.getClass(), "!".concat(qos.getType())));
        }

        // Register to checkpoint.
        for (ICheckpoint checkpoint : DataConnectorSpiFactory.load(ICheckpoint.class)) {
            constructor.addTypeDescription(new TypeDescription(checkpoint.getClass(), "!".concat(checkpoint.getType())));
        }

        // Register to coordinator.
        for (CoordinatorProvider provider : DataConnectorSpiFactory.load(CoordinatorProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to coordinator sharding strategy.
        for (IShardingStrategy strategy : DataConnectorSpiFactory.load(IShardingStrategy.class)) {
            constructor.addTypeDescription(new TypeDescription(strategy.getClass(), "!".concat(strategy.getType())));
        }
    }

}