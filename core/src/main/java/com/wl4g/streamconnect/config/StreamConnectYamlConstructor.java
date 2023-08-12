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

package com.wl4g.streamconnect.config;

import com.wl4g.streamconnect.checkpoint.ICheckpoint;
import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.coordinator.strategy.IShardingStrategy;
import com.wl4g.streamconnect.framework.StreamConnectSpiFactory;
import com.wl4g.streamconnect.qos.IQoS;
import com.wl4g.streamconnect.stream.process.filter.IProcessFilter;
import com.wl4g.streamconnect.stream.process.map.IProcessMapper;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import com.wl4g.streamconnect.stream.source.SourceStream;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.Constructor;

import static com.wl4g.infra.common.lang.Assert2.notNullOf;

/**
 * {@link StreamConnectYamlConstructor}
 *
 * @author James Wong
 * @since v1.0
 */
public class StreamConnectYamlConstructor extends Constructor {

    public static void configure(BaseConstructor constructor) {
        notNullOf(constructor, "constructor");

        // Register to configurator providers.
        for (IStreamConnectConfigurator.ConfiguratorProvider provider : StreamConnectSpiFactory.load(IStreamConnectConfigurator.ConfiguratorProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to source stream configs.
        for (SourceStream.SourceStreamConfig filter : StreamConnectSpiFactory.load(SourceStream.SourceStreamConfig.class)) {
            constructor.addTypeDescription(new TypeDescription(filter.getClass(), "!".concat(filter.getType())));
        }

        // Register to source stream providers.
        for (SourceStream.SourceStreamProvider filter : StreamConnectSpiFactory.load(SourceStream.SourceStreamProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(filter.getClass(), "!".concat(filter.getType())));
        }

        // Register to process filter providers.
        for (IProcessFilter.ProcessFilterProvider provider : StreamConnectSpiFactory.load(IProcessFilter.ProcessFilterProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to process mapper providers.
        for (IProcessMapper.ProcessMapperProvider provider : StreamConnectSpiFactory.load(IProcessMapper.ProcessMapperProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }

        // Register to sink stream configs.
        for (SinkStream.SinkStreamConfig filter : StreamConnectSpiFactory.load(SinkStream.SinkStreamConfig.class)) {
            constructor.addTypeDescription(new TypeDescription(filter.getClass(), "!".concat(filter.getType())));
        }

        // Register to sink stream providers.
        for (SinkStream.SinkStreamProvider filter : StreamConnectSpiFactory.load(SinkStream.SinkStreamProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(filter.getClass(), "!".concat(filter.getType())));
        }

        // Register to qos.
        for (IQoS qos : StreamConnectSpiFactory.load(IQoS.class)) {
            constructor.addTypeDescription(new TypeDescription(qos.getClass(), "!".concat(qos.getType())));
        }

        // Register to checkpoint.
        for (ICheckpoint checkpoint : StreamConnectSpiFactory.load(ICheckpoint.class)) {
            constructor.addTypeDescription(new TypeDescription(checkpoint.getClass(), "!".concat(checkpoint.getType())));
        }

        // Register to coordinator.
        for (IStreamConnectCoordinator.CoordinatorProvider coordinator : StreamConnectSpiFactory.load(IStreamConnectCoordinator.CoordinatorProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(coordinator.getClass(), "!".concat(coordinator.getType())));
        }

        // Register to coordinator sharding strategy.
        for (IShardingStrategy strategy : StreamConnectSpiFactory.load(IShardingStrategy.class)) {
            constructor.addTypeDescription(new TypeDescription(strategy.getClass(), "!".concat(strategy.getType())));
        }
    }

}