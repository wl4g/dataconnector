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
import com.wl4g.streamconnect.config.configurator.DefaultStreamConnectConfigurator;
import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.coordinator.strategy.IShardingStrategy;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.qos.IQoS;
import com.wl4g.streamconnect.stream.process.ProcessStream;
import com.wl4g.streamconnect.stream.process.filter.IProcessFilter;
import com.wl4g.streamconnect.stream.process.map.IProcessMapper;
import com.wl4g.infra.common.lang.Assert2;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * The {@link StreamConnectProperties}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@ToString
public class StreamConnectProperties implements InitializingBean {
    private @Default DefinitionProperties definitions = new DefinitionProperties();
    private @Default IStreamConnectConfigurator.ConfiguratorProvider configurator = new DefaultStreamConnectConfigurator.DefaultConfiguratorProvider();
    private @NotBlank String coordinator;
    private @Default List<ConnectorProperties> connectors = new ArrayList<>(1);

    @Override
    public void afterPropertiesSet() {
        try {
            validate();
        } catch (Throwable ex) {
            throw new StreamConnectException("Failed to validate stream connect properties", ex);
        }
    }

    private void validate() {
        Assert2.notNullOf(definitions, "definitions");
        requireNonNull(configurator, "configurator must not be null");
        Assert2.hasTextOf(coordinator, "coordinator");
        Assert2.notEmptyOf(connectors, "connectors");
        this.definitions.validate();
        this.connectors.forEach(ConnectorProperties::validate);
    }

    // ----- Definitions configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class DefinitionProperties {
        private @Default List<IProcessFilter> filters = new ArrayList<>(2);
        private @Default List<IProcessMapper> mappers = new ArrayList<>(2);
        private @Default List<IQoS> qoss = new ArrayList<>(2);
        private @Default List<ICheckpoint> checkpoints = new ArrayList<>(2);
        private @Default List<IShardingStrategy> shardingStrategies = new ArrayList<>(2);
        private @Default List<IStreamConnectCoordinator.CoordinatorProvider> coordinators = new ArrayList<>(2);

        public void validate() {
            Assert2.notEmptyOf(filters, "filters");
            Assert2.notEmptyOf(mappers, "mappers");
            Assert2.notEmptyOf(qoss, "qoss");
            Assert2.notEmptyOf(checkpoints, "checkpoints");
            Assert2.notEmptyOf(shardingStrategies, "shardingStrategies");
            Assert2.notEmptyOf(coordinators, "coordinators");

            // Check for filters name duplicate.
            Assert2.isTrueOf(filters.size() == new HashSet<>(filters.stream()
                    .map(IProcessFilter::getName).collect(toList())).size(), "filters name duplicate");
            // Check for mappers name duplicate.
            Assert2.isTrueOf(mappers.size() == new HashSet<>(mappers.stream()
                    .map(IProcessMapper::getName).collect(toList())).size(), "mappers name duplicate");
            // Check for qoss name duplicate.
            Assert2.isTrueOf(qoss.size() == new HashSet<>(qoss.stream()
                    .map(IQoS::getName).collect(toList())).size(), "qoss name duplicate");
            // Check for checkpoints name duplicate.
            Assert2.isTrueOf(checkpoints.size() == new HashSet<>(checkpoints.stream()
                    .map(ICheckpoint::getName).collect(toList())).size(), "checkpoints name duplicate");
            // Check for checkpoints name duplicate.
            Assert2.isTrueOf(shardingStrategies.size() == new HashSet<>(shardingStrategies.stream()
                    .map(IShardingStrategy::getName).collect(toList())).size(), "shardingStrategies name duplicate");
            // Check for checkpoints name duplicate.
            Assert2.isTrueOf(coordinators.size() == new HashSet<>(coordinators.stream()
                    .map(IStreamConnectCoordinator.CoordinatorProvider::getName).collect(toList())).size(), "coordinators name duplicate");
        }
    }

    // ----- Connectors configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class ConnectorProperties {
        private String name;
        private @Builder.Default boolean enable = true;
        private @NotEmpty List<String> processes;
        @NotNull
        private @Default ProcessStream.ProcessStreamConfig executor = new ProcessStream.ProcessStreamConfig();
        private @NotBlank String qos;
        private @NotBlank String checkpoint;

        public void validate() {
            Assert2.hasTextOf(name, "name");
            Assert2.notEmptyOf(processes, "processes");
            requireNonNull(executor, "props must not be null");
            Assert2.hasTextOf(qos, "qos");
            Assert2.hasTextOf(checkpoint, "checkpoint");
            executor.validate();
        }
    }

}
