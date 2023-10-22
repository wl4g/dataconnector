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
import com.wl4g.dataconnector.config.configurator.DefaultDataConnectorConfigurator.DefaultConfiguratorProvider;
import com.wl4g.dataconnector.config.configurator.IDataConnectorConfigurator.ConfiguratorProvider;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.CoordinatorProvider;
import com.wl4g.dataconnector.coordinator.strategy.IShardingStrategy;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.qos.IQoS;
import com.wl4g.dataconnector.stream.dispatch.DispatchStream.DispatchStreamConfig;
import com.wl4g.dataconnector.stream.dispatch.filter.IProcessFilter.ProcessFilterProvider;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper.ProcessMapperProvider;
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

import static com.wl4g.infra.common.lang.Assert2.hasTextOf;
import static com.wl4g.infra.common.lang.Assert2.isTrueOf;
import static com.wl4g.infra.common.lang.Assert2.notEmptyOf;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * The {@link DataConnectorProperties}
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
public class DataConnectorProperties implements InitializingBean {
    private @Default DefinitionProperties definitions = new DefinitionProperties();
    private @Default ConfiguratorProvider configurator = new DefaultConfiguratorProvider();
    private @NotBlank String coordinator;
    private @Default List<ConnectorProperties> connectors = new ArrayList<>(1);

    @Override
    public void afterPropertiesSet() {
        try {
            validate();
        } catch (Exception ex) {
            throw new DataConnectorException("Failed to validate stream connect properties", ex);
        }
    }

    private void validate() {
        requireNonNull(definitions, "definitions must not be null");
        requireNonNull(configurator, "configurator must not be null");
        hasTextOf(coordinator, "coordinator");
        notEmptyOf(connectors, "connectors");
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
        private @Default List<ProcessFilterProvider> filters = new ArrayList<>(2);
        private @Default List<ProcessMapperProvider> mappers = new ArrayList<>(2);
        private @Default List<IQoS> qoss = new ArrayList<>(2);
        private @Default List<ICheckpoint> checkpoints = new ArrayList<>(2);
        private @Default List<IShardingStrategy> shardingStrategies = new ArrayList<>(2);
        private @Default List<CoordinatorProvider> coordinators = new ArrayList<>(2);

        public void validate() {
            notEmptyOf(filters, "filters");
            notEmptyOf(mappers, "mappers");
            notEmptyOf(qoss, "qoss");
            notEmptyOf(checkpoints, "checkpoints");
            notEmptyOf(shardingStrategies, "shardingStrategies");
            notEmptyOf(coordinators, "coordinators");

            // Check for filters name duplicate.
            isTrueOf(filters.size() == new HashSet<>(filters.stream()
                    .map(ProcessFilterProvider::getName).collect(toList())).size(), "filters name duplicate");
            // Check for mappers name duplicate.
            isTrueOf(mappers.size() == new HashSet<>(mappers.stream()
                    .map(ProcessMapperProvider::getName).collect(toList())).size(), "mappers name duplicate");
            // Check for qoss name duplicate.
            isTrueOf(qoss.size() == new HashSet<>(qoss.stream()
                    .map(IQoS::getName).collect(toList())).size(), "qoss name duplicate");
            // Check for checkpoints name duplicate.
            isTrueOf(checkpoints.size() == new HashSet<>(checkpoints.stream()
                    .map(ICheckpoint::getName).collect(toList())).size(), "checkpoints name duplicate");
            // Check for checkpoints name duplicate.
            isTrueOf(shardingStrategies.size() == new HashSet<>(shardingStrategies.stream()
                    .map(IShardingStrategy::getName).collect(toList())).size(), "shardingStrategies name duplicate");
            // Check for checkpoints name duplicate.
            isTrueOf(coordinators.size() == new HashSet<>(coordinators.stream()
                    .map(CoordinatorProvider::getName).collect(toList())).size(), "coordinators name duplicate");
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
        private @Default boolean enable = true;
        private @NotEmpty List<String> sources;
        private @NotBlank String qos;
        private @NotBlank String checkpoint;
        @NotNull
        private @Default DispatchStreamConfig dispatcher = new DispatchStreamConfig();

        public void validate() {
            hasTextOf(name, "name");
            notEmptyOf(sources, "sources");
            hasTextOf(qos, "qos");
            hasTextOf(checkpoint, "checkpoint");
            requireNonNull(dispatcher, "dispatcher");
            dispatcher.validate();
        }
    }

}
