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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.dataconnector.checkpoint.AbstractCheckpoint;
import com.wl4g.dataconnector.checkpoint.ICheckpoint;
import com.wl4g.dataconnector.config.DataConnectorProperties.ConnectorProperties;
import com.wl4g.dataconnector.config.DataConnectorProperties.DefinitionProperties;
import com.wl4g.dataconnector.config.configurator.IDataConnectorConfigurator;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.CoordinatorProvider;
import com.wl4g.dataconnector.coordinator.strategy.IShardingStrategy;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import com.wl4g.dataconnector.qos.IQoS;
import com.wl4g.dataconnector.stream.dispatch.DispatchStream;
import com.wl4g.dataconnector.stream.dispatch.filter.IProcessFilter.ProcessFilterProvider;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper.ProcessMapperProvider;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.lang.Assert2.hasTextOf;
import static com.wl4g.infra.common.lang.Assert2.notEmptyOf;
import static com.wl4g.infra.common.lang.Assert2.notNullOf;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link DataConnectorConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
public class DataConnectorConfiguration {

    private final Environment environment;
    private final DataConnectorProperties properties;
    private final IDataConnectorConfigurator configurator;
    private final IDataConnectorCoordinator.CoordinatorProvider coordinatorProvider;
    private final DefinitionsConfig definitions;
    private final Map<String, ConnectorConfig> connectorMap;
    private final ApplicationEventPublisher eventPublisher;
    private final DataConnectorMeter meter;

    public DataConnectorConfiguration(@NotNull Environment environment,
                                      @NotNull DataConnectorProperties properties,
                                      @NotNull ApplicationEventPublisher eventPublisher,
                                      @NotNull DataConnectorMeter meter) {
        this.environment = requireNonNull(environment, "environment must not be null");
        this.properties = requireNonNull(properties, "properties must not be null");
        this.eventPublisher = requireNonNull(eventPublisher, "eventPublisher must not be null");
        this.meter = requireNonNull(meter, "meter must not be null");

        // Setup to configurator.
        properties.getConfigurator().validate();
        this.configurator = properties.getConfigurator().obtain(environment, this, meter);

        // Parse to actuate definitions config.
        this.definitions = parseDefinitionsConfig(properties);

        // Parse to actuate source,filter,sink
        this.connectorMap = safeList(properties.getConnectors())
                .stream()
                .map(this::parseConnectorConfig)
                .collect(toMap(ConnectorConfig::getName, e -> e));

        // Setup to coordinator provider.
        this.coordinatorProvider = this.definitions.getCoordinatorProviderMap().get(properties.getCoordinator());

        this.validate();
    }

    public DataConnectorConfiguration validate() {
        requireNonNull(configurator, "configurator must not be null");

        requireNonNull(definitions, "definitions must not be null");
        definitions.validate();

        notEmptyOf(connectorMap, "connectorMap");
        safeMap(connectorMap).values().forEach(ConnectorConfig::validate);

        // Check for connector refs processes name duplicate.
        safeMap(connectorMap).values()
                .stream()
                .collect(toMap(e -> e, e -> 1, Integer::sum))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .findFirst()
                .ifPresent(e -> {
                    throw new DataConnectorException(String.format("Found connector refs Processes name duplicate '%s'", e.getKey()));
                });

        // Check for connector refs qoss name duplicate.
        safeMap(connectorMap).values()
                .stream()
                .map(ConnectorConfig::getQos)
                .map(IQoS::getName)
                .collect(toMap(e -> e, e -> 1, Integer::sum))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .findFirst()
                .ifPresent(e -> {
                    throw new DataConnectorException(String.format("Found connector refs QoS name duplicate '%s'", e.getKey()));
                });

        // Check for connector refs checkpoints name duplicate.
        safeList(properties.getConnectors())
                .stream()
                .map(DataConnectorProperties.ConnectorProperties::getCheckpoint)
                .collect(toMap(e -> e, e -> 1, Integer::sum))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .findFirst()
                .ifPresent(e -> {
                    throw new DataConnectorException(String.format("Found connector refs Checkpoint name duplicate '%s'", e.getKey()));
                });

        requireNonNull(coordinatorProvider, "coordinatorProvider must not be null");
        coordinatorProvider.validate();

        return this;
    }

    private DefinitionsConfig parseDefinitionsConfig(@NotNull DataConnectorProperties properties) {
        final DefinitionProperties definitionProps = properties.getDefinitions();
        final DefinitionsConfig definitions = new DefinitionsConfig();

        // Parse to filters.
        final Map<String, ProcessFilterProvider> filterMap = safeList(definitionProps.getFilters())
                .stream()
                .collect(toMap(ProcessFilterProvider::getName, e -> e));
        definitions.setFilterMap(filterMap);

        // Parse to mappers.
        final Map<String, ProcessMapperProvider> mapperMap = safeList(definitionProps.getMappers())
                .stream()
                .collect(toMap(ProcessMapperProvider::getName, e -> e));
        definitions.setMapperMap(mapperMap);

        // Parse to qoss.
        final Map<String, IQoS> qosMap = safeList(definitionProps.getQoss())
                .stream()
                .collect(toMap(IQoS::getName, e -> e));
        definitions.setQoSMap(qosMap);

        // Parse to shardingStrategies.
        final Map<String, IShardingStrategy> shardingStrategyMap = safeList(definitionProps.getShardingStrategies())
                .stream()
                .collect(toMap(IShardingStrategy::getName, e -> e));
        definitions.setShardingStrategyMap(shardingStrategyMap);

        // Init to checkpoint providers.
        final Map<String, ICheckpoint> checkpointMap =
                safeList(definitionProps.getCheckpoints())
                        .stream()
                        .peek(checkpoint -> {
                            final AbstractCheckpoint ckp = ((AbstractCheckpoint) checkpoint);
                            ckp.setConfig(this);
                            ckp.init();
                        })
                        .collect(toMap(ICheckpoint::getName, e -> e));
        definitions.setCheckpointMap(checkpointMap);

        // Init to checkpoint providers.
        final Map<String, CoordinatorProvider> coordinatorProviderMap =
                safeList(definitionProps.getCoordinators())
                        .stream()
                        .collect(toMap(CoordinatorProvider::getName, e -> e));
        definitions.setCoordinatorProviderMap(coordinatorProviderMap);

        return definitions;
    }

    private ConnectorConfig parseConnectorConfig(@NotNull ConnectorProperties connectorProps) {
        Assert2.notNullOf(connectorProps, "connectorProps");

        final ConnectorConfig connectorConfig = new ConnectorConfig();
        connectorConfig.setName(connectorProps.getName());
        connectorConfig.setEnable(connectorProps.isEnable());

        // Setup to connector qos.
        final IQoS qos = definitions.getQoSMap().get(connectorProps.getQos());
        requireNonNull(qos, String.format("Not found the qos '%s'", connectorProps.getQos()));
        connectorConfig.setQos(qos);

        // Setup to connector checkpoint.
        final ICheckpoint checkpoint = definitions.getCheckpointMap().get(connectorProps.getCheckpoint());
        requireNonNull(checkpoint, String.format("Not found the checkpoint '%s'", connectorProps.getCheckpoint()));
        connectorConfig.setCheckpoint(checkpoint);

        //connectorConfig.setSourceConfigs(safeList(connectorProps.getSources()));
        connectorConfig.setDispatchConfig(connectorProps.getDispatcher());
        return connectorConfig;
    }

    public ConnectorConfig getConnectorConfig(@NotBlank String connectorName, boolean required) {
        Assert2.hasTextOf(connectorName, "connectorName");
        final ConnectorConfig connectorConfig = safeMap(getConnectorMap()).get(connectorName);
        if (required && isNull(connectorConfig)) {
            throw new DataConnectorException(String.format("Not found connector '%s'", connectorName));
        }
        return connectorConfig;
    }

    // ----- Actual configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class DefinitionsConfig {
        private @NotNull Map<String, ProcessFilterProvider> filterMap;
        private @Nullable Map<String, ProcessMapperProvider> mapperMap;
        private @NotEmpty Map<String, IQoS> qoSMap;
        private @NotEmpty Map<String, ICheckpoint> checkpointMap;
        private @NotEmpty Map<String, IShardingStrategy> shardingStrategyMap;
        private @NotEmpty Map<String, CoordinatorProvider> coordinatorProviderMap;

        public void validate() {
            notEmptyOf(filterMap, "filterMap");
            notEmptyOf(qoSMap, "qoSMap");
            notEmptyOf(checkpointMap, "checkpointMap");
            notEmptyOf(shardingStrategyMap, "shardingStrategyMap");
            this.filterMap.values().forEach(ProcessFilterProvider::validate);
            safeMap(this.mapperMap).values().forEach(ProcessMapperProvider::validate);
            this.qoSMap.values().forEach(IQoS::validate);
            this.checkpointMap.values().forEach(ICheckpoint::validate);
            this.shardingStrategyMap.values().forEach(IShardingStrategy::validate);
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class ConnectorConfig {
        private @NotBlank String name;
        private boolean enable;
        private @NotNull IQoS qos;
        private @NotNull ICheckpoint checkpoint;
        //private @NotEmpty List<SourceStreamConfig> sourceConfigs;
        private @NotNull DispatchStream.DispatchStreamConfig dispatchConfig;

        public void validate() {
            hasTextOf(name, "name");
            notNullOf(qos, "qos");
            notNullOf(checkpoint, "checkpoint");
            //notEmptyOf(sourceConfigs, "sourceConfigs");
            notNullOf(dispatchConfig, "dispatchConfig");
        }
    }

}
