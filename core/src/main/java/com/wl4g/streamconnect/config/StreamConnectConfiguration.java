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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.checkpoint.AbstractCheckpoint;
import com.wl4g.streamconnect.checkpoint.ICheckpoint;
import com.wl4g.streamconnect.config.StreamConnectProperties.ConnectorProperties;
import com.wl4g.streamconnect.config.StreamConnectProperties.DefinitionProperties;
import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator.CoordinatorProvider;
import com.wl4g.streamconnect.coordinator.strategy.IShardingStrategy;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.qos.IQoS;
import com.wl4g.streamconnect.stream.process.ComplexProcessChain;
import com.wl4g.streamconnect.stream.process.ComplexProcessHandler;
import com.wl4g.streamconnect.stream.process.ProcessStream.ProcessStreamConfig;
import com.wl4g.streamconnect.stream.process.filter.IProcessFilter;
import com.wl4g.streamconnect.stream.process.map.IProcessMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Map;
import java.util.Optional;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link StreamConnectConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
public class StreamConnectConfiguration {

    private final Environment environment;
    private final StreamConnectProperties properties;
    private final IStreamConnectConfigurator configurator;
    private final CoordinatorProvider coordinatorProvider;
    private final DefinitionsConfig definitions;
    private final Map<String, ConnectorConfig> connectorMap;
    private final ApplicationEventPublisher eventPublisher;
    private final StreamConnectMeter meter;

    public StreamConnectConfiguration(@NotNull Environment environment,
                                      @NotNull StreamConnectProperties properties,
                                      @NotNull ApplicationEventPublisher eventPublisher,
                                      @NotNull StreamConnectMeter meter) {
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

    public StreamConnectConfiguration validate() {
        requireNonNull(configurator, "configurator must not be null");

        requireNonNull(definitions, "definitions must not be null");
        definitions.validate();

        Assert2.notEmptyOf(connectorMap, "connectorMap");
        safeMap(connectorMap).values().forEach(ConnectorConfig::validate);

        // Check for connector refs processes name duplicate.
        safeMap(connectorMap).values()
                .stream()
                .map(ConnectorConfig::getProcessChain)
                .collect(toMap(e -> e, e -> 1, Integer::sum))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .findFirst()
                .ifPresent(e -> {
                    throw new StreamConnectException(String.format("Found connector refs Processes name duplicate '%s'", e.getKey()));
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
                    throw new StreamConnectException(String.format("Found connector refs QoS name duplicate '%s'", e.getKey()));
                });

        // Check for connector refs checkpoints name duplicate.
        safeList(properties.getConnectors())
                .stream()
                .map(ConnectorProperties::getCheckpoint)
                .collect(toMap(e -> e, e -> 1, Integer::sum))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .findFirst()
                .ifPresent(e -> {
                    throw new StreamConnectException(String.format("Found connector refs Checkpoint name duplicate '%s'", e.getKey()));
                });

        requireNonNull(coordinatorProvider, "coordinatorProvider must not be null");
        coordinatorProvider.validate();

        return this;
    }

    private DefinitionsConfig parseDefinitionsConfig(@NotNull StreamConnectProperties properties) {
        final DefinitionProperties definitionProps = properties.getDefinitions();
        final DefinitionsConfig definitions = new DefinitionsConfig();

        // Parse to filters.
        final Map<String, IProcessFilter> filterMap = safeList(definitionProps.getFilters())
                .stream()
                .collect(toMap(IProcessFilter::getName, e -> e));
        definitions.setFilterMap(filterMap);

        // Parse to mappers.
        final Map<String, IProcessMapper> mapperMap = safeList(definitionProps.getMappers())
                .stream()
                .collect(toMap(IProcessMapper::getName, e -> e));
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

        // Parse to connector processes(merge to filters/mappers).
        final ComplexProcessHandler[] processes = safeList(connectorProps.getProcesses())
                .stream()
                .map(processName -> {
                    // Find the definition if filter.
                    final Optional<IProcessFilter> foundFilterOp = safeList(properties.getDefinitions().getFilters())
                            .stream()
                            .filter(s -> StringUtils.equals(processName, s.getName()))
                            .findFirst();

                    // Find the definition if mapper.
                    final Optional<IProcessMapper> foundMapperOp = safeList(properties.getDefinitions().getMappers())
                            .stream()
                            .filter(s -> StringUtils.equals(processName, s.getName()))
                            .findFirst();

                    // Check the definition if filter or mapper.
                    return foundFilterOp
                            .map(iProcessFilter -> (ComplexProcessHandler) iProcessFilter)
                            .orElseGet(() -> (ComplexProcessHandler) foundMapperOp.orElseThrow(() -> new IllegalStateException(
                                    String.format("Not found the filter or mapper definition '%s'", processName))));
                }).toArray(ComplexProcessHandler[]::new);
        connectorConfig.setProcessChain(new ComplexProcessChain(processes));
        connectorConfig.setProcessConfig(connectorProps.getExecutor());

        // Setup to connector qos.
        final IQoS qos = definitions.getQoSMap().get(connectorProps.getQos());
        requireNonNull(qos, String.format("Not found the qos '%s'", connectorProps.getQos()));
        connectorConfig.setQos(qos);

        // Setup to connector checkpoint.
        final ICheckpoint checkpoint = definitions.getCheckpointMap().get(connectorProps.getCheckpoint());
        requireNonNull(checkpoint, String.format("Not found the checkpoint '%s'", connectorProps.getCheckpoint()));
        connectorConfig.setCheckpoint(checkpoint);

        return connectorConfig;
    }

    public ConnectorConfig getRequiredConnectorConfig(@NotBlank String connectorName) {
        Assert2.hasTextOf(connectorName, "connectorName");
        final ConnectorConfig connectorConfig = safeMap(getConnectorMap()).get(connectorName);
        if (isNull(connectorConfig)) {
            throw new StreamConnectException(String.format("Not found connector '%s'", connectorName));
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
        private @NotNull Map<String, IProcessFilter> filterMap;
        private @Null Map<String, IProcessMapper> mapperMap;
        private @NotEmpty Map<String, IQoS> qoSMap;
        private @NotEmpty Map<String, ICheckpoint> checkpointMap;
        private @NotEmpty Map<String, IShardingStrategy> shardingStrategyMap;
        private @NotEmpty Map<String, CoordinatorProvider> coordinatorProviderMap;

        public void validate() {
            Assert2.notEmptyOf(filterMap, "filterMap");
            Assert2.notEmptyOf(mapperMap, "mapperMap");
            Assert2.notEmptyOf(qoSMap, "qoSMap");
            Assert2.notEmptyOf(checkpointMap, "checkpointMap");
            Assert2.notEmptyOf(shardingStrategyMap, "shardingStrategyMap");
            this.filterMap.values().forEach(IProcessFilter::validate);
            this.mapperMap.values().forEach(IProcessMapper::validate);
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
        private @NotNull ComplexProcessChain processChain;
        private @NotNull ProcessStreamConfig processConfig;
        private @NotNull IQoS qos;
        private @NotNull ICheckpoint checkpoint;

        public void validate() {
            Assert2.hasTextOf(name, "name");
            Assert2.notNullOf(processChain, "processChain");
            Assert2.notNullOf(processConfig, "processConfig");
            Assert2.notNullOf(qos, "qos");
            Assert2.notNullOf(checkpoint, "checkpoint");
        }
    }

}
