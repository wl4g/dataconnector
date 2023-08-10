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
import com.wl4g.streamconnect.checkpoint.IProcessCheckpoint;
import com.wl4g.streamconnect.config.StreamConnectProperties.CoordinatorProperties;
import com.wl4g.streamconnect.config.StreamConnectProperties.DefinitionProperties;
import com.wl4g.streamconnect.coordinator.KafkaStreamConnectCoordinator.KafkaCoordinatorBusConfig;
import com.wl4g.streamconnect.coordinator.KafkaStreamConnectCoordinator.KafkaCoordinatorDiscoveryConfig;
import com.wl4g.streamconnect.coordinator.strategy.IShardingStrategy;
import com.wl4g.streamconnect.coordinator.strategy.ShardingStrategyFactory;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.process.ComplexProcessHandler;
import com.wl4g.streamconnect.process.filter.IProcessFilter;
import com.wl4g.streamconnect.process.map.IProcessMapper;
import com.wl4g.streamconnect.process.sink.IProcessSink;
import com.wl4g.streamconnect.source.ISourceProvider;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The {@link StreamConnectConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
public class StreamConnectConfiguration {

    private final StreamConnectProperties properties;
    private final DefinitionsConfig definitions;
    private final Map<String, PipelineConfig> pipelineMap;
    private final CoordinatorConfig coordinator;

    public StreamConnectConfiguration(@NotNull StreamConnectProperties properties) {
        Assert2.notNullOf(properties, "properties");
        this.properties = properties;

        // Parsing to actuate definitions config.
        this.definitions = parseDefinitionsConfig(properties);

        // Parsing to actuate source,filter,sink
        this.pipelineMap = safeList(properties.getPipelines())
                .stream()
                .map(p -> parsePipelineConfig(properties, p))
                .collect(toMap(PipelineConfig::getName, e -> e));

        // Parsing to actuate coordinator config.
        this.coordinator = parseCoordinatorConfig(properties);

        this.validate();
    }

    public StreamConnectConfiguration validate() {
        Assert2.notNullOf(definitions, "definitions");
        Assert2.notEmptyOf(pipelineMap, "pipelines");
        Assert2.notNullOf(coordinator, "coordinator");
        definitions.validate();
        safeMap(pipelineMap).values().forEach(PipelineConfig::validate);
        coordinator.validate();
        return this;
    }

    public PipelineConfig getRequiredPipelineConfig(@NotBlank String pipelineName) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        final PipelineConfig pipelineConfig = safeMap(getPipelineMap()).get(pipelineName);
        if (Objects.isNull(pipelineConfig)) {
            throw new StreamConnectException(String.format("Not found pipeline '%s'", pipelineName));
        }
        return pipelineConfig;
    }

    private DefinitionsConfig parseDefinitionsConfig(@NotNull StreamConnectProperties properties) {
        Assert2.notNullOf(properties, "properties");

        final DefinitionProperties definitionProps = properties.getDefinitions();
        final DefinitionsConfig definitions = new DefinitionsConfig();

        // Parse to checkpoint.
        final Map<String, IProcessCheckpoint> checkpointMap = safeList(definitionProps.getCheckpoints())
                .stream()
                .collect(toMap(IProcessCheckpoint::getName, e -> e));
        definitions.setCheckpointMap(checkpointMap);

        // Parse to source.
        final Map<String, ISourceProvider> sourceMap = safeList(definitionProps.getSources())
                .stream()
                .collect(toMap(ISourceProvider::getName, e -> e));
        definitions.setSourceMap(sourceMap);

        // Parse to filter.
        final Map<String, IProcessFilter> filterMap = safeList(definitionProps.getFilters())
                .stream()
                .collect(toMap(IProcessFilter::getName, e -> e));
        definitions.setFilterMap(filterMap);

        // Parse to mapper.
        final Map<String, IProcessMapper> mapperMap = safeList(definitionProps.getMappers())
                .stream()
                .collect(toMap(IProcessMapper::getName, e -> e));
        definitions.setMapperMap(mapperMap);

        // Parse to sink.
        final Map<String, IProcessSink> sinkMap = safeList(definitionProps.getSinks())
                .stream()
                .collect(toMap(IProcessSink::getName, e -> e));
        definitions.setSinkMap(sinkMap);

        return definitions;
    }

    private PipelineConfig parsePipelineConfig(@NotNull StreamConnectProperties properties,
                                               @NotNull StreamConnectProperties.PipelineProperties pipelineProps) {
        Assert2.notNullOf(properties, "properties");
        Assert2.notNullOf(pipelineProps, "pipelineProps");

        final PipelineConfig pipeline = new PipelineConfig();
        pipeline.setName(pipelineProps.getName());
        pipeline.setEnable(pipelineProps.isEnable());

        // Parse to pipeline checkpoint.
        final IProcessCheckpoint checkpoint = safeList(properties.getDefinitions().getCheckpoints())
                .stream()
                .filter(s -> StringUtils.equals(s.getName(), pipelineProps.getCheckpoint()))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(String.format("Not found the checkpoint '%s'",
                        pipelineProps.getCheckpoint())));
        pipeline.setCheckpoint(checkpoint);

        // Parse to pipeline source.
        final ISourceProvider sourceProvider = safeList(properties.getDefinitions().getSources())
                .stream()
                .filter(s -> StringUtils.equals(pipelineProps.getSource(), s.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(String.format("Not found the source definition '%s'",
                        pipelineProps.getSource())));
        pipeline.setSourceProvider(sourceProvider);

        // Merge parse to pipeline filters and mappers.
        final ComplexProcessHandler[] processes = safeList(pipelineProps.getProcesses())
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
        pipeline.setProcesses(processes);

        // Parse to pipeline sink.
        if (isNotBlank(pipelineProps.getSink())) {
            final IProcessSink sink = safeList(properties.getDefinitions().getSinks())
                    .stream()
                    .filter(s -> StringUtils.equals(s.getName(), pipelineProps.getSink()))
                    .findFirst().orElseThrow(() -> new IllegalArgumentException(String.format("Not found the sink '%s'",
                            pipelineProps.getSink())));
            pipeline.setSink(sink);
        }

        return pipeline;
    }

    private CoordinatorConfig parseCoordinatorConfig(@NotNull StreamConnectProperties properties) {
        Assert2.notNullOf(properties, "properties");

        final CoordinatorConfig coordinator = new CoordinatorConfig();
        final CoordinatorProperties coordinatorProps = properties.getCoordinator();

        // Parse to coordinator sharding strategy.
        coordinator.setShardingStrategy(ShardingStrategyFactory.getStrategy(coordinatorProps
                .getShardingStrategy()));

        coordinator.setBootstrapServers(coordinatorProps.getBootstrapServers());
        coordinator.setBusConfig(coordinatorProps.getConfigConfig());
        coordinator.setDiscoveryConfig(coordinatorProps.getDiscoveryConfig());

        return coordinator;
    }

    // ----- Definitions configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class DefinitionsConfig {
        private @NotEmpty Map<String, IProcessCheckpoint> checkpointMap;
        private @NotEmpty Map<String, ISourceProvider> sourceMap;
        private @NotNull Map<String, IProcessFilter> filterMap;
        private @Null Map<String, IProcessMapper> mapperMap;
        private @Null Map<String, IProcessSink> sinkMap;

        public void validate() {
            Assert2.notEmptyOf(checkpointMap, "checkpointMap");
            Assert2.notEmptyOf(sourceMap, "sourceMap");
            Assert2.notEmptyOf(filterMap, "filterMap");
            //Assert2.notEmptyOf(mapperMap, "mapperMap");
            //Assert2.notEmptyOf(sinkMap, "sinkMap");
        }
    }

    // ----- Pipelines configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class PipelineConfig {
        private @NotBlank String name;
        private @NotNull boolean enable;
        private @NotNull IProcessCheckpoint checkpoint;
        private @NotNull ISourceProvider sourceProvider;
        private @NotEmpty ComplexProcessHandler[] processes;
        private @Null IProcessSink sink;

        public void validate() {
            Assert2.hasTextOf(name, "name");
            Assert2.notNullOf(checkpoint, "checkpoint");
            Assert2.notNullOf(sourceProvider, "sourceProvider");
            Assert2.notEmptyOf(processes, "processes");
            //Assert2.notNullOf(sink, "sink");
        }
    }

    // ----- Coordinators configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class CoordinatorConfig {
        private @NotNull IShardingStrategy shardingStrategy;
        private @NotBlank String bootstrapServers;
        private @NotNull KafkaCoordinatorBusConfig busConfig;
        private @NotNull KafkaCoordinatorDiscoveryConfig discoveryConfig;

        public void validate() {
            Assert2.notNullOf(shardingStrategy, "shardingStrategy");
            Assert2.hasTextOf(bootstrapServers, "bootstrapServers");
            Assert2.notNullOf(busConfig, "busConfig");
            Assert2.notNullOf(discoveryConfig, "discoveryConfig");
        }
    }

}
