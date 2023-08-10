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

package com.wl4g.streamconnect.facade;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.PipelineConfig;
import com.wl4g.streamconnect.config.StreamConnectProperties.SourceProperties;
import com.wl4g.streamconnect.coordinator.StreamConnectEventPublisher;
import com.wl4g.streamconnect.dispatch.ProcessBatchMessageDispatcher;
import com.wl4g.streamconnect.dispatch.SinkBatchMessageDispatcher;
import com.wl4g.streamconnect.dispatch.StreamConnectEngineScheduler;
import com.wl4g.streamconnect.dispatch.StreamConnectEngineScheduler.ContainerStatus;
import com.wl4g.streamconnect.dispatch.StreamConnectEngineScheduler.SubscribeContainerBootstrap;
import com.wl4g.streamconnect.dispatch.StreamConnectEngineScheduler.SubscribePipelineBootstrap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link StreamConnectEngineFacade}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@AllArgsConstructor
public class StreamConnectEngineFacade {
    private final @Getter(AccessLevel.NONE) StreamConnectConfiguration config;
    private final @Getter(AccessLevel.NONE) StreamConnectEngineScheduler engineManager;
    private final @Getter(AccessLevel.NONE) StreamConnectEventPublisher eventPublisher;

    public Map<String, SubscribePipelineBootstrap> getPipelineRegistry() {
        return engineManager.getPipelineRegistry();
    }

    public SubscribePipelineBootstrap registerPipeline(PipelineConfig pipelineConfig) {
        return engineManager.registerPipeline(pipelineConfig);
    }

    public SubscribeContainerBootstrap<ProcessBatchMessageDispatcher> registerPipelineFilter(
            String pipelineName,
            SourceProperties subscribeSourceProps) {
        return engineManager.registerPipelineFilter(getRequiredPipelineProperties(pipelineName), subscribeSourceProps);
    }

    public SubscribeContainerBootstrap<SinkBatchMessageDispatcher> registerPipelineSink(
            String pipelineName,
            SubscriberInfo subscriber) {
        return engineManager.registerPipelineSink(getRequiredPipelineProperties(pipelineName), subscriber);
    }

    public @NotNull Map<String, Boolean> startFilters(@NotBlank String pipelineName,
                                                      String... sourceNames) {
        return getRequiredPipelineBootstrap(pipelineName).startFilters(sourceNames);
    }

    public @NotNull Map<String, Boolean> startSinks(@NotBlank String pipelineName,
                                                    String... subscriberIds) {
        return getRequiredPipelineBootstrap(pipelineName).startSinks(subscriberIds);
    }

    public @NotNull Map<String, Boolean> stopFilters(@NotBlank String pipelineName,
                                                     long perFilterTimeout,
                                                     @Null String... sourceNames) {
        return getRequiredPipelineBootstrap(pipelineName).stopFilters(perFilterTimeout, sourceNames);
    }

    public @NotNull Map<String, Boolean> stopSinks(@NotBlank String pipelineName,
                                                   long perSinkTimeout,
                                                   @Null String... subscriberIds) {
        return getRequiredPipelineBootstrap(pipelineName).stopSinks(perSinkTimeout, subscriberIds);
    }

    public @NotNull Map<String, ContainerStatus> statusFilters(@NotBlank String pipelineName,
                                                               String... sourceNames) {
        return getRequiredPipelineBootstrap(pipelineName).statusFilters(sourceNames);
    }

    public @NotNull Map<String, ContainerStatus> statusSinks(@NotBlank String pipelineName,
                                                             String... subscriberIds) {
        return getRequiredPipelineBootstrap(pipelineName).statusSinks(subscriberIds);
    }

    public Map<String, Integer> scalingFilters(@NotBlank String pipelineName,
                                               int perConcurrency,
                                               boolean restart,
                                               long perRestartTimeout,
                                               String... sourceNames) {
        return getRequiredPipelineBootstrap(pipelineName).scalingFilters(
                perConcurrency, restart, perRestartTimeout, sourceNames);
    }

    public Map<String, Integer> scalingSinks(@NotBlank String pipelineName,
                                             int perConcurrency,
                                             boolean restart,
                                             long perRestartTimeout,
                                             String... subscriberIds) {
        return getRequiredPipelineBootstrap(pipelineName).scalingSinks(
                perConcurrency, restart, perRestartTimeout, subscriberIds);
    }

    private PipelineConfig getRequiredPipelineProperties(
            String pipelineName) {
        return config.getRequiredPipelineConfig(pipelineName);
    }

    private SubscribePipelineBootstrap getRequiredPipelineBootstrap(
            @NotBlank String pipelineName) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        final SubscribePipelineBootstrap pipeline = getPipelineRegistry().get(pipelineName);
        if (Objects.isNull(pipeline)) {
            throw new IllegalStateException(String.format("Not found the pipeline bootstrap %s for stop.", pipelineName));
        }
        return pipeline;
    }

}
