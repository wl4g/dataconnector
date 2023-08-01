/*
 * Copyright 2017 ~ 2025 the original authors James Wong.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g.kafkasubscriber.facade;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeEnginePipelineConfig;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeSourceConfig;
import com.wl4g.kafkasubscriber.config.SubscriberInfo;
import com.wl4g.kafkasubscriber.dispatch.FilterBatchMessageDispatcher;
import com.wl4g.kafkasubscriber.dispatch.SinkBatchMessageDispatcher;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineManager;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineManager.SubscribeContainerBootstrap;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineManager.SubscribePipelineBootstrap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Map;
import java.util.Objects;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;

/**
 * The {@link SubscribeEngineFacade}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@AllArgsConstructor
public class SubscribeEngineFacade {
    private final KafkaSubscribeConfiguration config;
    private final SubscribeEngineManager engineManager;

    public Map<String, SubscribePipelineBootstrap> getPipelineRegistry() {
        return engineManager.getPipelineRegistry();
    }

    public SubscribePipelineBootstrap registerPipeline(SubscribeEnginePipelineConfig pipelineConfig) {
        return engineManager.registerPipeline(pipelineConfig);
    }

    public SubscribeContainerBootstrap<FilterBatchMessageDispatcher> registerPipelineFilter(String pipelineName,
                                                                                            SubscribeSourceConfig subscribeSourceConfig) {
        return engineManager.registerPipelineFilter(getRequiredPipelineProperties(pipelineName), subscribeSourceConfig);
    }

    public SubscribeContainerBootstrap<SinkBatchMessageDispatcher> registerPipelineSink(String pipelineName,
                                                                                        SubscriberInfo subscriber) {
        return engineManager.registerPipelineSink(getRequiredPipelineProperties(pipelineName), subscriber);
    }

    public @NotNull Map<String, Boolean> startFilters(@NotBlank String pipelineName,
                                                      String... sourceNames) {
        return getRequiredPipelineBootstrap(pipelineName).startFilters(sourceNames);
    }

    public @NotNull Map<String, Boolean> startSinks(@NotBlank String pipelineName,
                                                    String... pipelineNames) {
        return getRequiredPipelineBootstrap(pipelineName).startSinks(pipelineNames);
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

    private SubscribeEnginePipelineConfig getRequiredPipelineProperties(String pipelineName) {
        return safeList(config.getPipelines()).stream()
                .filter(p -> StringUtils.equals(p.getName(), pipelineName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Not found the pipeline properties of %s", pipelineName)));
    }

    private SubscribePipelineBootstrap getRequiredPipelineBootstrap(@NotBlank String pipelineName) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        final SubscribePipelineBootstrap pipeline = getPipelineRegistry().get(pipelineName);
        if (Objects.isNull(pipeline)) {
            throw new IllegalStateException(String.format("Not found the pipeline bootstrap %s for stop.", pipelineName));
        }
        return pipeline;
    }

}
