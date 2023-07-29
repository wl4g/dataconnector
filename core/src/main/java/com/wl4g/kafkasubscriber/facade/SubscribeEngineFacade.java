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
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Map;
import java.util.Objects;

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
    private final KafkaSubscriberProperties config;
    private final SubscribeEngineManager engineManager;

    public @NotNull Map<String, Boolean> startFilters(@NotBlank String pipelineName,
                                                      String... sourceNames) {
        return getRequiredPipeline(pipelineName).startFilters(sourceNames);
    }

    public @NotNull Map<String, Boolean> startSinks(@NotBlank String pipelineName,
                                                    String... pipelineNames) {
        return getRequiredPipeline(pipelineName).startSinks(pipelineNames);
    }

    public @NotNull Map<String, Boolean> stopFilters(@NotBlank String pipelineName,
                                                     long perFilterTimeout,
                                                     @Null String... sourceNames) {
        return getRequiredPipeline(pipelineName).stopFilters(perFilterTimeout, sourceNames);
    }

    public @NotNull Map<String, Boolean> stopSinks(@NotBlank String pipelineName,
                                                   long perSinkTimeout,
                                                   @Null String... subscriberIds) {
        return getRequiredPipeline(pipelineName).stopSinks(perSinkTimeout, subscriberIds);
    }

    private SubscribeEngineManager.SubscribePipelineBootstrap getRequiredPipeline(@NotBlank String pipelineName) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        final SubscribeEngineManager.SubscribePipelineBootstrap pipeline = engineManager
                .getPipelineRegistry().get(pipelineName);
        if (Objects.isNull(pipeline)) {
            throw new IllegalStateException(String.format("Not found pipeline %s for stop.", pipelineName));
        }
        return pipeline;
    }

}
