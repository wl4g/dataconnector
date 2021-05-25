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

import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.dispatch.SubscribeEngineBootstrap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;

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
    private final SubscribeEngineBootstrap engine;

    public @Null Boolean stopFilter(@NotBlank String sharedConsumerGroupId, long shutdownTimeout) throws InterruptedException {
        return engine.stopFilter(sharedConsumerGroupId, shutdownTimeout);
    }

    public @Null Boolean stopSinker(@NotNull Long subscriberId, long shutdownTimeout) throws InterruptedException {
        return engine.stopSinker(subscriberId, shutdownTimeout);
    }

}
