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

package com.wl4g.dataconnector.stream.sink.kafka;

import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.stream.sink.kafka.KafkaSinkStream.KafkaSinkStreamConfig;
import com.wl4g.dataconnector.util.ConcurrentKafkaProducerContainer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

/**
 * The {@link ConcurrentKafkaSinkContainer}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class ConcurrentKafkaSinkContainer extends ConcurrentKafkaProducerContainer {

    public ConcurrentKafkaSinkContainer(@NotNull ChannelInfo channel,
                                        @NotNull KafkaSinkStreamConfig sinkStreamConfig) {
        super("sink-".concat(channel.getId()), sinkStreamConfig.getParallelism(),
                null, sinkStreamConfig.getProducerProps());
    }
}
