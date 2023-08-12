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

package com.wl4g.streamconnect.stream.sink.rabbitmq;

import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.stream.sink.rabbitmq.RabbitMQSinkStream.RabbitMQSinkStreamConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * The {@link ConcurrentRabbitMQSinkContainer}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class ConcurrentRabbitMQSinkContainer implements Closeable {
    private final RabbitMQSinkStreamConfig sinkStreamConfig;
    private final ChannelInfo channel;
    public ConcurrentRabbitMQSinkContainer(RabbitMQSinkStreamConfig sinkStreamConfig,
                                           ChannelInfo channel) {
        this.sinkStreamConfig = requireNonNull(sinkStreamConfig, "sinkStreamConfig must not be null");
        this.channel = requireNonNull(channel, "channel must not be null");
    }

    public void close(final Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }
}
