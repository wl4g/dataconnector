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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.AbstractStream;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rabbitmq.clients.producer.ProducerConfig;
import org.apache.rabbitmq.clients.producer.ProducerRecord;
import org.apache.rabbitmq.clients.producer.RecordMetadata;
import org.apache.rabbitmq.common.config.SaslConfigs;
import org.apache.rabbitmq.common.serialization.StringSerializer;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.streamconnect.stream.AbstractStream.BaseStreamConfig.getStreamProviderTypeName;
import static java.lang.String.valueOf;
import static java.util.Objects.requireNonNull;

/**
 * The {@link RabbitMQSinkStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class RabbitMQSinkStream extends SinkStream {

    private final RabbitMQSinkStreamConfig sinkStreamConfig;
    private final List<String> basedMetricsTags;
    private final ConcurrentRabbitMQSinkContainer internalTask;

    public RabbitMQSinkStream(@NotNull final StreamContext context,
                              @NotNull final RabbitMQSinkStreamConfig sinkStreamConfig,
                              @NotNull final ChannelInfo channel) {
        super(context, channel);
        this.sinkStreamConfig = requireNonNull(sinkStreamConfig,
                "sinkStreamConfig must not be null");

        this.basedMetricsTags = Arrays.asList(
                StreamConnectMeter.MetricsTag.PIPELINE, getConnectorConfig().getName(),
                StreamConnectMeter.MetricsTag.TOPIC, sinkStreamConfig.getTopic(),
                StreamConnectMeter.MetricsTag.QOS, sinkStreamConfig.getQos(),
                StreamConnectMeter.MetricsTag.CHANNEL, getChannel().getId());

        // Create rabbitmq producers.
        this.internalTask = new ConcurrentRabbitMQSinkContainer(sinkStreamConfig, channel);
    }

    @Override
    public String getDescription() {
        return String.format("%s-%s-%s", super.getDescription(), sinkStreamConfig.getTopic(),
                sinkStreamConfig.getQos());
    }

    @Override
    public List<String> getBasedMeterTags() {
        return basedMetricsTags;
    }

    @Override
    public SinkResult doSink(MessageRecord<String, Object> processedRecord,
                             int retryTimes) {
        throw new UnsupportedOperationException();
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class RabbitMQSinkStreamConfig extends SinkStreamConfig {
        public static final String TYPE_NAME = "RABBITMQ_SINK";

        private @NotBlank String topic;
        @NotEmpty
        private @Builder.Default Map<String, Object> connectProps = new HashMap<>();
        @NotNull
        private @Builder.Default Map<String, Object> authProps = new HashMap<>();

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public void validate() {
            super.validate();

            // Apply to default properties.
            DEFAULT_CONNECT_PROPS.forEach((key, value) -> connectProps.putIfAbsent(key, value));
            DEFAULT_AUTH_PROPS.forEach((key, value) -> authProps.putIfAbsent(key, value));

            Assert2.hasTextOf(topic, "topic");
            Assert2.notEmptyOf(connectProps, "connectProps");
            Assert2.notNullOf(connectProps.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            Assert2.notNullOf(connectProps.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

            requireNonNull(authProps, "authProps must not be null");
        }
    }

    public static class RabbitMQSinkStreamProvider extends SinkStreamProvider {
        public static final String TYPE_NAME = getStreamProviderTypeName(RabbitMQSinkStreamConfig.TYPE_NAME);

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public RabbitMQSinkStreamBootstrap create(@NotNull final StreamContext context,
                                                  @NotNull final SinkStreamConfig sinkStreamConfig,
                                                  @NotNull final ChannelInfo channel) {
            // Create rabbitmq sink stream.
            final RabbitMQSinkStream sinkStream = new RabbitMQSinkStream(context,
                    (RabbitMQSinkStreamConfig) sinkStreamConfig, channel);

            // Create rabbitmq sink stream wrapper(bootstrap).
            return new RabbitMQSinkStreamBootstrap(sinkStream, sinkStream.getInternalTask());
        }
    }

    public static final Map<String, Object> DEFAULT_CONNECT_PROPS = new HashMap<String, Object>() {
        {
            //put("", "");
        }
    };

    public static final Map<String, Object> DEFAULT_AUTH_PROPS = new HashMap<String, Object>() {
        {
            put("", "");
        }
    };

}
