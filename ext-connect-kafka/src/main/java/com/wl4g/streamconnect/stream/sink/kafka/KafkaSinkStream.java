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

package com.wl4g.streamconnect.stream.sink.kafka;

import com.google.common.util.concurrent.SettableFuture;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.streamconnect.stream.AbstractStream.BaseStreamConfig.getStreamProviderTypeName;
import static java.util.Objects.requireNonNull;

/**
 * The {@link KafkaSinkStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class KafkaSinkStream extends SinkStream {

    private final KafkaSinkStreamConfig sinkStreamConfig;
    private final List<String> basedMetricsTags;
    private final ConcurrentKafkaSinkContainer internalTask;

    public KafkaSinkStream(@NotNull final StreamContext context,
                           @NotNull final KafkaSinkStreamConfig sinkStreamConfig,
                           @NotNull final ChannelInfo channel) {
        super(context, channel);
        this.sinkStreamConfig = requireNonNull(sinkStreamConfig,
                "sinkStreamConfig must not be null");

        this.basedMetricsTags = Arrays.asList(
                StreamConnectMeter.MetricsTag.CONNECTOR, getConnectorConfig().getName(),
                StreamConnectMeter.MetricsTag.TOPIC, sinkStreamConfig.getTopic(),
                StreamConnectMeter.MetricsTag.QOS, sinkStreamConfig.getQos(),
                StreamConnectMeter.MetricsTag.CHANNEL, getChannel().getId());

        // Create to kafka producers.
        this.internalTask = new ConcurrentKafkaSinkContainer(channel, getSinkStreamConfig());
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

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    @Override
    public SinkResult doSink(MessageRecord<String, Object> processedRecord,
                             int retryTimes) {
        final String key = processedRecord.getKey();
        final Object value = processedRecord.getValue();
        //final String tenantId = (String) safeMap(processedRecord.getMetadata()).get(KEY_TENANT);
        //final String channelId = (String) safeMap(processedRecord.getMetadata()).get(KEY_CHANNEL);
        final boolean isSequence = Boolean.parseBoolean((String) safeMap(processedRecord.getMetadata()).get(KEY_SEQUENCE));

        // Notice: For reduce the complexity, asynchronous execution is not supported here temporarily, because if the
        // sink implementation is like producer.send(), it is itself asynchronous, which will generate two layers of
        // future, and the processing is not concise enough
        //
        //// Determine the sink task executor.
        //final ThreadPoolExecutor executor = determineSinkExecutor(key);
        //final Future<RecordMetadata> future = internalTask.determineKafkaProducer(isSequence, key, channelId)
        //       .send(new ProducerRecord<>(sinkStreamConfig.getTopic(), key, value));
        //return new SinkResult(processedRecord, future, retryTimes);

        final Producer<String, Object> producer = internalTask.obtainDetermineProducer(key, isSequence);
        final Future<RecordMetadata> future = producer.send(new ProducerRecord<>(sinkStreamConfig.getTopic(), key, value));
        return new SinkResult(processedRecord, future, retryTimes);
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaSinkStreamConfig extends SinkStreamConfig {
        public static final String TYPE_NAME = "KAFKA_SINK";

        private @NotBlank String topic;
        @NotEmpty
        private @Builder.Default Map<String, Object> producerProps = new HashMap<>();

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public void validate() {
            super.validate();

            // Apply to default properties.
            DEFAULT_CONNECT_PROPS.forEach((key, value) -> producerProps.putIfAbsent(key, value));

            Assert2.hasTextOf(topic, "topic");
            Assert2.notEmptyOf(producerProps, "producerProps");
            Assert2.notNullOf(producerProps.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            Assert2.notNullOf(producerProps.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }
    }

    public static class KafkaSinkStreamProvider extends SinkStreamProvider {
        public static final String TYPE_NAME = getStreamProviderTypeName(KafkaSinkStreamConfig.TYPE_NAME);

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public KafkaSinkStreamBootstrap create(@NotNull final StreamContext context,
                                               @NotNull final SinkStreamConfig sinkStreamConfig,
                                               @NotNull final ChannelInfo channel) {
            // Create kafka sink stream.
            final KafkaSinkStream sinkStream = new KafkaSinkStream(context,
                    (KafkaSinkStreamConfig) sinkStreamConfig, channel);

            // Create kafka sink stream wrapper(bootstrap).
            return new KafkaSinkStreamBootstrap(sinkStream, sinkStream.getInternalTask());
        }
    }

    public static final Map<String, Object> DEFAULT_CONNECT_PROPS = new HashMap<String, Object>() {
        {
            //put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
            //put(ProducerConfig.CLIENT_ID_CONFIG, "");
            put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10_000);
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
            put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
            put(ProducerConfig.SEND_BUFFER_CONFIG, 128);
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // snappy|gzip|lz4|zstd|none
            put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
            put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
            put(ProducerConfig.LINGER_MS_CONFIG, 0);
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            put(ProducerConfig.ACKS_CONFIG, "all");
            put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            //put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "");
            put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            // SASL producer properties.
            //put("security.protocol", "SASL_PLAINTEXT");
            //put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            //put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u1001\" password=\"123456\";");
        }
    };

    public static final SettableFuture<?> NOOP_COMPLETED_FUTURE = SettableFuture.create();

    static {
        NOOP_COMPLETED_FUTURE.set(null);
    }

}
