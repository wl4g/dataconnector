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

package com.wl4g.streamconnect.checkpoint.kafka;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.checkpoint.AbstractCheckpoint;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.meter.MeterEventHandler.CountMeterEvent;
import com.wl4g.streamconnect.meter.MeterEventHandler.TimingMeterEvent;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsTag;
import com.wl4g.streamconnect.stream.process.ProcessStream.ChannelRecord;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import com.wl4g.streamconnect.stream.source.kafka.KafkaSourceStream.KafkaMessageRecord;
import com.wl4g.streamconnect.util.ConcurrentKafkaProducerContainer;
import com.wl4g.streamconnect.util.KafkaConsumerBuilder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.util.unit.DataSize;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.streamconnect.stream.AbstractStream.KEY_CHANNEL;
import static com.wl4g.streamconnect.stream.AbstractStream.KEY_SEQUENCE;
import static com.wl4g.streamconnect.stream.AbstractStream.KEY_TENANT;
import static com.wl4g.streamconnect.util.ConcurrentKafkaProducerContainer.buildDefaultAcknowledgedProducerContainer;
import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.DELETE_RETENTION_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

/**
 * The {@link KafkaCheckpoint}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@Setter
public class KafkaCheckpoint extends AbstractCheckpoint {
    public static final String TYPE_NAME = "KAFKA_CHECKPOINT";

    private KafkaCheckpointConfig checkpointConfig;

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public void init() {
        // Ignore
    }

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    @Override
    public PointWriter createWriter(@NotNull ConnectorConfig connectorConfig,
                                    @NotNull ChannelInfo channel,
                                    @NotNull CachingChannelRegistry registry) {
        return new PointWriter() {
            private final Map<String, ConcurrentKafkaProducerContainer> pointProducersMap = new ConcurrentHashMap<>();

            @Override
            public synchronized void stop(long timeoutMs, boolean force) throws Exception {
                // If all are closed, skip
                if (this.pointProducersMap.isEmpty()) {
                    if (log.isInfoEnabled()) {
                        log.info("Skip to close producers, because it has been closed.");
                    }
                    return;
                }

                // Close of multi-producers corresponding to each channel.
                final Iterator<Map.Entry<String, ConcurrentKafkaProducerContainer>> it = pointProducersMap
                        .entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<String, ConcurrentKafkaProducerContainer> entry = it.next();
                    final String channelId = entry.getKey();
                    final ConcurrentKafkaProducerContainer container = entry.getValue();
                    try {
                        if (log.isInfoEnabled()) {
                            log.info("{} :: {} :: Closing to write point producer...",
                                    connectorConfig.getName(), channel.getId());
                        }
                        container.close();
                        if (log.isInfoEnabled()) {
                            log.info("{} :: {} :: Closed to write point producer.",
                                    connectorConfig.getName(), channel.getId());
                        }
                    } catch (Throwable ex) {
                        throw new StreamConnectException(String.format("%s :: Failed to close write point producers.",
                                connectorConfig.getName()), ex);
                    } finally {
                        // The all producers corresponding to this channel are closed, remove it.
                        it.remove();
                        if (log.isInfoEnabled()) {
                            log.info("{} :: {} :: Removed to write point producers.",
                                    connectorConfig.getName(), channel.getId());
                        }
                    }
                }
            }

            @Override
            public WritePointResult writeAsync(ConnectorConfig connectorConfig,
                                               ChannelRecord record,
                                               int retryTimes) {
                final ChannelInfo channel = record.getChannel();
                final String key = record.getRecord().getKey();
                final Object value = record.getRecord().getValue();

                final Producer<String, Object> producer = determineKafkaProducer(channel, key);
                final String topic = getCheckpointConfig().generateDlqTopic(channel.getId());

                final ProducerRecord<String, Object> pr = new ProducerRecord<>(topic, key, value.toString());
                // Note: Hand down the channel metadata of each record to the downstream.
                pr.headers().add(new RecordHeader(KEY_TENANT, valueOf(channel.getTenantId()).getBytes()));
                pr.headers().add(new RecordHeader(KEY_CHANNEL, valueOf(channel.getId()).getBytes()));
                pr.headers().add(new RecordHeader(KEY_SEQUENCE, valueOf(channel.getSettingsSpec().getPolicySpec()
                        .isSequence()).getBytes()));

                if (log.isDebugEnabled()) {
                    log.debug("{} :: {} :: Writing to point record : {}",
                            connectorConfig.getName(), channel.getId(), pr);
                }
                return new WritePointResult(record, producer, producer.send(pr), retryTimes);
            }

            @SuppressWarnings("resource")
            private Producer<String, Object> determineKafkaProducer(ChannelInfo channel,
                                                                    String key) {
                // Checking for initialize by channel.
                final ConcurrentKafkaProducerContainer container = initIfNecessary(channel);

                // Determine write point producer with channel.
                return container.obtainDetermineProducer(key, channel.getSettingsSpec().getPolicySpec().isSequence());
            }

            @SuppressWarnings("unchecked")
            private <T> T initIfNecessary(ChannelInfo channel) {
                return (T) this.pointProducersMap.computeIfAbsent(channel.getId(), channelId -> {
                    if (log.isInfoEnabled()) {
                        log.info("{} :: {} Initializing to write point store ... ",
                                connectorConfig.getName(), channel.getId());
                    }
                    // Create or update checkpoint(DLQ) topic.
                    final KafkaTopicHelper helper = new KafkaTopicHelper(getConfig(), registry);
                    helper.initChannelsTopicIfNecessary(connectorConfig, getCheckpointConfig(), singletonList(channel));
                    if (log.isInfoEnabled()) {
                        log.info("{} :: {} :: Initialized to write point store ({})",
                                connectorConfig.getName(), channel.getId(), pointProducersMap.size());
                    }

                    final Map<String, Object> mergedProps = new HashMap<>();
                    safeMap(getCheckpointConfig().getProducerProps()).forEach(mergedProps::putIfAbsent);
                    mergedProps.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, getCheckpointBootstrapServers(getCheckpointConfig(), channel));
                    // TODO support protobuf,KafkaAvroSerializer(not arrow)?
                    mergedProps.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    mergedProps.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                    return new ConcurrentKafkaProducerContainer("checkpoint-".concat(channel.getId()), getCheckpointConfig().getParallelism(),
                            null, mergedProps);
                });
            }

            @SuppressWarnings("unchecked")
            @Override
            public void flush(Collection<WritePointResult> results) {
                safeList(results).parallelStream().forEach(result -> {
                    try {
                        ((Producer<String, Object>) result.getInternalOperator()).flush();
                    } catch (Throwable ex) {
                        log.error(String.format("%s :: Failed to flush write point record: %s",
                                connectorConfig.getName(), result.getRecord()), ex);
                    }
                });
                // Release to memory.
                results.clear();
            }
        };
    }

    @Override
    public PointReader createReader(@NotNull ConnectorConfig connectorConfig,
                                    @NotNull ChannelInfo channel,
                                    @NotNull ReadPointListener listener) {
        requireNonNull(connectorConfig, "connectorConfig must not be null");
        requireNonNull(channel, "channel must not be null");
        requireNonNull(listener, "listener must not be null");

        return new PointReader() {
            private ConcurrentKafkaProducerContainer acknowledgeProducerContainer;
            private ConcurrentMessageListenerContainer<String, Object> consumerContainer;

            @Override
            public synchronized void start() {
                if (nonNull(consumerContainer)) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: {} :: Skip to start checkpoint read listener container, because it has been started.",
                                connectorConfig.getName(), channel.getId());
                    }
                    return;
                }
                listener.setReader(this);

                // Initial acknowledge producer container.
                this.acknowledgeProducerContainer = buildDefaultAcknowledgedProducerContainer(channel.getId(),
                        getCheckpointConfig().getBootstrapServers());

                // Initial consumer container.
                final String groupId = getCheckpointConfig().generateDlqConsumerGroupId(channel.getId());
                final String topic = getCheckpointConfig().generateDlqTopic(channel.getId());
                final String checkpointServers = getCheckpointBootstrapServers(getCheckpointConfig(), channel);

                final Map<String, Object> props = new HashMap<>();
                safeMap(getCheckpointConfig().getConsumerProps()).forEach(props::putIfAbsent);
                props.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, checkpointServers);
                // TODO support protobuf,KafkaAvroSerializer(not arrow)?
                props.putIfAbsent(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.putIfAbsent(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

                this.consumerContainer = new KafkaConsumerBuilder(props)
                        .buildContainer(Pattern.compile(topic),
                                groupId,
                                getCheckpointConfig().getParallelism(),
                                (records, ack) -> {
                                    final long readTimingBegin = System.nanoTime();
                                    try {
                                        getEventPublisher().publishEvent(CountMeterEvent.of(
                                                MetricsName.checkpoint_read_success,
                                                MetricsTag.CONNECTOR,
                                                connectorConfig.getName(),
                                                MetricsTag.CHANNEL,
                                                channel.getId()));

                                        listener.onMessage(KafkaMessageRecord.from(records), ack::acknowledge);
                                    } catch (Throwable ex) {
                                        log.error(String.format("%s :: %s :: Failed to read point records : %s",
                                                connectorConfig.getName(), channel.getId(), records), ex);

                                        getEventPublisher().publishEvent(CountMeterEvent.of(
                                                MetricsName.checkpoint_read_failure,
                                                MetricsTag.CONNECTOR,
                                                connectorConfig.getName(),
                                                MetricsTag.CHANNEL,
                                                channel.getId()));

                                        // Commit directly if no quality of service is required.
                                        connectorConfig.getQos().acknowledgeIfFail(connectorConfig, ex, () -> {
                                            if (log.isDebugEnabled()) {
                                                log.debug("{} :: {} :: Retry to write point. - {}",
                                                        connectorConfig.getName(), channel.getId(), records);
                                            }
                                            ack.acknowledge();
                                        });
                                    } finally {
                                        getEventPublisher().publishEvent(TimingMeterEvent.of(
                                                MetricsName.checkpoint_read_time,
                                                StreamConnectMeter.DEFAULT_PERCENTILES,
                                                Duration.ofNanos(System.nanoTime() - readTimingBegin),
                                                MetricsTag.CONNECTOR,
                                                connectorConfig.getName(),
                                                MetricsTag.CHANNEL,
                                                channel.getId()));
                                    }
                                });
                if (log.isInfoEnabled()) {
                    log.info("{} :: {} :: Starting kafka checkpoint read listener container ...",
                            connectorConfig.getName(), channel.getId());
                }
                this.consumerContainer.start();
            }

            @Override
            public synchronized boolean stop(long timeout, boolean force) throws Exception {
                if (isRunning()) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    if (force) {
                        consumerContainer.stopAbnormally(latch::countDown);
                    } else { // graceful shutdown
                        consumerContainer.stop(latch::countDown);
                    }
                    this.acknowledgeProducerContainer.close(Duration.ofMillis(timeout));
                    if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                        throw new TimeoutException(String.format("Timeout close checkpoint reader for %sms", timeout));
                    }
                    if (log.isInfoEnabled()) {
                        log.info("{} :: Closed checkpoint reader.", channel.getId());
                    }
                }
                return isRunning();
            }

            @Override
            public void pause() {
                if (isRunning()) {
                    this.consumerContainer.pause();
                }
            }

            @Override
            public void resume() {
                if (!isRunning()) {
                    consumerContainer.resume();
                }
            }

            @Override
            public boolean scaling(int concurrency,
                                   boolean restart,
                                   long restartTimeout) throws Exception {
                consumerContainer.setConcurrency(concurrency);
                if (restart) {
                    if (stop(restartTimeout, false)) {
                        start();
                        return true;
                    } else {
                        return false;
                    }
                }
                // It is bound not to immediately scale the number of concurrent containers.
                return false;
            }

            @Override
            public boolean isRunning() {
                return consumerContainer.isRunning();
            }

            @Override
            public int getSubTaskCount() {
                return consumerContainer.getConcurrency();
            }

            /**
             * After the maximum number of retries, there may still be records that failed to process.
             * At this time, only the consecutively successful records starting from the earliest batch
             * of offsets will be submitted, and the offsets of other failed records will not be committed,
             * so as to strictly limit the processing failures that will not be submitted so that zero is not lost.
             *
             * @param sentResults completed sent results
             */
            @Override
            public void preferAutoAcknowledge(Collection<SinkStream.SinkResult> sentResults) {
                // grouping and sorting.
                final Map<TopicPartition, List<KafkaMessageRecord<String, Object>>> partitionRecords =
                        new HashMap<>(sentResults.size());
                for (SinkStream.SinkResult savepointResult : sentResults) {
                    final KafkaMessageRecord<String, Object> record0 = (KafkaMessageRecord<String, Object>)
                            savepointResult.getRecord();
                    final TopicPartition topicPartition = new TopicPartition(record0.getTopic(), record0.getPartition());
                    partitionRecords.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(record0);
                }

                // Find the maximum offset that increments consecutively for each partition.
                final Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
                for (Map.Entry<TopicPartition, List<KafkaMessageRecord<String, Object>>> entry : partitionRecords.entrySet()) {
                    final TopicPartition topicPartition = entry.getKey();
                    final List<KafkaMessageRecord<String, Object>> records = entry.getValue();
                    // ASC sorting by offset.
                    records.sort(Comparator.comparingLong(KafkaMessageRecord::getOffset));

                    records.stream().mapToLong(KafkaMessageRecord::getOffset)
                            .reduce((prev, curr) -> curr == prev + 1 ? curr : prev)
                            .ifPresent(maxOffset -> partitionOffsets.put(topicPartition, new OffsetAndMetadata(maxOffset)));
                }

                // Acknowledge to source kafka broker.
                final long acknowledgeTimingBegin = System.nanoTime();
                try {
                    // see:org.apache.kafka.clients.consumer.internals.ConsumerCoordinator#onJoinComplete(int, String, String, ByteBuffer)
                    // see:org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#doSendOffsets(Producer, Map)
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: Prefer acknowledging offsets to transaction. - : {}",
                                channel.getId(), partitionOffsets);
                    }
                    this.acknowledgeProducerContainer.obtainFirstProducer().sendOffsetsToTransaction(partitionOffsets,
                            new ConsumerGroupMetadata(channel.getId()));
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: Prefer acknowledged offsets to transaction. - : {}",
                                channel.getId(), partitionOffsets);
                    }
                } catch (Throwable ex) {
                    final String errmsg = String.format("%s :: Failed to prefer acknowledge offsets to transaction. - : %s",
                            channel.getId(), partitionOffsets);
                    log.error(errmsg, ex);
                    throw new StreamConnectException(errmsg, ex);
                } finally {
                    getEventPublisher().publishEvent(TimingMeterEvent.of(
                            MetricsName.acknowledge_time,
                            StreamConnectMeter.DEFAULT_PERCENTILES,
                            Duration.ofNanos(System.nanoTime() - acknowledgeTimingBegin),
                            MetricsTag.CHECKPOINT,
                            getName(),
                            MetricsTag.CHANNEL,
                            String.valueOf(channel.getId()),
                            MetricsTag.ACK_KIND,
                            MetricsTag.ACK_KIND_VALUE_SEND));
                }
            }

            @Override
            public void addAcknowledgeCountMeter(MetricsName metrics,
                                                 Collection<SinkStream.SinkResult> sinkResults) {
                sinkResults.stream()
                        .map(sr -> {
                            final KafkaMessageRecord<String, Object> record =
                                    (KafkaMessageRecord<String, Object>) sr.getRecord();
                            return new TopicPartition(record.getTopic(), record.getPartition());
                        })
                        .distinct()
                        .forEach(tp -> getEventPublisher().publishEvent(CountMeterEvent.of(
                                metrics,
                                MetricsTag.CHECKPOINT,
                                getName(),
                                MetricsTag.CHANNEL,
                                String.valueOf(channel.getId()))));
            }
        };
    }

    static String getCheckpointBootstrapServers(KafkaCheckpointConfig checkpointConfig,
                                                ChannelInfo channel) {
        // Determine to checkpoint store servers.
        String checkpointServers = channel.getSettingsSpec().getCheckpointSpec().getServers();
        if (isBlank(checkpointServers)) {
            // default by connector checkpoint config.
            checkpointServers = checkpointConfig.getBootstrapServers();
        }
        return checkpointServers;
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static final class KafkaCheckpointConfig extends CheckpointConfig {
        // Avoidance value, priority is given to configured in the channel.
        @Default
        private @NotBlank String bootstrapServers = "localhost:9092";
        @Default
        private @NotBlank String topicPrefix = "streamconnect-checkpoint-topic-";
        @Default
        private @Min(1) @Max(5000) int topicPartitions = 30;
        @Default
        private @Min(1) @Max(64) short replicationFactor = 1;
        @Default
        private @NotBlank String groupIdPrefix = "streamconnect-checkpoint-group-";
        @Default
        private @Min(1) @Max(64) int parallelism = 1;
        @Default
        private @Min(1) long initTopicTimeoutMs = 60_000L;
        @Default
        private Map<String, Object> producerProps = new HashMap<String, Object>() {
            {
                //put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                put(ACKS_CONFIG, "0");
                put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
                put(ProducerConfig.SEND_BUFFER_CONFIG, "131072");
                put(ProducerConfig.RETRIES_CONFIG, "5");
                put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "6000");
                put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // snappy|gzip|lz4|zstd|none
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                //put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            }
        };
        @Default
        private Map<String, Object> consumerProps = new HashMap<String, Object>() {
            {
                //put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                put(ConsumerConfig.GROUP_ID_CONFIG, "streamconnect-checkpoint-group-");
            }
        };
        @Default
        private Map<String, Object> defaultTopicProps = new HashMap<String, Object>() {
            {
                put(CLEANUP_POLICY_CONFIG, "delete");
                put(TopicConfig.RETENTION_MS_CONFIG, Duration.ofDays(1).toMillis());
                put(TopicConfig.RETENTION_BYTES_CONFIG, DataSize.ofGigabytes(1).toBytes());
                put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Duration.ofDays(1).toMillis());
                //put(TopicConfig.SEGMENT_MS_CONFIG, "86400000");
                //put(TopicConfig.SEGMENT_BYTES_DOC, "1073741824");
                //put(TopicConfig.SEGMENT_INDEX_BYTES_DOC, "10485760");
                //put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1");
                //put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "86400000");
                //put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "86400000");
                //put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
                //put(TopicConfig.FLUSH_MS_CONFIG, "1000");
                //put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, "10000");
                //put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
            }
        };

        @Override
        public void validate() {
            Assert2.hasTextOf(bootstrapServers, "bootstrapServers");
            Assert2.hasTextOf(topicPrefix, "topicPrefix");
            Assert2.isTrueOf(topicPartitions >= 1 && topicPartitions <= 5000, "topicPartitions >= 1 && topicPartitions <= 5000");
            Assert2.isTrueOf(replicationFactor >= 1 && replicationFactor <= 64, "replicationFactor >= 1 && replicationFactor <= 64");
            Assert2.hasTextOf(groupIdPrefix, "bootstrapServers");
            Assert2.isTrueOf(parallelism >= 1 && parallelism <= 64, "parallelism >= 1 && parallelism <= 64");
            Assert2.isTrueOf(initTopicTimeoutMs >= 1, "initTopicTimeoutMs >= 1");

            // Check for producer properties.
            //requireNonNull(producerProps.get(BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
            requireNonNull(producerProps.get(ACKS_CONFIG), "acks");
            requireNonNull(producerProps.get(REQUEST_TIMEOUT_MS_CONFIG), "request.timeout.ms");
            requireNonNull(producerProps.get(MAX_REQUEST_SIZE_CONFIG), "max.request.size");
            requireNonNull(producerProps.get(SEND_BUFFER_CONFIG), "send.buffer.bytes");
            requireNonNull(producerProps.get(RETRIES_CONFIG), "retries");
            requireNonNull(producerProps.get(RETRY_BACKOFF_MS_CONFIG), "retry.backoff.ms");
            requireNonNull(producerProps.get(COMPRESSION_TYPE_CONFIG), "compression.type");

            // Check for consumer properties.
            requireNonNull(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG), "group.id");

            // Check for default topic properties.
            requireNonNull(defaultTopicProps.get(CLEANUP_POLICY_CONFIG), "cleanup.policy");
            requireNonNull(defaultTopicProps.get(RETENTION_MS_CONFIG), "retention.ms");
            requireNonNull(defaultTopicProps.get(RETENTION_BYTES_CONFIG), "retention.bytes");
            requireNonNull(defaultTopicProps.get(DELETE_RETENTION_MS_CONFIG), "delete.retention.ms");
        }

        public String generateDlqTopic(@NotBlank String channelId) {
            Assert2.hasTextOf(channelId, "channelId");
            if (getTopicPrefix().endsWith("-")) {
                return getTopicPrefix().concat(channelId);
            }
            return getTopicPrefix().concat("-").concat(channelId);
        }

        public String generateDlqConsumerGroupId(@NotBlank String channelId) {
            Assert2.hasTextOf(channelId, "channelId");
            if (getGroupIdPrefix().endsWith("-")) {
                return getGroupIdPrefix().concat(channelId);
            }
            return getGroupIdPrefix().concat("-").concat(channelId);
        }
    }

}
