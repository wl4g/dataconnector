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

package com.wl4g.streamconnect.stream.source.kafka;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.checkpoint.ICheckpoint.PointWriter;
import com.wl4g.streamconnect.checkpoint.ICheckpoint.WritePointResult;
import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.meter.MeterEventHandler.CountMeterEvent;
import com.wl4g.streamconnect.meter.MeterEventHandler.TimingMeterEvent;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsTag;
import com.wl4g.streamconnect.stream.source.SourceStream;
import com.wl4g.streamconnect.util.ConcurrentKafkaProducerContainer;
import com.wl4g.streamconnect.util.KafkaConsumerBuilder;
import com.wl4g.streamconnect.util.KafkaConsumerBuilder.ObjectNodeDeserializer;
import com.wl4g.streamconnect.util.KafkaUtil;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.IterableUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.streamconnect.stream.AbstractStream.BaseStreamConfig.getStreamProviderTypeName;
import static com.wl4g.streamconnect.util.ConcurrentKafkaProducerContainer.buildDefaultAcknowledgedProducerContainer;
import static java.lang.Math.max;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * The {@link KafkaSourceStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class KafkaSourceStream extends SourceStream
        implements BatchAcknowledgingMessageListener<String, Object> {

    private final KafkaSourceStreamConfig sourceStreamConfig;
    private final List<String> basedMetricsTags;
    private final ConcurrentMessageListenerContainer<String, Object> internalTask;
    private final ConcurrentKafkaProducerContainer acknowledgeProducerContainer;

    public KafkaSourceStream(@NotNull final StreamContext context,
                             @NotNull final KafkaSourceStreamConfig sourceStreamConfig) {
        super(context);
        this.sourceStreamConfig = requireNonNull(sourceStreamConfig,
                "sourceStreamConfig must not be null");

        this.basedMetricsTags = Arrays.asList(
                MetricsTag.CONNECTOR, getConnectorConfig().getName(),
                MetricsTag.TOPIC, sourceStreamConfig.getTopicPattern(),
                MetricsTag.GROUP_ID, sourceStreamConfig.getGroupId());

        // Initial internal consumers.
        this.internalTask = createInternalTask();

        // Initial acknowledge producer container.
        this.acknowledgeProducerContainer = buildDefaultAcknowledgedProducerContainer(getSourceStreamConfig().getName(),
                (String) getSourceStreamConfig().getConsumerProps().get(BOOTSTRAP_SERVERS_CONFIG));
    }

    protected ConcurrentMessageListenerContainer<String, Object> createInternalTask() {
        return new KafkaConsumerBuilder(sourceStreamConfig.getConsumerProps())
                .buildContainer(Pattern.compile(sourceStreamConfig.getTopicPattern()),
                        sourceStreamConfig.getGroupId(),
                        sourceStreamConfig.getParallelism(), this);
    }

    @Override
    public String getDescription() {
        return String.format("%s(%s-%s-%s-%s-%s)",
                super.getDescription(),
                getConnectorConfig().getName(),
                sourceStreamConfig.getName(),
                sourceStreamConfig.getTopicPattern(),
                sourceStreamConfig.getGroupId(),
                sourceStreamConfig.getParallelism());
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.acknowledgeProducerContainer.close();
    }

    @Override
    public List<String> getBasedMeterTags() {
        return basedMetricsTags;
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, Object>> records,
                          Acknowledgment ack) {
        final long sourceConsumedTimingBegin = System.nanoTime();
        try {
            getEventPublisher().publishEvent(new CountMeterEvent(
                    MetricsName.source_records,
                    getBasedMeterTags()));

            process(KafkaMessageRecord.from(records), ack);
        } catch (Throwable ex) {
            log.error(String.format("%s :: %s :: Failed to process message. - %s",
                    getConnectorConfig().getName(), sourceStreamConfig.getGroupId(), records), ex);

            // Commit directly if no quality of service is required.
            getConnectorConfig().getQos().acknowledgeIfFail(getConnectorConfig(), ex, () -> {
                if (log.isDebugEnabled()) {
                    log.debug("{} :: Retry to process. - {}", getConnectorConfig().getName(), records);
                }
                ack.acknowledge();
            });
        } finally {
            getEventPublisher().publishEvent(new TimingMeterEvent(
                    MetricsName.source_records_time,
                    StreamConnectMeter.DEFAULT_PERCENTILES,
                    Duration.ofNanos(System.nanoTime() - sourceConsumedTimingBegin),
                    getBasedMeterTags()));
        }
    }

    private void process(List<? extends MessageRecord<String, Object>> records,
                         Acknowledgment ack) {
        final Queue<WritePointResult> writePointResults = getProcessStream().process(records);

        // If the sent result set is empty, it means there are no
        // matching records and you can submit it directly.
        if (isNull(writePointResults) || writePointResults.isEmpty()) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("{} :: {} :: Sink is disabled, skip sent acknowledge... records : {}",
                            getConnectorConfig().getName(), sourceStreamConfig.getGroupId(), records);
                }
                ack.acknowledge();
                if (log.isInfoEnabled()) {
                    log.info("{} :: {} :: Skip sent force to acknowledged.",
                            getConnectorConfig().getName(), sourceStreamConfig.getGroupId());
                }
            } catch (Throwable ex) {
                log.error(String.format("%s :: %s :: Failed to skip sent acknowledge for %s",
                        getConnectorConfig().getName(), sourceStreamConfig.getGroupId(), ack), ex);
            }
            return;
        }

        // Add timing processed to savepoint topic sent metrics.
        // The benefit of not using lamda records is better use of arthas for troubleshooting during operation.
        final long writePointTimingBegin = System.nanoTime();

        // Wait for all processed records to be sent completed.
        //
        // Consume the shared groupId from the original data topic, The purpose of this design is to adapt to the scenario
        // of large message traffic, because if different groupIds are used to consume the original message, a large amount
        // of COPY traffic may be generated before processing, that is, bandwidth is wasted from Kafka broker to this Pod Kafka consumer.
        //
        if (getConnectorConfig().getQos().supportRetry(getConnectorConfig())) {
            final int initSize = writePointResults.size();
            final Set<WritePointResult> completedResults = new HashSet<>(writePointResults.size());
            for (int i = 0; !writePointResults.isEmpty(); i++) {
                final WritePointResult wpr = writePointResults.poll();

                // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                if (wpr.getFuture().isDone()) {
                    Object rm;
                    try {
                        rm = wpr.getFuture().get();
                        completedResults.add(wpr);

                        if (log.isDebugEnabled()) {
                            log.debug("{} :: {} :: Processed record metadata : {}",
                                    getConnectorConfig().getName(), sourceStreamConfig.getGroupId(), rm);
                        }
                        getEventPublisher().publishEvent(new CountMeterEvent(
                                MetricsName.checkpoint_write_success,
                                getBasedMeterTags()));

                    } catch (InterruptedException | CancellationException | ExecutionException ex) {
                        log.error("{} :: {} :: Unable not to getting process result.",
                                getConnectorConfig().getName(), sourceStreamConfig.getGroupId(), ex);

                        getEventPublisher().publishEvent(new CountMeterEvent(
                                MetricsName.checkpoint_write_failure,
                                getBasedMeterTags()));

                        if (ex instanceof ExecutionException) {
                            getConnectorConfig().getQos().retryIfFail(getConnectorConfig(),
                                    wpr.getRetryTimes(), () -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug("{} :: Retrying to savepoint : {}", getConnectorConfig().getName(), wpr);
                                        }
                                        final PointWriter pointWriter = getProcessStream().obtainChannelPointWriter(wpr.getRecord().getChannel());
                                        writePointResults.offer(pointWriter.writeAsync(getConnectorConfig(), wpr.getRecord(),
                                                wpr.getRetryTimes() + 1));
                                    });
                        }
                    }
                }

                // Batch flush according to write point results size.
                final int completedSize = completedResults.size();
                if (completedSize == initSize || i % max(completedSize / 4, 128) == 0) {
                    getProcessStream().flushWritePoints(completedResults);
                }

                Thread.yield(); // May give up the CPU
            }

            // e.g: According to the records of each partition, only submit the part of
            // this batch that has been successively successful from the earliest.
            if (getConnectorConfig().getQos().supportPreferAcknowledge(getConnectorConfig())) {
                preferAutoAcknowledge(completedResults);
            } else {
                // After the maximum retries, there may still be records of processing failures.
                // At this time, the ack commit is forced and the failures are ignored.
                final long acknowledgeTimingBegin = System.nanoTime();
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: {} :: Batch sent acknowledging ...", getConnectorConfig().getName(),
                                sourceStreamConfig.getGroupId());
                    }
                    ack.acknowledge();
                    if (log.isInfoEnabled()) {
                        log.info("{} :: {} :: Sent acknowledged.", getConnectorConfig().getName(),
                                sourceStreamConfig.getGroupId());
                    }
                    postAcknowledgeCountMeter(MetricsName.acknowledge_success, completedResults);
                } catch (Throwable ex) {
                    log.error(String.format("%s :: Failed to sent success acknowledge for %s",
                            sourceStreamConfig.getGroupId(), ack), ex);
                    postAcknowledgeCountMeter(MetricsName.acknowledge_failure, completedResults);
                } finally {
                    getEventPublisher().publishEvent(new TimingMeterEvent(
                            MetricsName.acknowledge_time,
                            StreamConnectMeter.DEFAULT_PERCENTILES,
                            Duration.ofNanos(System.nanoTime() - acknowledgeTimingBegin),
                            getBasedMeterTags(),
                            MetricsTag.ACK_KIND,
                            MetricsTag.ACK_KIND_VALUE_COMMIT));
                }
            }

            // Release the memory objects.
            completedResults.clear();
        }
        // If retry is not supported, it means that the SLA requirements
        // are low and data losses are allowed and commit it directly.
        else {
            final long acknowledgeTimingBegin = System.nanoTime();
            try {
                if (log.isDebugEnabled()) {
                    log.debug("{} :: {} :: Batch regardless of success or failure force acknowledging ...",
                            getConnectorConfig().getName(), sourceStreamConfig.getGroupId());
                }
                ack.acknowledge();
                if (log.isInfoEnabled()) {
                    log.info("{} :: {} :: Force sent to acknowledged.", getConnectorConfig().getName(),
                            sourceStreamConfig.getGroupId());
                }
                postAcknowledgeCountMeter(MetricsName.acknowledge_success, writePointResults);
            } catch (Throwable ex) {
                log.error(String.format("%s :: %s :: Failed to sent force acknowledge for %s",
                        getConnectorConfig().getName(), sourceStreamConfig.getGroupId(), ack), ex);
                postAcknowledgeCountMeter(MetricsName.acknowledge_failure, writePointResults);
            } finally {
                getEventPublisher().publishEvent(new TimingMeterEvent(
                        MetricsName.acknowledge_time,
                        StreamConnectMeter.DEFAULT_PERCENTILES,
                        Duration.ofNanos(System.nanoTime() - acknowledgeTimingBegin),
                        getBasedMeterTags(),
                        MetricsTag.ACK_KIND,
                        MetricsTag.ACK_KIND_VALUE_COMMIT));
            }
        }

        getEventPublisher().publishEvent(new TimingMeterEvent(
                MetricsName.checkpoint_write_time,
                StreamConnectMeter.DEFAULT_PERCENTILES,
                Duration.ofNanos(System.nanoTime() - writePointTimingBegin),
                getBasedMeterTags()));
    }

    private void postAcknowledgeCountMeter(MetricsName metrics,
                                           Collection<WritePointResult> writePointResults) {
        writePointResults.stream()
                .map(sr -> {
                    final MessageRecord<String, Object> r = sr.getRecord().getRecord();
                    if (r instanceof KafkaMessageRecord) {
                        final KafkaMessageRecord<String, Object> r0 = (KafkaMessageRecord<String, Object>) r;
                        return new TopicPartition(r0.getTopic(), r0.getPartition());
                    } else if (r instanceof DelegateMessageRecord) {
                        final KafkaMessageRecord<String, Object> r0 = (KafkaMessageRecord<String, Object>)
                                ((DelegateMessageRecord<String, Object>) r).getOriginal();
                        return new TopicPartition(r0.getTopic(), r0.getPartition());
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .distinct()
                .forEach(tp -> getEventPublisher().publishEvent(new CountMeterEvent(
                        metrics, getBasedMeterTags())));
    }

    /**
     * After the maximum number of retries, there may still be records that failed to process.
     * At this time, only the consecutively successful records starting from the earliest batch
     * of offsets will be submitted, and the offsets of other failed records will not be committed,
     * so as to strictly limit the processing failures that will not be submitted so that zero is not lost.
     *
     * @param completedResults completed sent results
     */
    private void preferAutoAcknowledge(Set<WritePointResult> completedResults) {
        // grouping and sorting.
        final Map<TopicPartition, List<KafkaMessageRecord<String, Object>>> partitionRecords =
                new HashMap<>(completedResults.size());
        for (WritePointResult writePointResult : completedResults) {
            final KafkaMessageRecord<String, Object> _record = (KafkaMessageRecord<String, Object>) writePointResult
                    .getRecord().getRecord();
            final TopicPartition topicPartition = new TopicPartition(_record.getTopic(), _record.getPartition());
            partitionRecords.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(_record);
        }

        // Find the maximum offset that increments consecutively for each partition.
        final Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
        for (Entry<TopicPartition, List<KafkaMessageRecord<String, Object>>> entry : partitionRecords.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            final List<KafkaMessageRecord<String, Object>> records = entry.getValue();
            // ASC sorting by offset.
            records.sort(Comparator.comparingLong(KafkaMessageRecord::getOffset));

            records.stream().mapToLong(KafkaMessageRecord::getOffset)
                    .reduce((prev, curr) -> curr == prev + 1 ? curr : prev)
                    .ifPresent(maxOffset -> {
                        partitionOffsets.put(topicPartition, new OffsetAndMetadata(maxOffset));
                    });
        }

        // Acknowledge to source kafka broker.
        final long acknowledgeTimingBegin = System.nanoTime();
        try {
            // see:org.apache.kafka.clients.consumer.internals.ConsumerCoordinator#onJoinComplete(int, String, String, ByteBuffer)
            // see:org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#doSendOffsets(Producer, Map)
            if (log.isDebugEnabled()) {
                log.debug("{} :: Prefer acknowledging offsets to transaction. - : {}",
                        sourceStreamConfig.getGroupId(), partitionOffsets);
            }
            this.acknowledgeProducerContainer.obtainFirstProducer().sendOffsetsToTransaction(partitionOffsets,
                    new ConsumerGroupMetadata(sourceStreamConfig.getGroupId()));
            if (log.isDebugEnabled()) {
                log.debug("{} :: Prefer acknowledged offsets to transaction. - : {}",
                        sourceStreamConfig.getGroupId(), partitionOffsets);
            }
        } catch (Throwable ex) {
            final String errmsg = String.format("%s :: Failed to prefer acknowledge offsets to transaction. - : %s",
                    sourceStreamConfig.getGroupId(), partitionOffsets);
            log.error(errmsg, ex);
            throw new StreamConnectException(errmsg, ex);
        } finally {
            getEventPublisher().publishEvent(new TimingMeterEvent(
                    MetricsName.acknowledge_time,
                    StreamConnectMeter.DEFAULT_PERCENTILES,
                    Duration.ofNanos(System.nanoTime() - acknowledgeTimingBegin),
                    getBasedMeterTags(),
                    MetricsTag.ACK_KIND,
                    MetricsTag.ACK_KIND_VALUE_SEND));
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaSourceStreamConfig extends SourceStreamConfig {
        public static final String TYPE_NAME = "KAFKA_SOURCE";

        private String topicPattern;
        private @Default Map<String, Object> consumerProps = new HashMap<>();

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public void validate() {
            super.validate();

            // Apply to default properties.
            DEFAULT_CONSUMER_PROPS.forEach((key, value) -> consumerProps.putIfAbsent(key, value));

            Assert2.hasTextOf(topicPattern, "topicPattern");
            requireNonNull(getConsumerProps().get(ConsumerConfig.GROUP_ID_CONFIG),
                    "'group.id' must not be null");
            requireNonNull(consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    "'bootstrap.servers' must not be null");

            optimizeProperties();
        }

        private void optimizeProperties() {
            // The filter message handler is internally hardcoded to use JsonNode.
            final String oldKeyDeserializer = (String) getConsumerProps().get(KEY_DESERIALIZER_CLASS_CONFIG);
            getConsumerProps().put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            log.info("Optimized source '{}' from '{}' to '{}' of groupId: '{}'", KEY_DESERIALIZER_CLASS_CONFIG,
                    oldKeyDeserializer, getConsumerProps().get(KEY_DESERIALIZER_CLASS_CONFIG), getGroupId());

            final String oldValueDeserializer = (String) getConsumerProps().get(VALUE_DESERIALIZER_CLASS_CONFIG);
            getConsumerProps().put(VALUE_DESERIALIZER_CLASS_CONFIG, ObjectNodeDeserializer.class.getName());
            log.info("Optimized source '{}' from '{}' to '{}' of groupId: '{}'", VALUE_DESERIALIZER_CLASS_CONFIG,
                    oldValueDeserializer, getConsumerProps().get(VALUE_DESERIALIZER_CLASS_CONFIG), getGroupId());

            // Need auto create the filtered topic by channel. (broker should also be set to allow)
            final Boolean oldAutoCreateTopics = (Boolean) getConsumerProps().get(ALLOW_AUTO_CREATE_TOPICS_CONFIG);
            getConsumerProps().put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);
            log.info("Optimized source '{}' from '{}' to '{}' of groupId: '{}'", ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                    oldAutoCreateTopics, getConsumerProps().get(ALLOW_AUTO_CREATE_TOPICS_CONFIG), getGroupId());

            // Mandatory manual commit.
            final Boolean oldEnableAutoCommit = (Boolean) getConsumerProps().get(ENABLE_AUTO_COMMIT_CONFIG);
            getConsumerProps().put(ENABLE_AUTO_COMMIT_CONFIG, false);
            log.info("Optimized source '{}' from '{}' to '{}' of groupId: '{}'", ENABLE_AUTO_COMMIT_CONFIG,
                    oldEnableAutoCommit, getConsumerProps().get(ENABLE_AUTO_COMMIT_CONFIG), getGroupId());

            // TODO checking by merge to sources and filter with connector
            // Should be 'max.poll.records' equals to filter executor queue size.
            // final Object originalMaxPollRecords = getConsumerProps().get(MAX_POLL_RECORDS_CONFIG);
            // getConsumerProps().put(MAX_POLL_RECORDS_CONFIG,
            //         String.valueOf(getFilter().getProcessProps().getSharedExecutorQueueSize()));
            // log.info("Optimized '{}' from '{}' to '{}' of connector.source groupId: '{}'",
            //         MAX_POLL_RECORDS_CONFIG, originalMaxPollRecords, getFilter().getProcessProps()
            //                 .getSharedExecutorQueueSize(), getGroupId());
        }

        public String getGroupId() {
            return (String) getConsumerProps().get(ConsumerConfig.GROUP_ID_CONFIG);
        }

        public static final Map<String, Object> DEFAULT_CONSUMER_PROPS = new HashMap<String, Object>() {
            {
                //put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                //put(ConsumerConfig.CLIENT_ID_CONFIG, "");
                put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
                put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // none,latest,earliest
                put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
                put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
                put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
                put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 50 * 1024 * 1024);
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                // Must be 'ObjectDeserializer' for filter message handler.
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ObjectNodeDeserializer.class.getName());
            }
        };
    }

    public static class KafkaMessageRecord<K, V> implements MessageRecord<K, V> {
        private final ConsumerRecord<K, V> record;
        private volatile Map<String, V> _metadata;

        public KafkaMessageRecord(ConsumerRecord<K, V> record) {
            this.record = record;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<String, V> getMetadata() {
            if (isNull(_metadata)) {
                synchronized (this) {
                    if (isNull(_metadata)) {
                        _metadata = (Map<String, V>) KafkaUtil.toHeaderStringMap(record.headers());
                    }
                }
            }
            return _metadata;
        }

        @Override
        public K getKey() {
            return record.key();
        }

        @Override
        public V getValue() {
            return record.value();
        }

        @Override
        public long getTimestamp() {
            return record.timestamp();
        }

        public String getTopic() {
            return record.topic();
        }

        public int getPartition() {
            return record.partition();
        }

        public long getOffset() {
            return record.offset();
        }

        public static <K, V> List<KafkaMessageRecord<K, V>> from(
                ConsumerRecords<K, V> records) {
            return safeList(IterableUtils.toList(records))
                    .stream()
                    .map(KafkaMessageRecord::new)
                    .collect(toList());
        }

        public static <K, V> List<KafkaMessageRecord<K, V>> from(
                List<ConsumerRecord<K, V>> records) {
            return safeList(records)
                    .stream()
                    .map(KafkaMessageRecord::new)
                    .collect(toList());
        }
    }

    public static class KafkaSourceStreamProvider extends SourceStreamProvider {
        public static final String TYPE_NAME = getStreamProviderTypeName(KafkaSourceStreamConfig.TYPE_NAME);

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public KafkaSourceStreamBootstrap create(@NotNull final StreamContext context,
                                                 @NotNull final SourceStreamConfig sourceStreamConfig,
                                                 @NotNull final CachingChannelRegistry registry) {
            // Create to kafka source stream.
            final KafkaSourceStream sourceStream = new KafkaSourceStream(context,
                    (KafkaSourceStreamConfig) sourceStreamConfig);

            // Create to kafka source stream wrapper(bootstrap).
            return new KafkaSourceStreamBootstrap(sourceStream, sourceStream.getInternalTask());
        }
    }

}
