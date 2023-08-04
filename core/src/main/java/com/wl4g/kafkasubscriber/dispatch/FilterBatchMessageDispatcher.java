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

package com.wl4g.kafkasubscriber.dispatch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeEnginePipelineConfig;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeExecutorConfig;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeSourceConfig;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.exception.GiveUpRetryExecutionException;
import com.wl4g.kafkasubscriber.exception.KafkaSubscribeException;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter.MetricsName;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter.MetricsTag;
import com.wl4g.kafkasubscriber.util.Crc32Util;
import com.wl4g.kafkasubscriber.util.NamedThreadFactory;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * The {@link FilterBatchMessageDispatcher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class FilterBatchMessageDispatcher extends AbstractBatchMessageDispatcher {
    private final SubscribeSourceConfig subscribeSourceConfig;
    private final ThreadPoolExecutor sharedNonSequenceExecutor;
    private final List<ThreadPoolExecutor> isolationSequenceExecutors;
    private final ISubscribeFilter subscribeFilter;
    private final Map<String, List<Producer<String, String>>> checkpointProducersMap;
    private final Producer<String, String> acknowledgeProducer;

    public FilterBatchMessageDispatcher(KafkaSubscribeConfiguration config,
                                        SubscribeEnginePipelineConfig pipelineConfig,
                                        SubscribeSourceConfig subscribeSourceConfig,
                                        SubscribeEngineCustomizer customizer,
                                        CachingSubscriberRegistry registry,
                                        String topicDesc,
                                        String groupId,
                                        ISubscribeFilter filter,
                                        Producer<String, String> acknowledgeProducer) {
        super(config, pipelineConfig, customizer, registry, topicDesc, groupId);
        this.subscribeSourceConfig = Assert2.notNullOf(subscribeSourceConfig, "sourceConfig");
        this.subscribeFilter = Assert2.notNullOf(filter, "filter");
        this.acknowledgeProducer = Assert2.notNullOf(acknowledgeProducer, "acknowledgeProducer");
        this.checkpointProducersMap = new ConcurrentHashMap<>(8);

        // Create the shared filter single executor.
        final SubscribeExecutorConfig processConfig = pipelineConfig.getParsedFilter().getFilterConfig().getExecutorProps();
        this.sharedNonSequenceExecutor = new ThreadPoolExecutor(processConfig.getSharedExecutorThreadPoolSize(),
                processConfig.getSharedExecutorThreadPoolSize(),
                0L, TimeUnit.MILLISECONDS,
                // TODO support bounded queue
                new LinkedBlockingQueue<>(processConfig.getSharedExecutorQueueSize()),
                new NamedThreadFactory("shared-".concat(getClass().getSimpleName())));
        if (processConfig.isExecutorWarmUp()) {
            this.sharedNonSequenceExecutor.prestartAllCoreThreads();
        }

        // Create the sequence filter executors.
        this.isolationSequenceExecutors = synchronizedList(new ArrayList<>(processConfig.getSequenceExecutorsMaxCountLimit()));
        for (int i = 0; i < processConfig.getSequenceExecutorsMaxCountLimit(); i++) {
            final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    // TODO support bounded queue
                    new LinkedBlockingQueue<>(processConfig.getSequenceExecutorsPerQueueSize()),
                    new NamedThreadFactory("sequence-".concat(getClass().getSimpleName())));
            if (processConfig.isExecutorWarmUp()) {
                executor.prestartAllCoreThreads();
            }
            this.isolationSequenceExecutors.add(executor);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();

        try {
            log.info("{} :: Closing shared non filter executor...", groupId);
            this.sharedNonSequenceExecutor.shutdown();
            log.info("{} :: Closed shared non filter executor.", groupId);
        } catch (Throwable ex) {
            log.error(String.format("%s :: Failed to close shared filter executor.", groupId), ex);
        }

        this.isolationSequenceExecutors.forEach(executor -> {
            try {
                log.info("{} :: Closing filter executor {}...", groupId, executor);
                executor.shutdown();
                log.info("{} :: Closed filter executor {}.", groupId, executor);
            } catch (Throwable ex) {
                log.error(String.format("%s :: Failed to close filter executor %s.", groupId, executor), ex);
            }
        });

        this.checkpointProducersMap.forEach((tenantId, producers) -> {
            try {
                producers.parallelStream().forEach(producer -> {
                    log.info("{} :: Closing kafka producer of tenantId : {}...", groupId, tenantId);
                    producer.close();
                    log.info("{} :: Closed kafka producer of tenantId : {}.", groupId, tenantId);
                });
            } catch (Throwable ex) {
                log.error(String.format("%s :: Failed to close kafka producers of tenant: %s.", groupId, tenantId), ex);
            }
        });
    }

    @Override
    public void doOnMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack) {
        final Set<CheckpointSentResult> sentResults = doParallelFilterAndSendAsync(records);

        // If the sending result set is empty, it indicates that checkpoint produce is not enabled and send to filtered checkpoint topic.
        if (Objects.isNull(sentResults)) {
            try {
                log.debug("{} :: Sink is disabled, skip sent acknowledge... records : {}", groupId, records);
                ack.acknowledge();
                log.info("{} :: Skip sent force to acknowledged.", groupId);
            } catch (Throwable ex) {
                log.error(String.format("%s :: Failed to skip sent acknowledge for %s", groupId, ack), ex);
            }
            return;
        }

        // Add timing filtered to checkpoint topic sent metrics.
        // The benefit of not using lamda records is better use of arthas for troubleshooting during operation.
        final Timer checkpointTimer = addTimerMetrics(MetricsName.filter_records_time,
                subscribeSourceConfig.getTopicPattern().toString(), null, groupId, null);
        final long checkpointBeginTime = System.nanoTime();

        // Wait for all filtered records to be sent completed.
        //
        // Consume the shared groupId from the original data topic, The purpose of this design is to adapt to the scenario
        // of large message traffic, because if different groupIds are used to consume the original message, a large amount
        // of COPY traffic may be generated before filtering, that is, bandwidth is wasted from Kafka broker to this Pod Kafka consumer.
        //
        if (pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint().getQos().isAnyRetriesAtMostOrStrictly()) {
            final Set<CheckpointSentResult> completedSentResults = new HashSet<>(sentResults.size());
            while (sentResults.size() > 0) {
                final Iterator<CheckpointSentResult> it = sentResults.iterator();
                while (it.hasNext()) {
                    final CheckpointSentResult sr = it.next();
                    // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                    if (sr.getFuture().isDone()) {
                        RecordMetadata rm = null;
                        try {
                            rm = sr.getFuture().get();
                            completedSentResults.add(sr);

                            addCounterMetrics(MetricsName.filter_records_success,
                                    sr.getRecord().getRecord().topic(),
                                    sr.getRecord().getRecord().partition(), groupId, null);
                        } catch (InterruptedException | CancellationException | ExecutionException ex) {
                            log.error("{} :: Unable not to getting sent result.", groupId, ex);

                            addCounterMetrics(MetricsName.filter_records_failure,
                                    sr.getRecord().getRecord().topic(),
                                    sr.getRecord().getRecord().partition(), groupId, null);

                            if (shouldGiveUpRetry(sr.getRetryBegin(), sr.getRetryTimes())) {
                                continue; // give up and lose
                            }
                            sentResults.add(doCheckpointSendAsync(sr.getRecord(), sr.getRetryBegin(),
                                    sr.getRetryTimes() + 1));
                        } finally {
                            it.remove();
                        }
                        log.debug("{} :: Sent to filtered record(metadata) result : {}", groupId, rm);
                    }
                }
                Thread.yield(); // May give up the CPU
            }
            // According to the records of each partition, only submit the part of
            // this batch that has been successively successful from the earliest.
            if (pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint().getQos().isRetriesAtMostStrictly()) {
                preferredAcknowledgeWithRetriesAtMostStrictly(completedSentResults);
            } else {
                // After the maximum retries, there may still be records of processing failures.
                // At this time, the ack commit is forced and the failures are ignored.
                final Timer acknowledgeTimer = addTimerMetrics(MetricsName.acknowledge_time,
                        subscribeSourceConfig.getTopicPattern().toString(), null, groupId, null,
                        MetricsTag.ACK_KIND, MetricsTag.ACK_KIND_VALUE_COMMIT);
                acknowledgeTimer.record(() -> {
                    try {
                        log.debug("{} ;: Batch sent acknowledging ...", groupId);
                        ack.acknowledge();
                        log.info("{} :: Sent acknowledged.", groupId);

                        addAcknowledgeCounterMetrics(MetricsName.acknowledge_success, completedSentResults);
                    } catch (Throwable ex) {
                        log.error(String.format("%s :: Failed to sent success acknowledge for %s", groupId, ack), ex);

                        addAcknowledgeCounterMetrics(MetricsName.acknowledge_failure, completedSentResults);
                    }
                });
            }
        } else {
            final Timer acknowledgeTimer = addTimerMetrics(MetricsName.acknowledge_time,
                    subscribeSourceConfig.getTopicPattern().toString(), null, groupId, null,
                    MetricsTag.ACK_KIND, MetricsTag.ACK_KIND_VALUE_COMMIT);
            acknowledgeTimer.record(() -> {
                try {
                    log.debug("{} :: Batch regardless of success or failure force acknowledging ...", groupId);
                    ack.acknowledge();
                    log.info("{} :: Force sent to acknowledged.", groupId);

                    addAcknowledgeCounterMetrics(MetricsName.acknowledge_success, sentResults);
                } catch (Throwable ex) {
                    log.error(String.format("%s :: Failed to sent force acknowledge for %s",
                            groupId, ack), ex);

                    addAcknowledgeCounterMetrics(MetricsName.acknowledge_failure, sentResults);
                }
            });
        }

        checkpointTimer.record(Duration.ofNanos(System.nanoTime() - checkpointBeginTime));
    }

    private void addAcknowledgeCounterMetrics(MetricsName metrics,
                                              Set<CheckpointSentResult> checkpointSentResults) {
        checkpointSentResults.stream()
                .map(sr -> new TopicPartition(sr.getRecord().getRecord().topic(),
                        sr.getRecord().getRecord().partition()))
                .distinct().forEach(tp -> addCounterMetrics(metrics,
                        tp.topic(), tp.partition(), groupId, null));
    }

    private List<SubscriberRecord> matchToSubscribesRecords(List<ConsumerRecord<String, ObjectNode>> records) {
        final List<SubscriberInfo> subscribers = customizer.loadSubscribers(pipelineConfig.getName(),
                SubscriberInfo.builder().build());

        // Merge subscription server configurations and update to filters.
        // Notice: According to the consumption filtering model design, it is necessary to share groupId
        // consumption for unified processing, So here, all subscriber filtering rules should be merged.
        subscribeFilter.updateConfigWithMergeSubscribers(subscribers,
                Duration.ofMillis(subscribeSourceConfig.getMatchToSubscriberUpdateDelayTime()).toNanos());

        return records.parallelStream().map(r -> safeList(subscribers).stream()
                .filter(s -> customizer.matchSubscriberRecord(pipelineConfig.getName(), s, r)).limit(1)
                .findFirst()
                .map(s -> new SubscriberRecord(s, r)).orElseGet(() -> {
                    log.warn(String.format("%s :: No matched subscriber for headers: %s, key: %s, value: %s", groupId, r.headers(), r.key(), r.value()));
                    return null;
                })).filter(Objects::nonNull).collect(toList());
    }

    private Set<CheckpointSentResult> doParallelFilterAndSendAsync(List<ConsumerRecord<String, ObjectNode>> records) {
        // Match wrap to subscriber rules records.
        final List<SubscriberRecord> subscriberRecords = matchToSubscribesRecords(records);

        // Add timing filter metrics.
        // The benefit of not using lamda records is better use of arthas for troubleshooting during operation.
        final Timer filterTimer = addTimerMetrics(MetricsName.filter_records_time,
                subscribeSourceConfig.getTopicPattern(), null, groupId, null);
        final long filterBeginTime = System.nanoTime();

        // Execute custom filters in parallel them to different send executor queues.
        final List<FilteredResult> filteredResults = safeList(subscriberRecords).stream()
                .map(sr -> new FilteredResult(sr,
                        determineFilterExecutor(sr).submit(() -> {
                            final boolean matched = subscribeFilter.doMatch(sr.getSubscriber(), sr.getRecord());
                            final ConsumerRecord<String, ObjectNode> mappedRecords = matched ?
                                    subscribeFilter.doMap(sr.getSubscriber(), sr.getRecord()) : sr.getRecord();
                            return new FilteredFutureResult(matched, mappedRecords);
                        }), System.nanoTime(), 1))
                .collect(toList());

        Set<CheckpointSentResult> checkpointSentResults = null;
        // If the sink is enabled, the filtered results will be sent to the checkpoint topic.
        if (Objects.nonNull(pipelineConfig.getParsedSink())) {
            checkpointSentResults = new HashSet<>(filteredResults.size());
        } else {
            log.info("{} :: Sink is disabled, skip to send filtered results to kafka. sr : {}",
                    groupId, subscriberRecords);
        }

        // Wait for all parallel filtered results to be completed.
        while (filteredResults.size() > 0) {
            final Iterator<FilteredResult> it = filteredResults.iterator();
            while (it.hasNext()) {
                final FilteredResult fr = it.next();
                // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                if (fr.getFuture().isDone()) {
                    FilteredFutureResult ffr = null;
                    try {
                        ffr = fr.getFuture().get();

                        addCounterMetrics(MetricsName.filter_records_success,
                                fr.getRecord().getRecord().topic(),
                                fr.getRecord().getRecord().partition(), groupId, null);
                    } catch (InterruptedException | CancellationException ex) {
                        log.error("{} :: Unable to getting subscribe filter result.", groupId, ex);

                        addCounterMetrics(MetricsName.filter_records_failure,
                                fr.getRecord().getRecord().topic(),
                                fr.getRecord().getRecord().partition(), groupId, null);

                        if (pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint().getQos().isAnyRetriesAtMostOrStrictly()) {
                            if (shouldGiveUpRetry(fr.getRetryBegin(), fr.getRetryTimes())) {
                                continue; // give up and lose
                            }
                            enqueueFilteredExecutor(fr, filteredResults);
                        }
                    } catch (ExecutionException ex) {
                        log.error("{} :: Unable not to getting subscribe filter result.", groupId, ex);
                        final Throwable reason = ExceptionUtils.getRootCause(ex);
                        // User needs to give up trying again.
                        if (reason instanceof GiveUpRetryExecutionException) {
                            log.warn("{} :: User ask to give up re-trying again filter. fr : {}, reason : {}",
                                    groupId, fr, reason.getMessage());
                        } else {
                            if (pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint().getQos().isAnyRetriesAtMostOrStrictly()) {
                                if (shouldGiveUpRetry(fr.getRetryBegin(), fr.getRetryTimes())) {
                                    continue; // give up and lose
                                }
                                enqueueFilteredExecutor(fr, filteredResults);
                            }
                        }
                    } finally {
                        it.remove();
                    }
                    if (Objects.nonNull(ffr) && ffr.isMatched()) {
                        // Send to filtered topic and add sent future If necessary.
                        if (Objects.nonNull(checkpointSentResults)) {
                            // Replace to mapped record(eg: data permission filtering).
                            fr.getRecord().setRecord(ffr.getRecord());
                            checkpointSentResults.add(doCheckpointSendAsync(fr.getRecord(), System.nanoTime(), 0));
                        }
                    }
                }
            }
            Thread.yield(); // May give up the CPU
        }
        filterTimer.record(Duration.ofNanos(System.nanoTime() - filterBeginTime));

        // Flush all producers to ensure that all records in this batch are committed.
        safeList(checkpointSentResults).stream().map(CheckpointSentResult::getProducer).forEach(Producer::flush);

        return checkpointSentResults;
    }

    private ThreadPoolExecutor determineTaskExecutor(String subscriberId, boolean isSequence, String key) {
        ThreadPoolExecutor executor = this.sharedNonSequenceExecutor;
        if (isSequence) {
            //final int mod = (int) subscriber.getId();
            final int mod = (int) Crc32Util.compute(key);
            final int index = isolationSequenceExecutors.size() % mod;
            executor = isolationSequenceExecutors.get(index);
            log.debug("{} :: {} :: determined isolation sequence executor index : {}, mod : {}",
                    groupId, subscriberId, index, mod);
        }
        return executor;
    }

    private void enqueueFilteredExecutor(FilteredResult fr, List<FilteredResult> filteredResults) {
        log.info("{} :: Re-enqueue Requeue and retry filter execution. fr : {}", groupId, fr);
        final SubscriberRecord sr = fr.getRecord();
        filteredResults.add(new FilteredResult(fr.getRecord(),
                determineFilterExecutor(fr.getRecord()).submit(() -> {
                    final boolean matched = subscribeFilter.doMatch(sr.getSubscriber(), sr.getRecord());
                    final ConsumerRecord<String, ObjectNode> mappedRecords = matched ?
                            subscribeFilter.doMap(sr.getSubscriber(), sr.getRecord()) : sr.getRecord();
                    return new FilteredFutureResult(matched, mappedRecords);
                }), fr.getRetryBegin(), fr.getRetryTimes() + 1));
    }

    private ThreadPoolExecutor determineFilterExecutor(SubscriberRecord record) {
        final SubscriberInfo subscriber = record.getSubscriber();
        final String key = record.getRecord().key();
        return determineTaskExecutor(subscriber.getId(), subscriber.getSettings().getIsSequence(), key);
    }

    private Producer<String, String> determineKafkaProducer(SubscriberInfo subscriber, String key) {
        final List<Producer<String, String>> checkpointProducers = obtainFilteredCheckpointProducers(subscriber);
        final int producerSize = checkpointProducers.size();
        Producer<String, String> producer = checkpointProducers.get(RandomUtils.nextInt(0, producerSize));
        if (Objects.nonNull(subscriber.getSettings().getIsSequence()) && subscriber.getSettings().getIsSequence()) {
            //final int mod = (int) subscriber.getId();
            final int mod = (int) Crc32Util.compute(key);
            final int index = producerSize % mod;
            producer = checkpointProducers.get(index);
            log.debug("{} :: determined send isolation sequence producer index : {}, mod : {}, subscriber : {}",
                    groupId, index, mod, subscriber.getId());
        }
        if (Objects.isNull(producer)) {
            throw new KafkaSubscribeException(String.format("%s :: Could not getting producer by subscriber: %s, key: %s",
                    groupId, subscriber.getId(), key));
        }
        log.debug("{} :: Using kafka producer by subscriber: {}, key: {}", groupId, subscriber.getId(), key);
        return producer;
    }

    // Create the internal filtered checkpoint producers.
    private List<Producer<String, String>> obtainFilteredCheckpointProducers(SubscriberInfo subscriber) {
        final int maxCountLimit = pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint().getProducerMaxCountLimit();

        return checkpointProducersMap.computeIfAbsent(subscriber.getTenantId(), tenantId -> {
            log.info("{} :: Creating filtered checkpoint producers...", groupId);

            // Find the source config by subscriber tenantId.
            final SubscribeSourceConfig tenantSourceConfig =
                    customizer.loadSourceByTenant(pipelineConfig.getName(), subscriber.getTenantId());
            if (Objects.isNull(tenantSourceConfig)) {
                throw new KafkaSubscribeException(String.format("%s :: Could not found source config by tenantId: %s",
                        groupId, subscriber.getTenantId()));
            }

            final Properties producerProps = (Properties) pipelineConfig.getParsedFilter()
                    .getFilterConfig().getCheckpoint().getProducerProps().clone();
            producerProps.putIfAbsent(BOOTSTRAP_SERVERS_CONFIG, tenantSourceConfig.getRequiredBootstrapServers());
            // TODO support more serializer(e.g bytes,protobuf,arrow)
            producerProps.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Create to filtered checkpoint producers.
            final KafkaProducerBuilder builder = new KafkaProducerBuilder(producerProps);
            final List<Producer<String, String>> producers = new ArrayList<>(maxCountLimit);
            for (int i = 0; i < maxCountLimit; i++) {
                producers.add(builder.buildProducer());
            }

            return producers;
        });
    }

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    private CheckpointSentResult doCheckpointSendAsync(SubscriberRecord record, long retryBegin, int retryTimes) {
        final SubscriberInfo subscriber = record.getSubscriber();
        final String key = record.getRecord().key();
        final ObjectNode value = record.getRecord().value();

        final Producer<String, String> producer = determineKafkaProducer(subscriber, key);
        final String filteredTopic = customizer.generateCheckpointTopic(pipelineConfig.getName(),
                pipelineConfig.getParsedFilter().getFilterConfig().getTopicPrefix(), subscriber.getId());

        final ProducerRecord<String, String> _record = new ProducerRecord<>(filteredTopic, key, value.toString());
        // Notice: Hand down the subscriber metadata of each record to the downstream.
        _record.headers().add(new RecordHeader(KEY_SUBSCRIBER_ID, String.valueOf(subscriber.getId()).getBytes()));
        _record.headers().add(new RecordHeader(KEY_IS_SEQUENCE, String.valueOf(subscriber.getSettings().getIsSequence()).getBytes()));

        return new CheckpointSentResult(record, producer, producer.send(_record), retryBegin, retryTimes);
    }

    /**
     * After the maximum number of retries, there may still be records that failed to process.
     * At this time, only the consecutively successful records starting from the earliest batch
     * of offsets will be submitted, and the offsets of other failed records will not be committed,
     * so as to strictly limit the processing failures that will not be submitted so that zero is not lost.
     *
     * @param completedCheckpointSentResults completed sent results
     */
    private void preferredAcknowledgeWithRetriesAtMostStrictly(Set<CheckpointSentResult> completedCheckpointSentResults) {
        // grouping and sorting.
        final Map<TopicPartition, List<ConsumerRecord<String, ObjectNode>>> partitionRecords = new HashMap<>(completedCheckpointSentResults.size());
        for (CheckpointSentResult checkpointSentResult : completedCheckpointSentResults) {
            final ConsumerRecord<String, ObjectNode> consumerRecord = checkpointSentResult.getRecord().getRecord();
            final TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            partitionRecords.computeIfAbsent(topicPartition, k -> new ArrayList<>()).add(consumerRecord);
        }
        // Find the maximum offset that increments consecutively for each partition.
        final Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, List<ConsumerRecord<String, ObjectNode>>> entry : partitionRecords.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            final List<ConsumerRecord<String, ObjectNode>> records = entry.getValue();
            // ASC sorting by offset.
            records.sort(Comparator.comparingLong(ConsumerRecord::offset));

            records.stream().mapToLong(ConsumerRecord::offset)
                    .reduce((prev, curr) -> curr == prev + 1 ? curr : prev)
                    .ifPresent(maxOffset -> {
                        partitionOffsets.put(topicPartition, new OffsetAndMetadata(maxOffset));
                    });
        }
        // Acknowledge to source kafka broker.
        final Timer acknowledgeSentTimer = addTimerMetrics(MetricsName.acknowledge_time,
                subscribeSourceConfig.getTopicPattern().toString(), null, groupId, null,
                MetricsTag.ACK_KIND, MetricsTag.ACK_KIND_VALUE_SEND);
        acknowledgeSentTimer.record(() -> {
            try {
                // see:org.apache.kafka.clients.consumer.internals.ConsumerCoordinator#onJoinComplete(int, String, String, ByteBuffer)
                // see:org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#doSendOffsets(Producer, Map)
                log.debug("{} :: Preferred acknowledging offsets to transaction. - : {}", groupId, partitionOffsets);
                this.acknowledgeProducer.sendOffsetsToTransaction(partitionOffsets, new ConsumerGroupMetadata(groupId));
                log.debug("{} :: Preferred acknowledged offsets to transaction. - : {}", groupId, partitionOffsets);
            } catch (Throwable ex) {
                log.error(String.format("%s :: Failed to preferred acknowledge offsets to transaction. - : %s",
                        groupId, partitionOffsets), ex);
            }
        });
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    public static class SubscriberRecord {
        private SubscriberInfo subscriber;
        private ConsumerRecord<String, ObjectNode> record;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubscriberRecord that = (SubscriberRecord) o;
            return Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            return Objects.hash(record);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class FilteredResult {
        private SubscriberRecord record;
        private Future<FilteredFutureResult> future;
        private long retryBegin;
        private int retryTimes;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class FilteredFutureResult {
        private boolean matched;
        private ConsumerRecord<String, ObjectNode> record;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class CheckpointSentResult {
        private SubscriberRecord record;
        private Producer<String, String> producer;
        private Future<RecordMetadata> future;
        private long retryBegin;
        private int retryTimes;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CheckpointSentResult that = (CheckpointSentResult) o;
            return Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            return Objects.hash(record);
        }
    }

}
