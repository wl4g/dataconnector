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

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.exception.GiveUpRetryExecutionException;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import com.wl4g.kafkasubscriber.util.Crc32Util;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;

/**
 * The {@link FilterBatchMessageDispatcher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class FilterBatchMessageDispatcher extends AbstractBatchMessageDispatcher {
    private final List<KafkaProducer<String, String>> filteredCheckpointProducers;
    private ISubscribeFilter subscribeFilter;

    public FilterBatchMessageDispatcher(ApplicationContext context,
                                        KafkaSubscriberProperties.SubscribePipelineProperties pipelineConfig,
                                        SubscribeFacade subscribeFacade,
                                        SubscriberRegistry subscriberRegistry,
                                        String groupId) {
        super(context, pipelineConfig, pipelineConfig.getFilter().getProcessProps(), subscribeFacade, subscriberRegistry, groupId);

        // Create the internal filtered checkpoint producers.
        this.filteredCheckpointProducers = synchronizedList(new ArrayList<>(pipelineConfig.getFilter().getProcessProps().getCheckpointProducerMaxCountLimit()));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // Create custom subscribe filter. (Each processing pipeline uses different custom filter instances)
        this.subscribeFilter = obtainSubscribeFilter();
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.filteredCheckpointProducers.forEach(producer -> {
            try {
                log.info("Closing kafka producer {}...", producer);
                producer.close();
                log.info("Closed kafka producer {}.", producer);
            } catch (Throwable ex) {
                log.error(String.format("Failed to close kafka producer %s.", producer), ex);
            }
        });
    }

    @Override
    public void doOnMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack) {
        // final List<SentResult> sentResults = safeList(subscriberRecords).parallelStream()
        //        .filter(sr -> subscribeFilter.apply(sr.getSubscriber(), sr.getRecord().value()))
        //        .map(this::doSendToFilteredAsync).collect(toList());

        final Set<SentResult> sentResults = doParallelCustomFilterAndSendAsync(records);

        // If the sending result set is empty, it indicates that checkpoint produce is not enabled and send to the kafka filtered topic.
        if (Objects.isNull(sentResults)) {
            return;
        }

        // Add timing filter sent metrics.
        // The benefit of not using LAMDA records is better use of arthas for troubleshooting during operation.
        final Timer filterSentTimer = addTimerMetrics(SubscribeMeter.MetricsName.filter_records_time,
                pipelineConfig.getSource().getTopicPattern().toString(), groupId);
        final long filterSentBeginTime = System.nanoTime();

        // Wait for all filtered records to be sent completed.
        //
        // Consume the shared groupId from the original data topic, The purpose of this design is to adapt to the scenario
        // of large message traffic, because if different groupIds are used to consume the original message, a large amount
        // of COPY traffic may be generated before filtering, that is, bandwidth is wasted from Kafka broker to this Pod Kafka consumer.
        //
        if (processConfig.getCheckpointQoS().isMaxRetriesOrStrictly()) {
            while (sentResults.size() > 0) {
                final Iterator<SentResult> it = sentResults.iterator();
                while (it.hasNext()) {
                    final SentResult sr = it.next();
                    // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                    if (sr.getFuture().isDone()) {
                        RecordMetadata rm = null;
                        try {
                            rm = sr.getFuture().get();
                            addCounterMetrics(SubscribeMeter.MetricsName.filter_records_sent_success, sr.getRecord().getRecord().topic(),
                                    sr.getRecord().getRecord().partition(), groupId);
                        } catch (InterruptedException | CancellationException | ExecutionException ex) {
                            log.error("Unable not to getting sent result.", ex);
                            addCounterMetrics(SubscribeMeter.MetricsName.filter_records_sent_failure, sr.getRecord().getRecord().topic(),
                                    sr.getRecord().getRecord().partition(), groupId);
                            if (shouldGiveUpRetry(sr.getRetryBegin(), sr.getRetryTimes())) {
                                break; // give up and lose
                            }
                            sentResults.add(doSendToFilteredAsync(sr.getRecord(), sr.getRetryBegin(), sr.getRetryTimes() + 1));
                        } finally {
                            it.remove();
                        }
                        log.debug("Sent to filtered record(metadata) result : {}", rm);
                    }
                }
                Thread.yield(); // May give up the CPU
            }
            try {
                log.debug("Batch sent acknowledging ...");
                ack.acknowledge();
                log.info("Sent acknowledged.");
            } catch (Throwable ex) {
                log.error(String.format("Failed to sent success acknowledge for %s", ack), ex);
            }
        } else {
            try {
                log.debug("Batch regardless of success or failure force acknowledging ...");
                ack.acknowledge();
                log.info("Force to acknowledged.");
            } catch (Throwable ex) {
                log.error(String.format("Failed to sent force acknowledge for %s", ack), ex);
            }
        }

        filterSentTimer.record(Duration.ofNanos(System.nanoTime() - filterSentBeginTime));
    }

    private Set<SentResult> doParallelCustomFilterAndSendAsync(List<ConsumerRecord<String, ObjectNode>> records) {
        // Match wrap to subscriber rules records.
        final List<SubscriberRecord> subscriberRecords = matchWrapToSubscribesRecords(records);

        // Add timing filter metrics.
        // The benefit of not using LAMDA records is better use of arthas for troubleshooting during operation.
        final Timer filterTimer = addTimerMetrics(SubscribeMeter.MetricsName.filter_records_time,
                pipelineConfig.getSource().getTopicPattern().toString(), groupId);
        final long filterBeginTime = System.nanoTime();

        // Execute custom filters in parallel them to different send executor queues.
        final List<FilteredResult> filteredResults = safeList(subscriberRecords).stream()
                .map(sr -> new FilteredResult(sr, determineFilterExecutor(sr)
                        .submit(() -> subscribeFilter.apply(sr)), System.nanoTime(), 1)).collect(toList());

        Set<SentResult> sentResults = null;
        if (pipelineConfig.getSink().isEnable()) {
            sentResults = new HashSet<>(filteredResults.size());
        } else {
            log.info("Sink is disabled, skip to send filtered results to kafka. sr : {}", subscriberRecords);
        }

        // Wait for all parallel filtered results to be completed.
        while (filteredResults.size() > 0) {
            final Iterator<FilteredResult> it = filteredResults.iterator();
            while (it.hasNext()) {
                final FilteredResult fr = it.next();
                // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                if (fr.getFuture().isDone()) {
                    Boolean matched = null;
                    try {
                        matched = fr.getFuture().get();
                        addCounterMetrics(SubscribeMeter.MetricsName.filter_records_success, fr.getRecord().getRecord().topic(),
                                fr.getRecord().getRecord().partition(), groupId);
                    } catch (InterruptedException | CancellationException ex) {
                        log.error("Unable to getting subscribe filter result.", ex);
                        addCounterMetrics(SubscribeMeter.MetricsName.filter_records_failure, fr.getRecord().getRecord().topic(),
                                fr.getRecord().getRecord().partition(), groupId);
                        if (processConfig.getCheckpointQoS().isMaxRetriesOrStrictly()) {
                            if (shouldGiveUpRetry(fr.getRetryBegin(), fr.getRetryTimes())) {
                                break; // give up and lose
                            }
                            enqueueFilteredExecutor(fr, filteredResults);
                        }
                    } catch (ExecutionException ex) {
                        log.error("Unable not to getting subscribe filter result.", ex);
                        final Throwable reason = ExceptionUtils.getRootCause(ex);
                        // User needs to give up trying again.
                        if (reason instanceof GiveUpRetryExecutionException) {
                            log.warn("User ask to give up re-trying again filter. fr : {}, reason :{}", fr, reason.getMessage());
                        } else {
                            if (processConfig.getCheckpointQoS().isMaxRetriesOrStrictly()) {
                                if (shouldGiveUpRetry(fr.getRetryBegin(), fr.getRetryTimes())) {
                                    break; // give up and lose
                                }
                                enqueueFilteredExecutor(fr, filteredResults);
                            }
                        }
                    } finally {
                        it.remove();
                    }
                    if (Objects.nonNull(matched) && matched) {
                        // Send to filtered topic and add sent future If necessary.
                        if (Objects.nonNull(sentResults)) {
                            sentResults.add(doSendToFilteredAsync(fr.getRecord(), System.nanoTime(), 0));
                        }
                    }
                }
            }
            Thread.yield(); // May give up the CPU
        }
        filterTimer.record(Duration.ofNanos(System.nanoTime() - filterBeginTime));

        // Flush all producers to ensure that all records in this batch are committed.
        safeList(sentResults).stream().map(SentResult::getProducer).forEach(KafkaProducer::flush);

        return sentResults;
    }

    private void enqueueFilteredExecutor(FilteredResult fr, List<FilteredResult> filteredResults) {
        log.info("Re-enqueue Requeue and retry filter execution. fr : {}", fr);
        filteredResults.add(new FilteredResult(fr.getRecord(), determineFilterExecutor(fr.getRecord())
                .submit(() -> subscribeFilter.apply(fr.getRecord())), fr.getRetryBegin(), fr.getRetryTimes() + 1));
    }

    private List<SubscriberRecord> matchWrapToSubscribesRecords(List<ConsumerRecord<String, ObjectNode>> records) {
        final List<SubscriberInfo> subscribers = subscribeFacade.findSubscribers(SubscriberInfo.builder().build());
        return records.stream().map(r -> safeList(subscribers).stream()
                .filter(s -> subscribeFacade.matchSubscriberRecord(s, r))
                .limit(1).findFirst().map(s -> new SubscriberRecord(s, r))
                .orElseGet(() -> {
                    log.warn(String.format("No matched subscriber for record : %s", r));
                    return null;
                })).filter(Objects::nonNull).collect(toList());
    }

    private ThreadPoolExecutor determineFilterExecutor(SubscriberRecord record) {
        final SubscriberInfo subscriber = record.getSubscriber();
        final String key = record.getRecord().key();
        if (subscriber.getIsSequence()) {
            //final int mod = (int) subscriber.getId();
            final int mod = (int) Crc32Util.compute(key);
            return isolationSequenceExecutors.get(Math.abs((int) (isolationSequenceExecutors.size() % mod)));
        } else {
            return sharedNonSequenceExecutor;
        }
    }

    private ISubscribeFilter obtainSubscribeFilter() {
        try {
            return context.getBean(pipelineConfig.getFilter().getCustomFilterBeanName(), ISubscribeFilter.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new IllegalStateException(String.format("Could not getting custom subscriber filter of bean %s", pipelineConfig.getFilter().getCustomFilterBeanName()));
        }
    }

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    private SentResult doSendToFilteredAsync(SubscriberRecord record, long retryBegin, int retryTimes) {
        final SubscriberInfo subscriber = record.getSubscriber();
        final String key = record.getRecord().key();
        final ObjectNode value = record.getRecord().value();

        final KafkaProducer<String, String> producer = determineKafkaProducer(subscriber, key);
        final String filteredTopic = subscribeFacade.generateFilteredTopic(pipelineConfig, subscriber);

        // Notice: Hand down the subscriber metadata of each record to the downstream.
        value.set("$$subscriberId", LongNode.valueOf(subscriber.getId()));
        value.set("$$isSequence", BooleanNode.valueOf(subscriber.getIsSequence()));

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(filteredTopic, key, value.asText());
        return new SentResult(record, producer, producer.send(producerRecord), retryBegin, retryTimes);
    }

    private KafkaProducer<String, String> determineKafkaProducer(SubscriberInfo subscriber, String key) {
        KafkaProducer<String, String> producer = filteredCheckpointProducers.get(RandomUtils.nextInt(0, filteredCheckpointProducers.size()));
        if (subscriber.getIsSequence()) {
            //final int mod = (int) subscriber.getId();
            final int mod = (int) Crc32Util.compute(key);
            producer = filteredCheckpointProducers.get(filteredCheckpointProducers.size() % mod);
        }
        if (Objects.isNull(producer)) {
            throw new IllegalStateException(String.format("Could not getting producer by subscriber: %s, key: %s", subscriber.getId(), key));
        }
        log.debug("Using kafka producer by subscriber: {}, key: {}", subscriber.getId(), key);
        return producer;
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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
        private Future<Boolean> future;
        private long retryBegin;
        private int retryTimes;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class SentResult {
        private SubscriberRecord record;
        private KafkaProducer<String, String> producer;
        private Future<RecordMetadata> future;
        private long retryBegin;
        private int retryTimes;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SentResult that = (SentResult) o;
            return Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            return Objects.hash(record);
        }
    }

}




