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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import com.wl4g.kafkasubscriber.util.Crc32Util;
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
                                        SubscriberRegistry subscriberRegistry) {
        super(context, pipelineConfig, pipelineConfig.getFilter().getProcessProps(), subscribeFacade, subscriberRegistry);

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
        // Match wrap to subscriber rules records.
        final List<SubscriberRecord> subscriberRecords = matchWrapToSubscribesRecords(records);

        // Execute directly custom filters.
        //final List<SentResult> sentResults = safeList(subscriberRecords).parallelStream()
        //        .filter(sr -> subscribeFilter.apply(sr.getSubscriber(), sr.getRecord().value()))
        //        .map(this::doSendToFilteredAsync).collect(toList());

        // Execute custom filters in parallel them to different send executor queues.
        final List<FilteredResult> filteredResults = safeList(subscriberRecords).stream()
                .map(sr -> new FilteredResult(sr, determineFilterExecutor(sr).submit(() -> subscribeFilter.apply(sr)), 1)).collect(toList());

        // TODO Wait for all parallel filtered results to be completed.
        if (!pipelineConfig.getSink().isEnable()) {
            log.info("Sink is disabled, skip to send filtered results to kafka.");
            return;
        }

        // Wait for all parallel filtered results to be completed.
        final Set<SentResult> sentResults = new HashSet<>(filteredResults.size());
        while (filteredResults.size() > 0) {
            final Iterator<FilteredResult> it = filteredResults.iterator();
            while (it.hasNext()) {
                final FilteredResult fr = it.next();
                // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                if (fr.getFuture().isDone()) {
                    Boolean matched = null;
                    try {
                        matched = fr.getFuture().get();
                    } catch (InterruptedException | CancellationException ex) {
                        log.error("Unable to getting subscribe filter result.", ex);
                        if (pipelineConfig.getFilter().getProcessProps().getQos().isMaxRetriesOrStrictly()) {
                            if (shouldGiveUpRetry(fr)) {
                                break; // give up and lose
                            }
                            enqueueFilteredExecutor(fr, filteredResults);
                        }
                    } catch (ExecutionException ex) {
                        log.error("Could not to getting subscribe filter result.", ex);
                        if (pipelineConfig.getFilter().getProcessProps().getQos().isMaxRetriesOrStrictly()) {
                            if (shouldGiveUpRetry(fr)) {
                                break; // give up and lose
                            }
                            enqueueFilteredExecutor(fr, filteredResults);

                            // TODO re-add to filter executor queue?
                            final Throwable rootCause = ExceptionUtils.getRootCause(ex);
                            if (rootCause instanceof RuntimeException) {
                                // TODO
                            }
                        }
                    } finally {
                        it.remove();
                    }
                    if (Objects.nonNull(matched) && matched) {
                        // Send to filtered topic and add sent future.
                        sentResults.add(doSendToFilteredAsync(fr.getRecord()));
                    }
                }
            }
            Thread.yield(); // May give up the CPU
        }

        // Flush all producers to ensure that all records in this batch are committed.
        sentResults.stream().map(SentResult::getProducer).forEach(KafkaProducer::flush);

        // Wait for all filtered records to be sent completed.
        //
        // Notice: Although it is best to support the config ‘bestQoS’ to the subscriber level, the current model
        // of sharing the original data of the groupId can only support the whole batch.
        //
        // The purpose of this design is to adapt to the scenario of large message traffic, because if different
        // groupIds are used to consume the original message, a large amount of COPY traffic may be generated before
        // filtering, that is, bandwidth is wasted from Kafka broker to this Pod Kafka consumer.
        //
        if (pipelineConfig.getFilter().getProcessProps().getQos().isMaxRetriesOrStrictly()) {
            final long timeout = pipelineConfig.getFilter().getProcessProps().getCheckpointTimeout().toNanos();
            final List<Future<RecordMetadata>> futures = sentResults.stream().map(SentResult::getFuture).collect(toList());
            final long begin = System.nanoTime();
            while (futures.stream().filter(Future::isDone).count() < futures.size()) {
                Thread.yield(); // May give up the CPU
                if ((System.nanoTime() - begin) > timeout) {
                    if (pipelineConfig.getFilter().getProcessProps().getQos().isStrictly()) {
                        log.error("Timeout sent to filtered kafka topic, shutdown...");
                        // TODO re-add to filter executor queue for forever retry.
                        // If the timeout is exceeded, the program will exit.
                        //System.exit(1);
                    } else {
                        final List<JsonNode> recordValues = safeList(subscriberRecords).stream().map(SubscriberRecord::getRecord)
                                .map(ConsumerRecord::value).collect(toList());
                        log.error("Timeout sent to filtered kafka topic, fail fast has been disabled,"
                                + "which is likely to result in loss of filtered data. - {}", recordValues);
                    }
                }
            }
            try {
                log.debug("Batch sent success and acknowledging ...");
                ack.acknowledge();
                log.info("Sent success acknowledged.");
            } catch (Throwable ex) {
                log.error(String.format("Failed to sent success acknowledge for %s", ack), ex);
            }
        } else {
            try {
                log.debug("Batch regardless of success or failure force acknowledging ...");
                ack.acknowledge();
                log.info("Force to acknowledged.");
            } catch (Throwable ex) {
                log.error(String.format("Failed to force acknowledge for %s", ack), ex);
            }
        }
    }

    /**
     * Max retries then give up if it fails.
     */
    private boolean shouldGiveUpRetry(FilteredResult fr) {
        return pipelineConfig.getFilter().getProcessProps().getQos().isMoseOnceOrMaxRetries()
                && fr.getRetryTimes() > pipelineConfig.getFilter().getProcessProps().getQosMaxRetries();
    }

    private void enqueueFilteredExecutor(FilteredResult fr, List<FilteredResult> filteredResults) {
        filteredResults.add(new FilteredResult(fr.getRecord(), determineFilterExecutor(fr.getRecord())
                .submit(() -> subscribeFilter.apply(fr.getRecord())), fr.getRetryTimes() + 1));
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
    private SentResult doSendToFilteredAsync(SubscriberRecord record) {
        final SubscriberInfo subscriber = record.getSubscriber();
        final String key = record.getRecord().key();
        final ObjectNode value = record.getRecord().value();

        final KafkaProducer<String, String> producer = determineKafkaProducer(subscriber, key);
        final String filteredTopic = subscribeFacade.generateFilteredTopic(pipelineConfig, subscriber);

        // Notice: Hand down the subscriber metadata of each record to the downstream.
        value.set("$$subscriberId", LongNode.valueOf(subscriber.getId()));
        value.set("$$isSequence", BooleanNode.valueOf(subscriber.getIsSequence()));

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(filteredTopic, key, value.asText());
        return new SentResult(record, producer, producer.send(producerRecord));
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
    public static class FilteredResult {
        private SubscriberRecord record;
        private Future<Boolean> future;
        private int retryTimes = 0;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    public static class SentResult {
        private SubscriberRecord record;
        private KafkaProducer<String, String> producer;
        private Future<RecordMetadata> future;

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




