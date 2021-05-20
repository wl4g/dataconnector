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
import com.wl4g.kafkasubscriber.util.NamedThreadFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private final ThreadPoolExecutor sharedNonSequenceFilterExecutor;
    private final List<ThreadPoolExecutor> isolationSequenceFilterExecutors;
    private final List<KafkaProducer<String, String>> filterProducers;

    public FilterBatchMessageDispatcher(ApplicationContext context,
                                        KafkaSubscriberProperties.SubscribePipelineProperties config,
                                        SubscribeFacade facade,
                                        SubscriberRegistry registry) {
        super(context, config, facade, registry);

        // Create the shared filter single executor.
        this.sharedNonSequenceFilterExecutor = new ThreadPoolExecutor(config.getFilter().getSharedExecutorThreadPoolSize(), config.getFilter().getSharedExecutorThreadPoolSize(), 0L, TimeUnit.MILLISECONDS,
                // TODO or use bounded queue
                new LinkedBlockingQueue<>(config.getFilter().getSharedExecutorQueueSize()), new NamedThreadFactory("shared-".concat(getClass().getSimpleName())));
        if (config.getFilter().isPreStartAllCoreThreads()) {
            this.sharedNonSequenceFilterExecutor.prestartAllCoreThreads();
        }

        // Create the sequence filter executors.
        this.isolationSequenceFilterExecutors = synchronizedList(new ArrayList<>(config.getFilter().getSequenceExecutorsMaxCountLimit()));
        for (int i = 0; i < config.getFilter().getSequenceExecutorsMaxCountLimit(); i++) {
            final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    // TODO or use bounded queue
                    new LinkedBlockingQueue<>(config.getFilter().getSequenceExecutorsPerQueueSize()), new NamedThreadFactory("sequence-".concat(getClass().getSimpleName())));
            if (config.getFilter().isPreStartAllCoreThreads()) {
                executor.prestartAllCoreThreads();
            }
            this.isolationSequenceFilterExecutors.add(executor);
        }

        // Create the internal filtered producers.
        this.filterProducers = synchronizedList(new ArrayList<>(config.getFilter().getProducerMaxCountLimit()));
    }

    @Override
    public void close() throws IOException {
        try {
            log.info("Closing shared non sequence filter executor...");
            sharedNonSequenceFilterExecutor.shutdown();
            log.info("Closed shared non sequence filter executor.");
        } catch (Throwable ex) {
            log.error("Failed to close shared filter executor.", ex);
        }
        isolationSequenceFilterExecutors.forEach(executor -> {
            try {
                log.info("Closing sequence filter executor {}...", executor);
                executor.shutdown();
                log.info("Closed sequence filter executor {}.", executor);
            } catch (Throwable ex) {
                log.error(String.format("Failed to close sequence filter executor %s.", executor), ex);
            }
        });
        filterProducers.forEach(producer -> {
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

        // Each processing pipeline uses different custom filter instances.
        final ISubscribeFilter subscribeFilter = getSubscribeFilter();

        // Execute directly custom filters.
        //final List<SentResult> result = safeList(subscriberRecords).parallelStream()
        //        .filter(sr -> subscribeFilter.apply(sr.getSubscriber(), sr.getRecord().value()))
        //        .map(this::doSendToFilteredAsync).collect(toList());

        // Execute custom filters in parallel them to different send executor queues.
        final List<Future<FilteredResult>> filteredResults = safeList(subscriberRecords).stream()
                .map(sr -> obtainFilterExecutor(sr)
                        .submit(() -> subscribeFilter.apply(sr))).collect(toList());

        // Wait for all parallel filtered results to be completed.
        final Set<SentResult> sentResults = new HashSet<>(filteredResults.size());
        while (filteredResults.size() > 0) {
            final Iterator<Future<FilteredResult>> it = filteredResults.iterator();
            while (it.hasNext()) {
                final Future<FilteredResult> ff = it.next();
                if (ff.isDone()) {
                    try {
                        final FilteredResult fr = ff.get();
                        it.remove(); // Remove completed future.
                        if (fr.getMatched()) {
                            // Send to filtered record and add sent future.
                            sentResults.add(doSendToFilteredAsync(fr.getRecord()));
                        }
                    } catch (Throwable ex) {
                        log.error("Failed to getting subscriber filtered result.", ex);
                    }
                }
            }
            Thread.yield();
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
        if (config.getFilter().isBestQoS()) {
            final long timeout = config.getFilter().getCheckpointTimeout().toNanos();
            final List<Future<RecordMetadata>> futures = sentResults.stream().map(SentResult::getFuture).collect(toList());
            final long begin = System.nanoTime();
            while (futures.stream().filter(Future::isDone).count() < futures.size()) {
                Thread.yield();
                if ((System.nanoTime() - begin) > timeout) {
                    if (config.getFilter().isCheckpointFastFail()) {
                        log.error("Timeout sent to filtered kafka topic, shutdown...");
                        // If the timeout is exceeded, the program will exit.
                        System.exit(1);
                    } else {
                        final List<JsonNode> recordValues = safeList(subscriberRecords).stream().map(SubscriberRecord::getRecord)
                                .map(ConsumerRecord::value).collect(toList());
                        log.error("Timeout sent to filtered kafka topic, fail fast has been disabled," + "which is likely to result in loss of filtered data. - {}", recordValues);
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

    private List<SubscriberRecord> matchWrapToSubscribesRecords(List<ConsumerRecord<String, ObjectNode>> records) {
        final List<SubscriberInfo> subscribers = facade.findSubscribers(SubscriberInfo.builder().build());
        return records.stream().map(r -> safeList(subscribers).stream().filter(s -> facade.matchSubscriberRecord(s, r)).limit(1).findFirst().map(s -> new SubscriberRecord(s, r)).get()).collect(toList());
    }

    private ThreadPoolExecutor obtainFilterExecutor(SubscriberRecord record) {
        final SubscriberInfo subscriber = record.getSubscriber();
        final String key = record.getRecord().key();
        if (subscriber.getIsSequence()) {
            //final int mod = (int) Math.abs(subscribeId);
            final int mod = (int) Math.abs(Crc32Util.compute(key));
            return isolationSequenceFilterExecutors.get(Math.abs((int) (isolationSequenceFilterExecutors.size() % mod)));
        } else {
            return sharedNonSequenceFilterExecutor;
        }
    }

    private ISubscribeFilter getSubscribeFilter() {
        try {
            return context.getBean(config.getFilter().getCustomFilterBeanName(), ISubscribeFilter.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new IllegalStateException(String.format("Could not getting custom filter of bean %s", config.getFilter().getCustomFilterBeanName()));
        }
    }

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    private SentResult doSendToFilteredAsync(SubscriberRecord SubscriberRecord) {
        final SubscriberInfo subscriber = SubscriberRecord.getSubscriber();
        final String key = SubscriberRecord.getRecord().key();
        final ObjectNode value = SubscriberRecord.getRecord().value();

        final KafkaProducer<String, String> producer = obtainKafkaProducer(subscriber, key);
        final String filteredTopic = facade.generateFilteredTopic(config, subscriber);

        // Notice: Hand down the subscriber metadata of each record to the downstream.
        value.set("$subscriberId", LongNode.valueOf(subscriber.getId()));
        value.set("$isSequence", BooleanNode.valueOf(subscriber.getIsSequence()));

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(filteredTopic, key, value.asText());
        return new SentResult(SubscriberRecord, producer, producer.send(producerRecord));
    }

    private KafkaProducer<String, String> obtainKafkaProducer(SubscriberInfo subscriber, String key) {
        KafkaProducer<String, String> producer = null;
        if (subscriber.getIsSequence()) {
            producer = filterProducers.get(filterProducers.size() % Math.abs(key.hashCode()));
        }
        if (Objects.isNull(producer)) {
            throw new IllegalStateException(String.format("Could not getting producer by subscriber: %s, key: %s", subscriber.getId(), key));
        }
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
        private Boolean matched;
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




