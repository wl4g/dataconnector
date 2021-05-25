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
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.facade.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.util.Crc32Util;
import com.wl4g.kafkasubscriber.util.NamedThreadFactory;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.synchronizedList;

/**
 * The {@link AbstractBatchMessageDispatcher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public abstract class AbstractBatchMessageDispatcher
        implements BatchAcknowledgingMessageListener<String, ObjectNode>, Closeable {

    protected final ApplicationContext context;
    protected final KafkaSubscriberProperties.SubscribePipelineProperties pipelineConfig;
    protected final KafkaSubscriberProperties.GenericProcessProperties processConfig;
    protected final SubscribeEngineCustomizer customizer;
    protected final CachingSubscriberRegistry registry;
    protected final ThreadPoolExecutor sharedNonSequenceExecutor;
    protected final List<ThreadPoolExecutor> isolationSequenceExecutors;
    protected final String groupId;
    protected final Producer<String, String> acknowledgeProducer;

    public AbstractBatchMessageDispatcher(ApplicationContext context,
                                          KafkaSubscriberProperties.SubscribePipelineProperties config,
                                          KafkaSubscriberProperties.GenericProcessProperties processConfig,
                                          SubscribeEngineCustomizer customizer,
                                          CachingSubscriberRegistry registry,
                                          String groupId,
                                          Producer<String, String> acknowledgeProducer) {
        this.context = Assert2.notNullOf(context, "context");
        this.pipelineConfig = Assert2.notNullOf(config, "config");
        this.processConfig = Assert2.notNullOf(processConfig, "processConfig");
        this.customizer = Assert2.notNullOf(customizer, "customizer");
        this.registry = Assert2.notNullOf(registry, "registry");
        this.groupId = Assert2.hasTextOf(groupId, "groupId");
        this.acknowledgeProducer = Assert2.notNullOf(acknowledgeProducer, "acknowledgeProducer");

        // Create the shared filter single executor.
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

    public void init() {
    }

    @Override
    public void close() throws IOException {
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
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack) {
        try {
            addCounterMetrics(SubscribeMeter.MetricsName.shared_consumed,
                    pipelineConfig.getSource().getTopicPattern().toString(), groupId);

            final Timer timer = addTimerMetrics(SubscribeMeter.MetricsName.shared_consumed_time,
                    pipelineConfig.getSource().getTopicPattern().toString(), groupId);

            timer.record(() -> doOnMessage(records, ack));

        } catch (Throwable ex) {
            log.error(String.format("%s :: Failed to process message. - %s", groupId, records), ex);
            // Commit directly if no quality of service is required.
            if (pipelineConfig.getFilter().getCheckpoint().getQos().isMoseOnce()) {
                ack.acknowledge();
            }
        }
    }

    protected abstract void doOnMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack);

    protected ThreadPoolExecutor determineTaskExecutor(Long subscriberId, boolean isSequence, String key) {
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

    /**
     * Max retries then give up if it fails.
     */
    protected boolean shouldGiveUpRetry(long retryBegin, int retryTimes) {
        return pipelineConfig.getFilter().getCheckpoint().getQos().isMoseOnceOrAnyRetriesAtMost()
                && (retryTimes > pipelineConfig.getFilter().getCheckpoint().getQoSMaxRetries()
                || (System.nanoTime() - retryBegin) > pipelineConfig.getFilter().getCheckpoint().getQoSMaxRetriesTimeout().toNanos());
    }

    protected void addCounterMetrics(SubscribeMeter.MetricsName metrics, String topic, String groupId) {
        addCounterMetrics(metrics, topic, null, groupId);
    }

    protected void addCounterMetrics(SubscribeMeter.MetricsName metrics, String topic,
                                     Integer partition, String groupId) {
        addCounterMetrics(metrics, null, topic, partition, groupId);
    }

    protected void addCounterMetrics(SubscribeMeter.MetricsName metrics, Long subscriberId,
                                     String topic, Integer partition, String groupId) {
        final List<String> tags = new ArrayList<>(8);
        tags.add(SubscribeMeter.MetricsTag.TOPIC);
        tags.add(topic);
        tags.add(SubscribeMeter.MetricsTag.GROUP_ID);
        tags.add(groupId);
        if (Objects.nonNull(subscriberId)) {
            tags.add(SubscribeMeter.MetricsTag.SUBSCRIBE);
            tags.add(String.valueOf(subscriberId));
        }
        if (Objects.nonNull(partition)) {
            tags.add(SubscribeMeter.MetricsTag.PARTITION);
            tags.add(String.valueOf(partition));
        }
        SubscribeMeter.getDefault().counter(metrics.getName(), metrics.getHelp(), tags.toArray(new String[0])).increment();
    }

    protected Timer addTimerMetrics(SubscribeMeter.MetricsName metrics, String topic, String groupId, String... addTags) {
        return addTimerMetrics(metrics, null, topic, null, groupId, addTags);
    }

    protected Timer addTimerMetrics(SubscribeMeter.MetricsName metrics, Long subscriberId, String topic,
                                    Integer partition, String groupId, String... addTags) {
        final List<String> tags = new ArrayList<>(8);
        tags.add(SubscribeMeter.MetricsTag.TOPIC);
        tags.add(topic);
        tags.add(SubscribeMeter.MetricsTag.GROUP_ID);
        tags.add(groupId);
        if (Objects.nonNull(subscriberId)) {
            tags.add(SubscribeMeter.MetricsTag.SUBSCRIBE);
            tags.add(String.valueOf(subscriberId));
        }
        if (Objects.nonNull(partition)) {
            tags.add(SubscribeMeter.MetricsTag.PARTITION);
            tags.add(String.valueOf(partition));
        }
        if (Objects.nonNull(addTags)) {
            tags.addAll(Arrays.asList(addTags));
        }
        final String[] tagArray = tags.toArray(new String[0]);
        return SubscribeMeter.getDefault().timer(metrics.getName(), metrics.getHelp(),
                SubscribeMeter.DEFAULT_PERCENTILES, tagArray);
    }

}




