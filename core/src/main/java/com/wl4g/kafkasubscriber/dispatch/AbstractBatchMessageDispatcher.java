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
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import com.wl4g.kafkasubscriber.util.NamedThreadFactory;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        implements BatchAcknowledgingMessageListener<String, ObjectNode>, InitializingBean, Closeable {

    protected final ApplicationContext context;
    protected final KafkaSubscriberProperties.SubscribePipelineProperties pipelineConfig;
    protected final KafkaSubscriberProperties.GenericProcessProperties processConfig;
    protected final SubscribeFacade subscribeFacade;
    protected final SubscriberRegistry subscriberRegistry;
    protected final ThreadPoolExecutor sharedNonSequenceExecutor;
    protected final List<ThreadPoolExecutor> isolationSequenceExecutors;
    protected final String groupId;

    public AbstractBatchMessageDispatcher(ApplicationContext context,
                                          KafkaSubscriberProperties.SubscribePipelineProperties config,
                                          KafkaSubscriberProperties.GenericProcessProperties processConfig,
                                          SubscribeFacade subscribeFacade,
                                          SubscriberRegistry subscriberRegistry,
                                          String groupId) {
        this.context = Assert2.notNullOf(context, "context");
        this.pipelineConfig = Assert2.notNullOf(config, "config");
        this.processConfig = Assert2.notNullOf(processConfig, "processConfig");
        this.subscribeFacade = Assert2.notNullOf(subscribeFacade, "subscribeFacade");
        this.subscriberRegistry = Assert2.notNullOf(subscriberRegistry, "subscriberRegistry");
        this.groupId = Assert2.hasTextOf(groupId, "groupId");

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

    @Override
    public void close() throws IOException {
        try {
            log.info("Closing shared non filter executor...");
            this.sharedNonSequenceExecutor.shutdown();
            log.info("Closed shared non filter executor.");
        } catch (Throwable ex) {
            log.error("Failed to close shared filter executor.", ex);
        }
        this.isolationSequenceExecutors.forEach(executor -> {
            try {
                log.info("Closing filter executor {}...", executor);
                executor.shutdown();
                log.info("Closed filter executor {}.", executor);
            } catch (Throwable ex) {
                log.error(String.format("Failed to close filter executor %s.", executor), ex);
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
            log.error(String.format("Failed to process message. - %s", records), ex);
            // Commit directly if no quality of service is required.
            if (processConfig.getCheckpointQoS().isMoseOnce()) {
                ack.acknowledge();
            }
        }
    }

    protected abstract void doOnMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack);

    /**
     * Max retries then give up if it fails.
     */
    protected boolean shouldGiveUpRetry(long retryBegin, int retryTimes) {
        return processConfig.getCheckpointQoS().isMoseOnceOrMaxRetries()
                && (retryTimes > processConfig.getCheckpointQoSMaxRetries()
                || (System.nanoTime() - retryBegin) > processConfig.getCheckpointQoSMaxRetriesTimeout().toNanos());
    }

    protected void addCounterMetrics(SubscribeMeter.MetricsName metrics, String topic, String groupId) {
        addCounterMetrics(metrics, topic, -1, groupId);
    }

    protected void addCounterMetrics(SubscribeMeter.MetricsName metrics, String topic, int partition, String groupId) {
        if (partition > 0) {
            SubscribeMeter.getDefault().counter(
                    metrics.getName(),
                    metrics.getHelp(),
                    SubscribeMeter.MetricsTag.TOPIC, topic,
                    SubscribeMeter.MetricsTag.PARTITION, String.valueOf(partition),
                    SubscribeMeter.MetricsTag.GROUP_ID, groupId
            ).increment();
        } else {
            SubscribeMeter.getDefault().counter(
                    metrics.getName(),
                    metrics.getHelp(),
                    SubscribeMeter.MetricsTag.TOPIC, topic,
                    SubscribeMeter.MetricsTag.GROUP_ID, groupId
            ).increment();
        }
    }


    protected Timer addTimerMetrics(SubscribeMeter.MetricsName metrics, String topic, String groupId) {
        return addTimerMetrics(metrics, topic, groupId);
    }

    protected Timer addTimerMetrics(SubscribeMeter.MetricsName metrics, String topic, int partition, String groupId) {
        if (partition > 0) {
            return SubscribeMeter.getDefault().timer(
                    metrics.getName(),
                    metrics.getHelp(),
                    SubscribeMeter.DEFAULT_PERCENTILES,
                    SubscribeMeter.MetricsTag.TOPIC, topic,
                    SubscribeMeter.MetricsTag.GROUP_ID, groupId
            );
        } else {
            return SubscribeMeter.getDefault().timer(
                    metrics.getName(),
                    metrics.getHelp(),
                    SubscribeMeter.DEFAULT_PERCENTILES,
                    SubscribeMeter.MetricsTag.TOPIC, topic,
                    SubscribeMeter.MetricsTag.PARTITION, String.valueOf(partition),
                    SubscribeMeter.MetricsTag.GROUP_ID, groupId
            );
        }
    }

}




