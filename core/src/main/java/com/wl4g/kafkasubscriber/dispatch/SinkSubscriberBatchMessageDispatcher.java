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
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.exception.GiveUpRetryExecutionException;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.stream.Collectors.toList;

/**
 * The {@link SinkSubscriberBatchMessageDispatcher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class SinkSubscriberBatchMessageDispatcher extends AbstractBatchMessageDispatcher {
    private final SubscriberInfo subscriber;
    private final String sinkFromTopic;
    private ISubscribeSink subscribeSink;

    public SinkSubscriberBatchMessageDispatcher(ApplicationContext context,
                                                KafkaSubscriberProperties.SubscribePipelineProperties pipelineConfig,
                                                SubscribeFacade subscribeFacade,
                                                SubscriberRegistry subscriberRegistry,
                                                String groupId,
                                                SubscriberInfo subscriber) {
        super(context, pipelineConfig, pipelineConfig.getSink().getProcessProps(), subscribeFacade, subscriberRegistry, groupId);
        this.subscriber = Assert2.notNullOf(subscriber, "subscriber");
        this.sinkFromTopic = subscribeFacade.generateFilteredTopic(pipelineConfig.getFilter(), subscriber.getId());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // Create custom subscribe sinker. (Each processing pipeline uses different custom sink instances)
        this.subscribeSink = obtainSubscribeSink();
    }

    @Override
    public void doOnMessage(List<ConsumerRecord<String, ObjectNode>> filteredRecords, Acknowledgment ack) {
        // Sink from filtered records (per subscriber a topic).
        final List<SinkResult> sinkResults = safeList(filteredRecords).stream()
                .map(fr -> doSinkAsync(fr, System.nanoTime(), 0)).collect(toList());

        // Add timing sink metrics. (The benefit of not using LAMDA records is better use of arthas for troubleshooting during operation.)
        final Timer sinkTimer = addTimerMetrics(SubscribeMeter.MetricsName.sink_time, sinkFromTopic, groupId);
        final long sinkBeginTime = System.nanoTime();

        // Wait for all sink to be completed.
        if (processConfig.getCheckpointQoS().isMaxRetriesOrStrictly()) {
            while (sinkResults.size() > 0) {
                final Iterator<SinkResult> it = sinkResults.iterator();
                while (it.hasNext()) {
                    final SinkResult sr = it.next();
                    // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                    if (sr.getFuture().isDone()) {
                        SinkCompleted sc = null;
                        try {
                            sc = sr.getFuture().get();
                            addCounterMetrics(SubscribeMeter.MetricsName.sink_records_success, sr.getFilteredRecord().topic(),
                                    sr.getFilteredRecord().partition(), groupId);
                        } catch (InterruptedException | CancellationException ex) {
                            log.error("{} :: {} :: Unable to getting sink result.", groupId, subscriber.getId(), ex);
                            addCounterMetrics(SubscribeMeter.MetricsName.sink_records_failure, sr.getFilteredRecord().topic(),
                                    sr.getFilteredRecord().partition(), groupId);
                            if (processConfig.getCheckpointQoS().isMaxRetriesOrStrictly()) {
                                if (shouldGiveUpRetry(sr.getRetryBegin(), sr.getRetryTimes())) {
                                    break; // give up and lose
                                }
                                sinkResults.add(doSinkAsync(sr.getFilteredRecord(), sr.getRetryBegin(), sr.getRetryTimes() + 1));
                            }
                        } catch (ExecutionException ex) {
                            log.error("{} :: {} :: Unable not to getting sink result.", groupId, subscriber.getId(), ex);
                            addCounterMetrics(SubscribeMeter.MetricsName.sink_records_failure, sr.getFilteredRecord().topic(),
                                    sr.getFilteredRecord().partition(), groupId);
                            final Throwable reason = ExceptionUtils.getRootCause(ex);
                            // User needs to give up trying again.
                            if (reason instanceof GiveUpRetryExecutionException) {
                                log.warn("{} :: {} :: User ask to give up re-trying again sink. sr : {}, reason :{}",
                                        groupId, subscriber.getId(), sr, reason.getMessage());
                            } else {
                                if (processConfig.getCheckpointQoS().isMaxRetriesOrStrictly()) {
                                    if (shouldGiveUpRetry(sr.getRetryBegin(), sr.getRetryTimes())) {
                                        break; // give up and lose
                                    }
                                    sinkResults.add(doSinkAsync(sr.getFilteredRecord(), sr.getRetryBegin(), sr.getRetryTimes() + 1));
                                }
                            }
                        } finally {
                            it.remove();
                        }
                        log.debug("{} :: {} :: Sink to completed result : {}", groupId, subscriber.getId(), sc);
                    }
                }
                Thread.yield(); // May give up the CPU
            }
            try {
                log.debug("{} :: {} :: Batch sink acknowledging ...", groupId, subscriber.getId());
                ack.acknowledge();
                log.info("{} :: {} :: Sink to acknowledged.", groupId, subscriber.getId());
            } catch (Throwable ex) {
                log.error(String.format("%s :: %s :: Failed to sink success acknowledge for %s", groupId, subscriber.getId(), ack), ex);
            }
        } else {
            try {
                log.debug("{} :: {} :: Batch regardless of success or failure sink force acknowledging ...", groupId, subscriber.getId());
                ack.acknowledge();
                log.info("{} :: {} :: Force sink to acknowledged.", groupId, subscriber.getId());
            } catch (Throwable ex) {
                log.error(String.format("%s :: %s :: Failed to sink force acknowledge for %s", groupId, subscriber.getId(), ack), ex);
            }
        }

        sinkTimer.record(Duration.ofNanos(System.nanoTime() - sinkBeginTime));
    }

    private ThreadPoolExecutor determineSinkExecutor(String key) {
        return determineTaskExecutor(subscriber.getId(), subscriber.getIsSequence(), key);
    }

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    private SinkResult doSinkAsync(ConsumerRecord<String, ObjectNode> filteredRecord, long retryBegin, int retryTimes) {
        final String key = filteredRecord.key();
        final ObjectNode value = filteredRecord.value();
        final long subscribeId = value.remove("$$subscriberId").asLong(-1L);
        final boolean isSequence = value.remove("$$isSequence").asBoolean(false);

        // Determine the sink task executor.
        final ThreadPoolExecutor executor = determineSinkExecutor(key);

        final Future<? extends SinkCompleted> future = executor.submit(() -> subscribeSink.doSink(subscribeId, isSequence, filteredRecord));
        return new SinkResult(filteredRecord, future, retryBegin, retryTimes);
    }

    private ISubscribeSink obtainSubscribeSink() {
        try {
            return context.getBean(pipelineConfig.getSink().getCustomSinkBeanName(), ISubscribeSink.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new IllegalStateException(String.format("%s :: %s :: Could not getting custom subscriber sink of bean %s",
                    groupId, subscriber.getId(), pipelineConfig.getSink().getCustomSinkBeanName()));
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @AllArgsConstructor
    @ToString(callSuper = true)
    static class SinkResult {
        private ConsumerRecord<String, ObjectNode> filteredRecord;
        private Future<? extends SinkCompleted> future;
        private long retryBegin;
        private int retryTimes;
    }

    public interface SinkCompleted {
        SinkCompleted EMPTY = new SinkCompleted() {
        };
    }

}



