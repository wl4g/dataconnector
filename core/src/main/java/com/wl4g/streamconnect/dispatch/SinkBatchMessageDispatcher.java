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

package com.wl4g.streamconnect.dispatch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.PipelineConfig;
import com.wl4g.streamconnect.coordinator.CachingSubscriberRegistry;
import com.wl4g.streamconnect.custom.StreamConnectEngineCustomizer;
import com.wl4g.streamconnect.exception.GiveUpRetryExecutionException;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.meter.StreamConnectMeterEventHandler.CountMeterEvent;
import com.wl4g.streamconnect.meter.StreamConnectMeterEventHandler.TimingMeterEvent;
import com.wl4g.streamconnect.sink.IProcessSink;
import com.wl4g.streamconnect.util.KafkaUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.support.Acknowledgment;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.stream.Collectors.toList;

/**
 * The {@link SinkBatchMessageDispatcher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class SinkBatchMessageDispatcher extends AbstractBatchMessageDispatcher {
    private final SubscriberInfo subscriber;
    private final IProcessSink subscribeSink;

    public SinkBatchMessageDispatcher(StreamConnectConfiguration config,
                                      PipelineConfig pipelineConfig,
                                      StreamConnectEngineCustomizer customizer,
                                      CachingSubscriberRegistry registry,
                                      ApplicationEventPublisher eventPublisher,
                                      String topicDesc,
                                      String groupId,
                                      SubscriberInfo subscriber,
                                      IProcessSink sink) {
        super(config, pipelineConfig, customizer, registry, eventPublisher, topicDesc, groupId);
        this.subscribeSink = Assert2.notNullOf(sink, "sink");
        this.subscriber = Assert2.notNullOf(subscriber, "subscriber");
    }

    @Override
    public void doOnMessage(List<ConsumerRecord<String, ObjectNode>> filteredRecords, Acknowledgment ack) {
        // Sink from filtered records (per subscriber a topic).
        final List<SinkResult> sinkResults = safeList(filteredRecords)
                .stream()
                .map(fr -> doSinkAsync(fr, System.nanoTime(), 0))
                .collect(toList());

        // Add timing sink metrics. (The benefit of not using lamda records is better use of arthas for troubleshooting during operation.)
        final long sinkTimerBegin = System.nanoTime();

        // Wait for all sink to be completed.
        if (pipelineConfig.getCheckpoint().getQoS().isAnyRetriesAtMostOrStrictly()) {
            final Set<SinkResult> completedSinkResults = new HashSet<>(sinkResults.size());
            while (sinkResults.size() > 0) {
                final Iterator<SinkResult> it = sinkResults.iterator();
                while (it.hasNext()) {
                    final SinkResult sr = it.next();
                    // Notice: is done is not necessarily successful, both exception and cancellation will be done.
                    if (sr.getFuture().isDone()) {
                        Serializable sc = null;
                        try {
                            sc = sr.getFuture().get();
                            completedSinkResults.add(sr);

                            eventPublisher.publishEvent(new CountMeterEvent(
                                    MetricsName.sink_records_success,
                                    sr.getRecord().topic(),
                                    sr.getRecord().partition(),
                                    groupId,
                                    subscriber.getId(),
                                    null));
                        } catch (InterruptedException | CancellationException ex) {
                            log.error("{} :: {} :: Unable to getting sink result.", groupId, subscriber.getId(), ex);

                            eventPublisher.publishEvent(new CountMeterEvent(
                                    MetricsName.sink_records_failure,
                                    sr.getRecord().topic(),
                                    sr.getRecord().partition(),
                                    groupId,
                                    subscriber.getId(),
                                    null));

                            if (pipelineConfig.getCheckpoint().getQoS().isAnyRetriesAtMostOrStrictly()) {
                                if (shouldGiveUpRetry(sr.getRetryBegin(), sr.getRetryTimes())) {
                                    break; // give up and lose
                                }
                                sinkResults.add(doSinkAsync(sr.getRecord(), sr.getRetryBegin(), sr.getRetryTimes() + 1));
                            }
                        } catch (ExecutionException ex) {
                            log.error("{} :: {} :: Unable not to getting sink result.", groupId, subscriber.getId(), ex);

                            eventPublisher.publishEvent(new CountMeterEvent(
                                    MetricsName.sink_records_failure,
                                    sr.getRecord().topic(),
                                    sr.getRecord().partition(),
                                    groupId,
                                    subscriber.getId(),
                                    null));

                            final Throwable reason = ExceptionUtils.getRootCause(ex);
                            // User needs to give up trying again.
                            if (reason instanceof GiveUpRetryExecutionException) {
                                log.warn("{} :: {} :: User ask to give up re-trying again sink. sr : {}, reason :{}",
                                        groupId, subscriber.getId(), sr, reason.getMessage());
                            } else {
                                if (pipelineConfig.getCheckpoint().getQoS().isAnyRetriesAtMostOrStrictly()) {
                                    if (shouldGiveUpRetry(sr.getRetryBegin(), sr.getRetryTimes())) {
                                        break; // give up and lose
                                    }
                                    sinkResults.add(doSinkAsync(sr.getRecord(), sr.getRetryBegin(), sr.getRetryTimes() + 1));
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

                postAcknowledgeCountMeter(MetricsName.acknowledge_success, completedSinkResults);
            } catch (Throwable ex) {
                log.error(String.format("%s :: %s :: Failed to sink success acknowledge for %s", groupId, subscriber.getId(), ack), ex);

                postAcknowledgeCountMeter(MetricsName.acknowledge_failure, completedSinkResults);
            }
        } else {
            try {
                log.debug("{} :: {} :: Batch regardless of success or failure sink force acknowledging ...", groupId, subscriber.getId());
                ack.acknowledge();
                log.info("{} :: {} :: Force sink to acknowledged.", groupId, subscriber.getId());

                postAcknowledgeCountMeter(MetricsName.acknowledge_success, sinkResults);
            } catch (Throwable ex) {
                log.error(String.format("%s :: %s :: Failed to sink force acknowledge for %s", groupId, subscriber.getId(), ack), ex);

                postAcknowledgeCountMeter(MetricsName.acknowledge_failure, sinkResults);
            }
        }

        eventPublisher.publishEvent(new TimingMeterEvent(
                MetricsName.sink_time,
                topicDesc,
                null,
                groupId,
                subscriber.getId(),
                Duration.ofNanos(System.nanoTime() - sinkTimerBegin)));
    }

    private void postAcknowledgeCountMeter(MetricsName metrics,
                                           Collection<SinkResult> sinkResults) {
        sinkResults.stream()
                .map(sr -> new TopicPartition(sr.getRecord().topic(),
                        sr.getRecord().partition()))
                .distinct().forEach(tp -> {
                    eventPublisher.publishEvent(new CountMeterEvent(
                            metrics,
                            tp.topic(),
                            tp.partition(),
                            groupId,
                            subscriber.getId(),
                            null));
                });
    }

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    private SinkResult doSinkAsync(ConsumerRecord<String, ObjectNode> filteredRecord, long retryBegin, int retryTimes) {
        //final String key = filteredRecord.key();
        //final ObjectNode value = filteredRecord.value();
        final String subscribeId = KafkaUtil.getFirstValueAsString(filteredRecord.headers(), KEY_TENANT);
        final boolean isSequence = KafkaUtil.getFirstValueAsBoolean(filteredRecord.headers(), KEY_SEQUENCE);

        // Notice: For reduce the complexity, asynchronous execution is not supported here temporarily, because if the
        // sink implementation is like producer.send(), it is itself asynchronous, which will generate two layers of
        // future, and the processing is not concise enough
        //
        //// Determine the sink task executor.
        //final ThreadPoolExecutor executor = determineSinkExecutor(key);
        //final Future<? extends Serializable> future = executor.submit(() ->
        //        subscribeSink.doSink(registry, subscribeId, isSequence, filteredRecord));
        //return new SinkResult(filteredRecord, future, retryBegin, retryTimes);

        return new SinkResult(filteredRecord, subscribeSink.doSink(subscribeId, isSequence, filteredRecord),
                retryBegin, retryTimes);
    }

    @Getter
    @Setter
    @SuperBuilder
    @AllArgsConstructor
    @ToString(callSuper = true)
    static class SinkResult {
        private ConsumerRecord<String, ObjectNode> record;
        private Future<? extends Serializable> future;
        private long retryBegin;
        private int retryTimes;
    }

}