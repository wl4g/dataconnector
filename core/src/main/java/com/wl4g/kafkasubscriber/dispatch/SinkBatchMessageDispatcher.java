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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import com.wl4g.kafkasubscriber.util.Crc32Util;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

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
    private ISubscribeSink subscribeSink;

    public SinkBatchMessageDispatcher(ApplicationContext context,
                                      KafkaSubscriberProperties.SubscribePipelineProperties pipelineConfig,
                                      SubscribeFacade subscribeFacade,
                                      SubscriberRegistry registry) {
        super(context, pipelineConfig, pipelineConfig.getSink().getProcessProps(), subscribeFacade, registry);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // Create custom subscribe sinker. (Each processing pipeline uses different custom sink instances)
        this.subscribeSink = obtainSubscribeSink();
    }

    @Override
    public void doOnMessage(List<ConsumerRecord<String, ObjectNode>> filteredRecords, Acknowledgment ack) {
        // Sink from filtered topic.
        final List<SinkResult> result = safeList(filteredRecords).stream().map(this::doSinkAsync).collect(toList());

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
            final long timeout = pipelineConfig.getSink().getProcessProps().getCheckpointTimeout().toNanos();
            final List<Future<?>> futures = result.stream().map(SinkResult::getFuture).collect(toList());
            final long begin = System.nanoTime();
            while (futures.stream().filter(Future::isDone).count() < futures.size()) {
                Thread.yield(); // May give up the CPU
                if ((System.nanoTime() - begin) > timeout) {
                    if (pipelineConfig.getFilter().getProcessProps().getQos().isStrictly()) {
                        log.error("Timeout sent to filtered kafka topic, shutdown...");
                        // TODO re-add to sink executor queue for forever retry.
                        // If the timeout is exceeded, the program will exit.
                        //System.exit(1);
                    } else {
                        final List<JsonNode> recordValues = safeList(filteredRecords).stream().map(ConsumerRecord::value).collect(toList());
                        log.error("Timeout sent to filtered kafka topic, fail fast has been disabled,"
                                + "which is likely to result in loss of filtered data. - {}", recordValues);
                    }
                }
            }
            try {
                log.info("Batch sent success and acknowledging ...");
                ack.acknowledge();
                log.info("Sent success acknowledged.");
            } catch (Throwable ex) {
                log.error(String.format("Failed to sent success acknowledge for %s", ack), ex);
            }
        } else {
            try {
                log.info("Batch regardless of success or failure force acknowledging ...");
                ack.acknowledge();
                log.info("Force acknowledged.");
            } catch (Throwable ex) {
                log.error(String.format("Failed to force acknowledge for %s", ack), ex);
            }
        }
    }

    /**
     * {@link org.apache.kafka.clients.producer.internals.ProducerBatch completeFutureAndFireCallbacks at #L281}
     */
    private SinkResult doSinkAsync(ConsumerRecord<String, ObjectNode> filteredRecord) {
        final String key = filteredRecord.key();
        final JsonNode value = filteredRecord.value();
        //final long subscribeId = value.get("$$subscriberId").asLong(-1L);
        final boolean isSequence = value.get("$$isSequence").asBoolean(false);

        ThreadPoolExecutor executor = this.sharedNonSequenceExecutor;
        if (isSequence) {
            //final int mod = (int) subscribeId;
            final int mod = (int) Math.abs(Crc32Util.compute(key));
            executor = isolationSequenceExecutors.get(isolationSequenceExecutors.size() % mod);
        }
        final Future<?> future = executor.submit(() -> subscribeSink.apply(filteredRecord));

        return new SinkResult(filteredRecord, future);
    }

    private ISubscribeSink obtainSubscribeSink() {
        try {
            return context.getBean(pipelineConfig.getSink().getCustomSinkBeanName(), ISubscribeSink.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new IllegalStateException(String.format("Could not getting custom subscriber sink of bean %s",
                    pipelineConfig.getSink().getCustomSinkBeanName()));
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @AllArgsConstructor
    @ToString(callSuper = true)
    public static class SinkResult {
        private ConsumerRecord<String, ObjectNode> filteredRecord;
        private Future<?> future;
    }

}




