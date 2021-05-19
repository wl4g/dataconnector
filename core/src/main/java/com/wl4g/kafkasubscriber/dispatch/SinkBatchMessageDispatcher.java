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
import com.wl4g.kafkasubscriber.util.NamedThreadFactory;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Collections.synchronizedList;
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
    private final ThreadPoolExecutor sharedNonSequenceSinkExecutor;
    private final List<ThreadPoolExecutor> isolationSequenceSinkExecutors;

    public SinkBatchMessageDispatcher(ApplicationContext context,
                                      KafkaSubscriberProperties.SubscribePipelineProperties config,
                                      SubscribeFacade customizer,
                                      SubscriberRegistry registry) {
        super(context, config, customizer, registry);

        // Create the shared filter single executor.
        this.sharedNonSequenceSinkExecutor = new ThreadPoolExecutor(
                config.getSink().getSharedExecutorThreadPoolSize(),
                config.getSink().getSharedExecutorThreadPoolSize(),
                0L, TimeUnit.MILLISECONDS,
                // TODO or use bounded queue
                new LinkedBlockingQueue<>(config.getSink().getSharedExecutorQueueSize()),
                new NamedThreadFactory("shared-".concat(getClass().getSimpleName())));
        if (config.getSink().isPreStartAllCoreThreads()) {
            this.sharedNonSequenceSinkExecutor.prestartAllCoreThreads();
        }

        // Create the sequence filter executors.
        this.isolationSequenceSinkExecutors = synchronizedList(new ArrayList<>(config.getSink().getSequenceExecutorsMaxCountLimit()));
        for (int i = 0; i < config.getSink().getSequenceExecutorsMaxCountLimit(); i++) {
            final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    // TODO or use bounded queue
                    new LinkedBlockingQueue<>(config.getSink().getSequenceExecutorsPerQueueSize()),
                    new NamedThreadFactory("sequence-".concat(getClass().getSimpleName())));
            if (config.getSink().isPreStartAllCoreThreads()) {
                executor.prestartAllCoreThreads();
            }
            this.isolationSequenceSinkExecutors.add(executor);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            log.info("Closing shared non sequence filter executor...");
            sharedNonSequenceSinkExecutor.shutdown();
            log.info("Closed shared non sequence filter executor.");
        } catch (Throwable ex) {
            log.error("Failed to close shared filter executor.", ex);
        }
        isolationSequenceSinkExecutors.forEach(executor -> {
            try {
                log.info("Closing sequence filter executor {}...", executor);
                executor.shutdown();
                log.info("Closed sequence filter executor {}.", executor);
            } catch (Throwable ex) {
                log.error(String.format("Failed to close sequence filter executor %s.", executor), ex);
            }
        });
    }

    @Override
    public void doOnMessage(List<ConsumerRecord<String, ObjectNode>> filteredRecords, Acknowledgment ack) {
        // Sink from filtered topic.
        final List<SinkResult> result = safeList(filteredRecords).stream().map(this::doSinkFromFilteredAsync).collect(toList());

        // Wait for all filtered records to be sent completed.
        //
        // Notice: Although it is best to support the config ‘bestQoS’ to the subscriber level, the current model
        // of sharing the original data of the groupId can only support the whole batch.
        //
        // The purpose of this design is to adapt to the scenario of large message traffic, because if different
        // groupIds are used to consume the original message, a large amount of COPY traffic may be generated before
        // filtering, that is, bandwidth is wasted from Kafka broker to this Pod Kafka consumer.
        //
        if (config.getSink().isBestQoS()) {
            final long timeout = config.getSink().getCheckpointTimeout().toNanos();
            final List<Future<?>> futures = result.stream().map(SinkResult::getFuture).collect(toList());
            final long begin = System.nanoTime();
            while (futures.stream().filter(Future::isDone).count() < futures.size()) {
                Thread.yield();
                if ((System.nanoTime() - begin) > timeout) {
                    if (config.getSink().isCheckpointFastFail()) {
                        log.error("Timeout sent to filtered kafka topic, shutdown...");
                        // If the timeout is exceeded, the program will exit.
                        System.exit(1);
                    } else {
                        final List<JsonNode> recordValues = safeList(filteredRecords).stream()
                                .map(ConsumerRecord::value).collect(toList());
                        log.error("Timeout sent to filtered kafka topic, fail fast has been disabled," +
                                "which is likely to result in loss of filtered data. - {}", recordValues);
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
    private SinkResult doSinkFromFilteredAsync(ConsumerRecord<String, ObjectNode> filteredRecord) {
        final JsonNode value = filteredRecord.value();
        final long subscribeId = value.get("$subscriberId").asLong();
        final boolean isSequence = value.get("$isSequence").asBoolean();

        ThreadPoolExecutor executor = this.sharedNonSequenceSinkExecutor;
        if (isSequence) {
            executor = isolationSequenceSinkExecutors.get(
                    isolationSequenceSinkExecutors.size() % Math.abs(Long.hashCode(subscribeId)));
        }

        final ISubscribeSink subscribeSink = getSubscribeSink();
        final Future<?> future = executor.submit(() -> subscribeSink.apply(filteredRecord));

        return new SinkResult(filteredRecord, future);
    }

    private ISubscribeSink getSubscribeSink() {
        try {
            return context.getBean(config.getSink().getCustomSinkBeanName(), ISubscribeSink.class);
        } catch (NoSuchBeanDefinitionException ex) {
            throw new IllegalStateException(String.format("Could not getting custom sink of bean %s",
                    config.getSink().getCustomSinkBeanName()));
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




