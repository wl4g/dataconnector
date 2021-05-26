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
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.CheckpointConfig;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeEnginePipelineConfig;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter.MetricsName;
import com.wl4g.kafkasubscriber.meter.SubscribeMeterEventHandler.CountMeterEvent;
import com.wl4g.kafkasubscriber.meter.SubscribeMeterEventHandler.TimingMeterEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static java.lang.System.getenv;

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

    protected final KafkaSubscribeConfiguration config;
    protected final SubscribeEnginePipelineConfig pipelineConfig;
    protected final SubscribeEngineCustomizer customizer;
    protected final CachingSubscriberRegistry registry;
    protected final ApplicationEventPublisher eventPublisher;
    protected final String topicDesc;
    protected final String groupId;

    public AbstractBatchMessageDispatcher(KafkaSubscribeConfiguration config,
                                          SubscribeEnginePipelineConfig pipelineConfig,
                                          SubscribeEngineCustomizer customizer,
                                          CachingSubscriberRegistry registry,
                                          ApplicationEventPublisher eventPublisher,
                                          String topicDesc,
                                          String groupId) {
        this.config = Assert2.notNullOf(config, "config");
        this.pipelineConfig = Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        this.customizer = Assert2.notNullOf(customizer, "customizer");
        this.registry = Assert2.notNullOf(registry, "registry");
        this.eventPublisher = Assert2.notNullOf(eventPublisher, "eventPublisher");
        this.groupId = Assert2.hasTextOf(groupId, "groupId");
        this.topicDesc = Assert2.notNullOf(topicDesc, "topicDesc");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack) {
        final long sharedConsumedTimerBegin = System.nanoTime();
        try {
            eventPublisher.publishEvent(new CountMeterEvent(
                    MetricsName.shared_consumed,
                    topicDesc,
                    null,
                    groupId, null, null));

            doOnMessage(records, ack);
        } catch (Throwable ex) {
            log.error(String.format("%s :: Failed to process message. - %s", groupId, records), ex);
            // Commit directly if no quality of service is required.
            if (pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint().getQos().isMoseOnce()) {
                ack.acknowledge();
            }
        } finally {
            eventPublisher.publishEvent(new TimingMeterEvent(
                    MetricsName.shared_consumed_time,
                    topicDesc,
                    null,
                    groupId,
                    null,
                    Duration.ofNanos(System.nanoTime() - sharedConsumedTimerBegin)));
        }
    }

    protected abstract void doOnMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack);

    /**
     * Max retries then give up if it fails.
     */
    protected boolean shouldGiveUpRetry(long retryBegin, int retryTimes) {
        final CheckpointConfig checkpoint = pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint();
        return checkpoint.getQos().isMoseOnceOrAnyRetriesAtMost()
                && (retryTimes > checkpoint.getQoSMaxRetries()
                || (System.nanoTime() - retryBegin) > Duration.ofMillis(checkpoint.getQoSMaxRetriesTimeout()).toNanos());
    }

    public static final String KEY_SUBSCRIBER_ID = getenv().getOrDefault("INTERNAL_SUBSCRIBER_ID", "$$sub");
    public static final String KEY_IS_SEQUENCE = getenv().getOrDefault("INTERNAL_IS_SEQUENCE", "$$seq");

}




