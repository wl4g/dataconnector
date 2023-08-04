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
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter.MetricsName;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
    protected final String topicDesc;
    protected final String groupId;

    public AbstractBatchMessageDispatcher(KafkaSubscribeConfiguration config,
                                          SubscribeEnginePipelineConfig pipelineConfig,
                                          SubscribeEngineCustomizer customizer,
                                          CachingSubscriberRegistry registry,
                                          String topicDesc,
                                          String groupId) {
        this.config = Assert2.notNullOf(config, "config");
        this.pipelineConfig = Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        this.customizer = Assert2.notNullOf(customizer, "customizer");
        this.registry = Assert2.notNullOf(registry, "registry");
        this.groupId = Assert2.hasTextOf(groupId, "groupId");
        this.topicDesc = Assert2.notNullOf(topicDesc, "topicDesc");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack) {
        try {
            addCounterMetrics(MetricsName.shared_consumed, topicDesc, null, groupId, null);

            final Timer timer = addTimerMetrics(MetricsName.shared_consumed_time,
                    topicDesc, null, groupId, null);

            timer.record(() -> doOnMessage(records, ack));
        } catch (Throwable ex) {
            log.error(String.format("%s :: Failed to process message. - %s", groupId, records), ex);
            // Commit directly if no quality of service is required.
            if (pipelineConfig.getParsedFilter().getFilterConfig().getCheckpoint().getQos().isMoseOnce()) {
                ack.acknowledge();
            }
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

    protected void addCounterMetrics(@NotNull SubscribeMeter.MetricsName metrics,
                                     @NotBlank String topic,
                                     @Null Integer partition,
                                     @NotBlank String groupId,
                                     @Null String subscriberId,
                                     @Null String... addTags) {
        Assert2.notNullOf(metrics, "metrics");
        Assert2.hasTextOf(topic, "topic");
        Assert2.hasTextOf(groupId, "groupId");

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
        SubscribeMeter.getDefault().counter(metrics.getName(), metrics.getHelp(), tags.toArray(new String[0])).increment();
    }

    protected Timer addTimerMetrics(@NotNull SubscribeMeter.MetricsName metrics,
                                    @NotBlank String topic,
                                    @Null Integer partition,
                                    @NotBlank String groupId,
                                    @Null String subscriberId,
                                    @Null String... addTags) {
        Assert2.notNullOf(metrics, "metrics");
        Assert2.hasTextOf(topic, "topic");
        Assert2.hasTextOf(groupId, "groupId");

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
        return SubscribeMeter.getDefault().timer(metrics.getName(), metrics.getHelp(),
                SubscribeMeter.DEFAULT_PERCENTILES, tags.toArray(new String[0]));
    }

    public static final String KEY_SUBSCRIBER_ID = getenv().getOrDefault("INTERNAL_SUBSCRIBER_ID", "$$sub");
    public static final String KEY_IS_SEQUENCE = getenv().getOrDefault("INTERNAL_IS_SEQUENCE", "$$seq");

}




