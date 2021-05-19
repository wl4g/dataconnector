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

package com.wl4g.kafkasubscriber.config;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.filter.DefaultRecordMatchSubscribeFilter;
import com.wl4g.kafkasubscriber.sink.DefaultSubscribeSink;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.InitializingBean;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The {@link KafkaSubscriberProperties}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@Setter
@SuperBuilder
@ToString
@NoArgsConstructor
public class KafkaSubscriberProperties implements InitializingBean {
    public static final String LOCAL_PROCESS_ID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    private @Builder.Default List<SubscribePipelineProperties> pipelines = new ArrayList<>();
    private @Builder.Default List<SubscriberInfo> subscribers = new ArrayList<>(2);

    @Override
    public void afterPropertiesSet() throws Exception {
        pipelines.forEach(SubscribePipelineProperties::validate);
        preOptimizeProperties();
    }

    private void preOptimizeProperties() {
        pipelines.forEach(p -> {
            // The filter message handler is internally hardcoded to use JsonNode.
            p.getSource().getConsumerProps().put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConsumerBuilder.ObjectNodeDeserializer.class.getName());

            // Should be 'max.poll.records' equals to filter executor queue size.
            final Object originalMaxPollRecords = p.getSource().getConsumerProps().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
            p.getSource().getConsumerProps().put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(p.getFilter().getSharedExecutorQueueSize()));
            log.info("Optimized '{}' from {} to {} of pipeline.source groupId: {}", ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                    originalMaxPollRecords, p.getFilter().getSharedExecutorQueueSize(), p.getSource().getGroupId());

            // Need auto create the filtered topic by subscriber. (broker should also be set to allow)
            p.getSource().getConsumerProps().put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
            log.info("Optimized '{}' from {} to {} of pipeline.source groupId: {}", ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                    originalMaxPollRecords, "true", p.getSource().getGroupId());

            // Mandatory manual commit.
            p.getSource().getConsumerProps().put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            log.info("Optimized '{}' from {} to {} of pipeline.source groupId: {}", ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    originalMaxPollRecords, "false", p.getSource().getGroupId());
        });
    }


    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribePipelineProperties {
        private @Builder.Default SourceProperties source = new SourceProperties();
        private @Builder.Default FilterProperties filter = new FilterProperties();
        private @Builder.Default SinkProperties sink = new SinkProperties();

        public void validate() {
            source.validate();
            filter.validate();
            sink.validate();
        }
    }


    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static abstract class AbstractConsumerProperties {
        // By force: min(concurrency, topicPartitions.length)
        // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
        // But it's a pity that spring doesn't get it dynamically from broker.
        // Therefore, tuning must still be set manually, generally equal to the number of partitions.
        private @Builder.Default Integer parallelism = 1;
        private @Builder.Default Map<String, String> consumerProps = new HashMap<String, String>(4) {
            {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

                put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");

                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // none,latest,earliest
                put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

                put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
                put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
                put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(50 * 1024 * 1024));

                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                // Must be 'ObjectNodeDeserializer' for filter message handler.
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConsumerBuilder.ObjectNodeDeserializer.class.getName());
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            }
        };

        public String getGroupId() {
            return (String) consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG);
        }

        public void validate() {
            Assert2.isTrueOf(parallelism > 0, "parallelism > 0");
            Assert2.notNullOf(consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
            Assert2.notNullOf(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG), "group.id");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    public static class SourceProperties extends AbstractConsumerProperties {
        private Pattern topicPattern;

        public SourceProperties() {
            getConsumerProps().put(ConsumerConfig.GROUP_ID_CONFIG, "shared-source-".concat(LOCAL_PROCESS_ID));
        }

        @Override
        public void validate() {
            super.validate();
            Assert2.notNullOf(topicPattern, "topicPattern");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class FilterProperties {
        private @Builder.Default String topicPrefix = "filtered-";
        private @Builder.Default String customFilterBeanName = DefaultRecordMatchSubscribeFilter.BEAN_NAME;
        private @Builder.Default int sharedExecutorThreadPoolSize = 50;
        private @Builder.Default int sharedExecutorQueueSize = 500;
        private @Builder.Default int sequenceExecutorsMaxCountLimit = 100;
        private @Builder.Default int sequenceExecutorsPerQueueSize = 100;
        private @Builder.Default boolean preStartAllCoreThreads = true;
        private @Builder.Default int producerMaxCountLimit = 10;
        private @Builder.Default boolean isBestQoS = true;
        private @Builder.Default Duration checkpointTimeout = Duration.ofHours(6);
        private @Builder.Default boolean checkpointFastFail = true;
        private @Builder.Default Map<String, String> producerProps = new HashMap<String, String>(4) {
            {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                put(ProducerConfig.ACKS_CONFIG, "0");
                put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
                put(ProducerConfig.SEND_BUFFER_CONFIG, "131072");
                put(ProducerConfig.RETRIES_CONFIG, "5");
                put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "6000");
                put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // snappy|gzip|lz4|zstd|none
            }
        };

        public void validate() {
            Assert2.hasTextOf(customFilterBeanName, "customFilterBeanName");
            Assert2.isTrueOf(checkpointTimeout.toMillis() > 0, "checkpointTimeoutMs > 0");
            Assert2.notNullOf(producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
            Assert2.notNullOf(producerProps.get(ProducerConfig.ACKS_CONFIG), "acks");
            Assert2.notNullOf(producerProps.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "request.timeout.ms");
            Assert2.notNullOf(producerProps.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "max.request.size");
            Assert2.notNullOf(producerProps.get(ProducerConfig.SEND_BUFFER_CONFIG), "send.buffer.bytes");
            Assert2.notNullOf(producerProps.get(ProducerConfig.RETRIES_CONFIG), "retries");
            Assert2.notNullOf(producerProps.get(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), "retry.backoff.ms");
            Assert2.notNullOf(producerProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG), "compression.type");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    public static class SinkProperties extends AbstractConsumerProperties {
        private @Builder.Default String customSinkBeanName = DefaultSubscribeSink.BEAN_NAME;
        // By force: min(concurrency, topicPartitions.length)
        // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
        // But it's a pity that spring doesn't get it dynamically from broker.
        // Therefore, tuning must still be set manually, generally equal to the number of partitions.
        private @Builder.Default Integer parallelism = 1;
        private @Builder.Default int sharedExecutorThreadPoolSize = 50;
        private @Builder.Default int sharedExecutorQueueSize = 500;
        private @Builder.Default int sequenceExecutorsMaxCountLimit = 100;
        private @Builder.Default int sequenceExecutorsPerQueueSize = 100;
        private @Builder.Default boolean preStartAllCoreThreads = true;
        private @Builder.Default int producerMaxCountLimit = 10;
        private @Builder.Default boolean isBestQoS = true;
        private @Builder.Default Duration checkpointTimeout = Duration.ofHours(6);
        private @Builder.Default boolean checkpointFastFail = true;

        public SinkProperties() {
            getConsumerProps().put(ConsumerConfig.GROUP_ID_CONFIG, "filtered-sink-".concat(LOCAL_PROCESS_ID));
        }

        public void validate() {
            Assert2.isTrueOf(parallelism > 0, "parallelism > 0");
        }
    }

}
