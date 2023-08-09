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

package com.wl4g.streamconnect.config;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.bean.TenantInfo;
import com.wl4g.streamconnect.checkpoint.IProcessCheckpoint;
import com.wl4g.streamconnect.coordinator.KafkaStreamConnectCoordinator.KafkaCoordinatorBusConfig;
import com.wl4g.streamconnect.coordinator.KafkaStreamConnectCoordinator.KafkaCoordinatorDiscoveryConfig;
import com.wl4g.streamconnect.coordinator.strategy.AverageShardingStrategy;
import com.wl4g.streamconnect.filter.IProcessFilter;
import com.wl4g.streamconnect.filter.StandardExprProcessFilter;
import com.wl4g.streamconnect.map.IProcessMapper;
import com.wl4g.streamconnect.sink.IProcessSink;
import com.wl4g.streamconnect.sink.NoOpProcessSink;
import com.wl4g.streamconnect.source.ISourceProvider;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.InitializingBean;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * The {@link StreamConnectProperties}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@Setter
@ToString
public class StreamConnectProperties implements InitializingBean {
    private @Builder.Default SubscribeDefinitionProperties definitions = new SubscribeDefinitionProperties();
    private @Builder.Default SubscribeCoordinatorProperties coordinator = new SubscribeCoordinatorProperties();
    private @Builder.Default List<SubscribePipelineProperties> pipelines = new ArrayList<>(1);

    @Override
    public void afterPropertiesSet() {
        try {
            validate();
        } catch (Throwable th) {
            log.error("Failed to validate stream connect properties", th);
            throw th;
        }
    }

    private void validate() {
        Assert2.notNullOf(definitions, "definitions");
        Assert2.notNullOf(coordinator, "coordinator");
        Assert2.notNullOf(pipelines, "pipelines");
        definitions.validate();
        coordinator.validate();
        pipelines.forEach(SubscribePipelineProperties::validate);
    }

    // ----- Definitions configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribeDefinitionProperties {
        private @Builder.Default List<IProcessCheckpoint> checkpoints = new ArrayList<>(2);
        private @Builder.Default List<ISourceProvider> sources = new ArrayList<>(2);
        private @Builder.Default List<IProcessFilter> filters = new ArrayList<>(2);
        private @Builder.Default List<IProcessMapper> mappers = new ArrayList<>(2);
        private @Builder.Default List<IProcessSink> sinks = new ArrayList<>(2);
        private @Builder.Default List<SubscriberInfo> subscribers = new ArrayList<>(2);
        private @Builder.Default List<TenantInfo> tenants = new ArrayList<>(2);

        public void validate() {
            Assert2.notEmptyOf(checkpoints, "checkpoints");
            Assert2.notEmptyOf(sources, "sources");
            Assert2.notEmptyOf(filters, "filters");
            Assert2.notEmptyOf(sinks, "sinks");
            Assert2.notEmptyOf(subscribers, "subscribers");
            Assert2.notEmptyOf(tenants, "tenants");

            checkpoints.forEach(IProcessCheckpoint::validate);
            sources.forEach(ISourceProvider::validate);
            filters.forEach(IProcessFilter::validate);
            sinks.forEach(IProcessSink::validate);
            subscribers.forEach(SubscriberInfo::validate);
            tenants.forEach(TenantInfo::validate);

            // Check for checkpoint name duplicate.
            Assert2.isTrueOf(checkpoints.size() == new HashSet<>(checkpoints.stream()
                    .map(IProcessCheckpoint::getName).collect(toList())).size(), "checkpoint name duplicate");
            // Check for sources name duplicate.
            Assert2.isTrueOf(sources.size() == new HashSet<>(sources.stream()
                    .map(ISourceProvider::getName).collect(toList())).size(), "sources name duplicate");
            // Check for filters name duplicate.
            Assert2.isTrueOf(filters.size() == new HashSet<>(filters.stream()
                    .map(IProcessFilter::getName).collect(toList())).size(), "filters name duplicate");
            // Check for sinks name duplicate.
            Assert2.isTrueOf(sinks.size() == new HashSet<>(sinks.stream()
                    .map(IProcessSink::getName).collect(toList())).size(), "sinks name duplicate");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribeSourceProperties extends BaseConsumerProperties {
        private String topicPattern;

        @Override
        public void validate() {
            super.validate();
            Assert2.hasTextOf(topicPattern, "topicPattern");
            Assert2.notNullOf(getConsumerProps().get(ConsumerConfig.GROUP_ID_CONFIG), "group.id");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribeProcessProperties {
        private @Builder.Default String name = StandardExprProcessFilter.TYPE_NAME;
        private @Builder.Default SubscribeExecutorProperties executorConfig = new SubscribeExecutorProperties();

        public void validate() {
            this.executorConfig.validate();
            Assert2.hasTextOf(name, "customFilterBeanName");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    public static class SubscribeSinkProperties extends BaseConsumerProperties {
        private @Builder.Default String groupIdPrefix = "subscribe_group_sink_";
        private @Builder.Default SubscribeExecutorProperties executorConfig = new SubscribeExecutorProperties();

        public SubscribeSinkProperties() {
            setName(NoOpProcessSink.TYPE_NAME);
        }

        public void validate() {
            super.validate();
            Assert2.hasTextOf(groupIdPrefix, "groupIdPrefix");
            getExecutorConfig().validate();
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static abstract class BaseConsumerProperties {
        private String name;
        // By force: min(concurrency, topicPartitions.length)
        // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
        // But it's a pity that spring doesn't get it dynamically from broker.
        // Therefore, tuning must still be set manually, generally equal to the number of partitions.
        private @Builder.Default int parallelism = 1;
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

        public void validate() {
            Assert2.hasTextOf(name, "name");
            Assert2.isTrueOf(parallelism > 0 && parallelism < 100, "parallelism > 0 && parallelism < 100");
            Assert2.notNullOf(consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
        }

        public String getRequiredBootstrapServers() {
            return Assert2.hasTextOf(consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
        }

        public String getGroupId() {
            return getConsumerProps().get(ConsumerConfig.GROUP_ID_CONFIG);
        }

        public BaseConsumerProperties optimizeProperties() {
            // The filter message handler is internally hardcoded to use JsonNode.
            final String oldKeyDeserializer = getConsumerProps().get(KEY_DESERIALIZER_CLASS_CONFIG);
            getConsumerProps().put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            log.info("Optimized source '{}' from {} to {} of groupId: {}", KEY_DESERIALIZER_CLASS_CONFIG,
                    getConsumerProps().get(KEY_DESERIALIZER_CLASS_CONFIG), oldKeyDeserializer, getGroupId());

            final String oldValueDeserializer = getConsumerProps().get(VALUE_DESERIALIZER_CLASS_CONFIG);
            getConsumerProps().put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConsumerBuilder.ObjectNodeDeserializer.class.getName());
            log.info("Optimized source '{}' from {} to {} of groupId: {}", VALUE_DESERIALIZER_CLASS_CONFIG,
                    getConsumerProps().get(VALUE_DESERIALIZER_CLASS_CONFIG), oldValueDeserializer, getGroupId());

            // Need auto create the filtered topic by subscriber. (broker should also be set to allow)
            final String oldAutoCreateTopics = getConsumerProps().get(ALLOW_AUTO_CREATE_TOPICS_CONFIG);
            getConsumerProps().put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
            log.info("Optimized source '{}' from {} to {} of groupId: {}", ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                    getConsumerProps().get(ALLOW_AUTO_CREATE_TOPICS_CONFIG), oldAutoCreateTopics, getGroupId());

            // Mandatory manual commit.
            final String oldEnableAutoCommit = getConsumerProps().get(ENABLE_AUTO_COMMIT_CONFIG);
            getConsumerProps().put(ENABLE_AUTO_COMMIT_CONFIG, "false");
            log.info("Optimized source '{}' from {} to {} of groupId: {}", ENABLE_AUTO_COMMIT_CONFIG,
                    getConsumerProps().get(ENABLE_AUTO_COMMIT_CONFIG), oldEnableAutoCommit, getGroupId());

            // TODO checking by merge to sources and filter with pipeline
            // Should be 'max.poll.records' equals to filter executor queue size.
            // final Object originalMaxPollRecords = getConsumerProps().get(MAX_POLL_RECORDS_CONFIG);
            // getConsumerProps().put(MAX_POLL_RECORDS_CONFIG,
            //         String.valueOf(getFilter().getProcessProps().getSharedExecutorQueueSize()));
            // log.info("Optimized '{}' from {} to {} of pipeline.source groupId: {}",
            //         MAX_POLL_RECORDS_CONFIG, originalMaxPollRecords, getFilter().getProcessProps()
            //                 .getSharedExecutorQueueSize(), getGroupId());

            return this;
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribeExecutorProperties {
        private @Builder.Default int sharedExecutorThreadPoolSize = 50;
        private @Builder.Default int sharedExecutorQueueSize = 500;
        private @Builder.Default int sequenceExecutorsMaxCountLimit = 100;
        private @Builder.Default int sequenceExecutorsPerQueueSize = 100;
        private @Builder.Default boolean executorWarmUp = true;

        public void validate() {
            Assert2.isTrueOf(sharedExecutorThreadPoolSize > 0, "sharedExecutorThreadPoolSize > 0");
            Assert2.isTrueOf(sharedExecutorQueueSize > 0, "sharedExecutorQueueSize > 0");
            Assert2.isTrueOf(sequenceExecutorsMaxCountLimit > 0, "sequenceExecutorsMaxCountLimit > 0");
            Assert2.isTrueOf(sequenceExecutorsPerQueueSize > 0, "sequenceExecutorsPerQueueSize > 0");
        }
    }

    public enum CheckpointQoS {
        /**
         * 0: Execute only once and give up if it fails.
         */
        AT_MOST_ONCE,

        /**
         * 1: Max retries then give up if it fails.
         */
        RETRIES_AT_MOST,

        /**
         * 2: If the failure still occurs after the maximum retries, the first consecutive
         * successful part in the offset order of this batch will be submitted, and the other
         * failed offsets will be abandoned and submitted until the next re-consumption.
         */
        RETRIES_AT_MOST_STRICTLY,

        /**
         * 3: Strictly not lost, forever retries if it fails.
         */
        STRICTLY;

        public boolean isMoseOnce() {
            return this == AT_MOST_ONCE;
        }

        public boolean isRetriesAtMost() {
            return this == RETRIES_AT_MOST;
        }

        public boolean isRetriesAtMostStrictly() {
            return this == RETRIES_AT_MOST_STRICTLY;
        }

        public boolean isStrictly() {
            return this == STRICTLY;
        }

        public boolean isMoseOnceOrRetriesAtMost() {
            return isMoseOnce() || isRetriesAtMost();
        }

        public boolean isMoseOnceOrAnyRetriesAtMost() {
            return isMoseOnceOrRetriesAtMost() || isRetriesAtMostStrictly();
        }

        public boolean isRetriesAtMostOrStrictly() {
            return isRetriesAtMost() || isStrictly();
        }

        public boolean isAnyRetriesAtMostOrStrictly() {
            return isRetriesAtMostOrStrictly() || isRetriesAtMostStrictly();
        }

        public boolean isAnd(CheckpointQoS... qos) {
            return Arrays.stream(qos).allMatch(this::equals);
        }

        public boolean isOr(CheckpointQoS... qos) {
            return Arrays.asList(qos).contains(this);
        }

    }

    // ----- Pipelines configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribePipelineProperties {
        private String name;
        private @Builder.Default boolean enable = true;
        private @NotBlank String checkpoint;
        private @NotBlank String source;
        private @NotEmpty List<String> filters;
        private @NotEmpty List<String> mappers;
        private @NotBlank String sink;

        public void validate() {
            Assert2.hasTextOf(name, "name");
            Assert2.hasTextOf(checkpoint, "checkpoint");
            Assert2.hasTextOf(source, "source");
            Assert2.notEmptyOf(filters, "filters");
            Assert2.notEmptyOf(mappers, "mappers");
            Assert2.hasTextOf(sink, "sink");
        }
    }

    // ----- Coordinator configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribeCoordinatorProperties {
        private @Builder.Default String shardingStrategy = AverageShardingStrategy.TYPE;
        private @Builder.Default String bootstrapServers = "localhost:9092";
        private @Builder.Default KafkaCoordinatorBusConfig configConfig = new KafkaCoordinatorBusConfig();
        private @Builder.Default KafkaCoordinatorDiscoveryConfig discoveryConfig = new KafkaCoordinatorDiscoveryConfig();

        public void validate() {
            Assert2.hasTextOf(shardingStrategy, "shardingStrategy");
            Assert2.notNullOf(configConfig, "configConfig");
            Assert2.notNullOf(discoveryConfig, "discoveryConfig");
        }
    }

}
