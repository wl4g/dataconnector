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
import com.wl4g.kafkasubscriber.bean.TenantInfo;
import com.wl4g.kafkasubscriber.coordinator.KafkaSubscribeCoordinator.KafkaCoordinatorConfigConfig;
import com.wl4g.kafkasubscriber.coordinator.KafkaSubscribeCoordinator.KafkaCoordinatorDiscoveryConfig;
import com.wl4g.kafkasubscriber.coordinator.strategy.AverageShardingStrategy;
import com.wl4g.kafkasubscriber.filter.DefaultRecordMatchSubscribeFilter;
import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.sink.DefaultPrintSubscribeSink;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import com.wl4g.kafkasubscriber.source.ISubscribeSourceProvider;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.util.unit.DataSize;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * The {@link SubscribeConfiguration}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@Setter
@SuperBuilder
@ToString
public class SubscribeConfiguration implements InitializingBean {
    private final ApplicationContext context;

    private @Builder.Default SubscribeDefinitionConfig definitions = new SubscribeDefinitionConfig();
    private @Builder.Default SubscribeCoordinatorConfig coordinator = new SubscribeCoordinatorConfig();
    private @Builder.Default List<SubscribeEnginePipelineConfig> pipelines = new ArrayList<>(1);

    public SubscribeConfiguration(ApplicationContext context) {
        this.context = Assert2.notNullOf(context, "context");
    }

    @Override
    public void afterPropertiesSet() {
        try {
            beforeValidate();
            initParse();
        } catch (Throwable th) {
            log.error("Failed to init subscriber properties", th);
            throw th;
        }
    }

    private void beforeValidate() {
        Assert2.notNullOf(definitions, "definitions");
        Assert2.notNullOf(coordinator, "coordinator");
        Assert2.notNullOf(pipelines, "pipelines");
        definitions.validate();
        coordinator.validate();
        pipelines.forEach(SubscribeEnginePipelineConfig::validate);
    }

    private void initParse() {
        // Parsing to actuate source,filter,sink
        safeList(pipelines).forEach(p -> {
            // Parse the sources.
            final ISubscribeSourceProvider sourceProvider = safeList(definitions.getSources()).stream()
                    .filter(s -> StringUtils.equals(p.getSource(), s.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(String.format("Not found the source definition '%s'", p.getSource())));
            p.setParsedSourceProvider(sourceProvider);

            // Parse the filter.
            final ISubscribeFilter filter = safeList(definitions.getFilters()).stream()
                    .filter(f -> StringUtils.equals(p.getFilter(), f.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Not found the filter '%s'", p.getFilter())));
            p.setParsedFilter(filter);

            // Parse the sink.
            if (isNotBlank(p.getSink())) {
                final ISubscribeSink sink = safeList(definitions.getSinks()).stream()
                        .filter(s -> StringUtils.equals(s.getName(), p.getSink()))
                        .findFirst().orElseThrow(() -> new IllegalArgumentException(String.format("Not found the sink '%s'", p.getSink())));
                p.setParsedSink(sink);
            }
        });
    }

    // ----- Definitions configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribeDefinitionConfig {
        private @Builder.Default List<ISubscribeSourceProvider> sources = new ArrayList<>(2);
        private @Builder.Default List<ISubscribeFilter> filters = new ArrayList<>(2);
        private @Builder.Default List<ISubscribeSink> sinks = new ArrayList<>(2);
        private @Builder.Default List<SubscriberInfo> subscribers = new ArrayList<>(2);
        private @Builder.Default List<TenantInfo> tenants = new ArrayList<>(2);
        //
        // Parsed to transient properties.
        //
        private transient Map<String, ISubscribeSourceProvider> sourceMap;
        private transient Map<String, ISubscribeFilter> filterMap;
        private transient Map<String, ISubscribeSink> sinkMap;
        private transient Map<String, SubscriberInfo> subscriberMap;
        private transient Map<String, TenantInfo> tenantMap;

        public void validate() {
            Assert2.notNullOf(sources, "sources");
            Assert2.notNullOf(filters, "filters");
            Assert2.notNullOf(sinks, "sinks");
            Assert2.notNullOf(subscribers, "subscribers");
            Assert2.notNullOf(tenants, "tenants");

            sources.forEach(ISubscribeSourceProvider::validate);
            filters.forEach(ISubscribeFilter::validate);
            sinks.forEach(ISubscribeSink::validate);
            subscribers.forEach(SubscriberInfo::validate);
            tenants.forEach(TenantInfo::validate);

            // Check for sources name duplicate.
            Assert2.isTrueOf(sources.size() == new HashSet<>(sources.stream()
                    .map(ISubscribeSourceProvider::getName).collect(toList())).size(), "sources name duplicate");
            // Check for filters name duplicate.
            Assert2.isTrueOf(filters.size() == new HashSet<>(filters.stream()
                    .map(ISubscribeFilter::getName).collect(toList())).size(), "filters name duplicate");
            // Check for sinks name duplicate.
            Assert2.isTrueOf(sinks.size() == new HashSet<>(sinks.stream()
                    .map(ISubscribeSink::getName).collect(toList())).size(), "sinks name duplicate");
        }

        //
        // Parsed to transient properties.
        //

        public Map<String, ISubscribeSourceProvider> getSourceMap() {
            if (Objects.isNull(sourceMap)) {
                sourceMap = safeList(sources).stream().collect(toMap(ISubscribeSourceProvider::getName, e -> e));
            }
            return sourceMap;
        }

        public Map<String, ISubscribeFilter> getFilterMap() {
            if (Objects.isNull(filterMap)) {
                filterMap = safeList(filters).stream().collect(toMap(ISubscribeFilter::getName, e -> e));
            }
            return filterMap;
        }

        public Map<String, ISubscribeSink> getSinkMap() {
            if (Objects.isNull(sinkMap)) {
                sinkMap = safeList(sinks).stream().collect(toMap(ISubscribeSink::getName, e -> e));
            }
            return sinkMap;
        }

        public Map<String, SubscriberInfo> getSubscriberMap() {
            if (Objects.isNull(subscriberMap)) {
                subscriberMap = safeList(subscribers)
                        .stream()
                        .filter(SubscriberInfo::isEnable)
                        .collect(toMap(SubscriberInfo::getId, e -> e));
            }
            return subscriberMap;
        }

        public Map<String, TenantInfo> getTenantMap() {
            if (Objects.isNull(tenantMap)) {
                tenantMap = safeList(tenants)
                        .stream()
                        .filter(TenantInfo::isEnable)
                        .collect(toMap(TenantInfo::getId, e -> e));
            }
            return tenantMap;
        }

    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    public static class SubscribeSourceConfig extends BaseConsumerConfig {
        private String topicPattern;

        public SubscribeSourceConfig() {
            getConsumerProps().put(ConsumerConfig.GROUP_ID_CONFIG, "shared_source_0");
        }

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
    public static class SubscribeFilterConfig {
        private @Builder.Default String name = DefaultRecordMatchSubscribeFilter.TYPE_NAME;
        private @Builder.Default String topicPrefix = "shared_filtered_";
        private @Builder.Default int topicPartitions = 10;
        private @Builder.Default short replicationFactor = 1;
        private @Builder.Default SubscribeExecutorConfig executorProps = new SubscribeExecutorConfig();
        private @Builder.Default CheckpointConfig checkpoint = new CheckpointConfig();

        public void validate() {
            this.executorProps.validate();
            Assert2.hasTextOf(topicPrefix, "topicPrefix");
            Assert2.hasTextOf(name, "customFilterBeanName");
            Assert2.notNullOf(checkpoint, "checkpoint");
            checkpoint.validate();
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    public static class SubscribeSinkConfig extends BaseConsumerConfig {
        private @Builder.Default String groupIdPrefix = "isolation_sink_";
        private @Builder.Default SubscribeExecutorConfig executorProps = new SubscribeExecutorConfig();

        public SubscribeSinkConfig() {
            setName(DefaultPrintSubscribeSink.TYPE_NAME);
        }

        public void validate() {
            super.validate();
            Assert2.hasTextOf(groupIdPrefix, "groupIdPrefix");
            getExecutorProps().validate();
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static abstract class BaseConsumerConfig {
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

        public BaseConsumerConfig optimizeProperties() {
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
    public static class SubscribeExecutorConfig {
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

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class CheckpointConfig {
        // TODO custom yaml with tag not support ???
        //private @Builder.Default CheckpointQoS qos = CheckpointQoS.RETRIES_AT_MOST;
        private @Builder.Default String qosType = CheckpointQoS.RETRIES_AT_MOST.name();
        private @Builder.Default int qoSMaxRetries = 5;
        private @Builder.Default long qoSMaxRetriesTimeout = 1800_000; // 30min
        private @Builder.Default int producerMaxCountLimit = 10;
        private @Builder.Default Properties producerProps = new Properties() {
            {
                setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                setProperty(ProducerConfig.ACKS_CONFIG, "0");
                setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
                setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "131072");
                setProperty(ProducerConfig.RETRIES_CONFIG, "5");
                setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "6000");
                setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // snappy|gzip|lz4|zstd|none
                setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            }
        };
        private @Builder.Default Properties defaultTopicProps = new Properties() {
            {
                setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");
                setProperty(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(1)));
                setProperty(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(DataSize.ofGigabytes(1).toBytes()));
                setProperty(TopicConfig.DELETE_RETENTION_MS_CONFIG, "86400000");
                //setProperty(TopicConfig.SEGMENT_MS_CONFIG, "86400000");
                //setProperty(TopicConfig.SEGMENT_BYTES_DOC, "1073741824");
                //setProperty(TopicConfig.SEGMENT_INDEX_BYTES_DOC, "10485760");
                //setProperty(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1");
                //setProperty(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "86400000");
                //setProperty(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "86400000");
                //setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
                //setProperty(TopicConfig.FLUSH_MS_CONFIG, "1000");
                //setProperty(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, "10000");
                //setProperty(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
            }
        };

        public void validate() {
            Assert2.notNullOf(qosType, "qos");
            Assert2.isTrueOf(qoSMaxRetries > 0, "qosMaxRetries > 0");
            Assert2.isTrueOf(producerMaxCountLimit > 0, "checkpointProducerMaxCountLimit > 0");
            Assert2.isTrueOf(qoSMaxRetriesTimeout > 0, "checkpointTimeoutMs > 0");
            // check for producer props.
            // TODO should to be priority choose sources bootstrap servers?
            // 但这里也应该统一使用, com.wl4g.kafkasubscriber.dispatch.CheckpointTopicManager.lambda$addTopicAllIfNecessary$0(CheckpointTopicManager.java:88)
            //Assert2.notNullOf(producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
            Assert2.notNullOf(producerProps.get(ProducerConfig.ACKS_CONFIG), "acks");
            Assert2.notNullOf(producerProps.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "request.timeout.ms");
            Assert2.notNullOf(producerProps.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "max.request.size");
            Assert2.notNullOf(producerProps.get(ProducerConfig.SEND_BUFFER_CONFIG), "send.buffer.bytes");
            Assert2.notNullOf(producerProps.get(ProducerConfig.RETRIES_CONFIG), "retries");
            Assert2.notNullOf(producerProps.get(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), "retry.backoff.ms");
            Assert2.notNullOf(producerProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG), "compression.type");
            // check for topic props.
            Assert2.notNullOf(defaultTopicProps.get(TopicConfig.CLEANUP_POLICY_CONFIG), "cleanup.policy");
            Assert2.notNullOf(defaultTopicProps.get(TopicConfig.RETENTION_MS_CONFIG), "retention.ms");
            Assert2.notNullOf(defaultTopicProps.get(TopicConfig.RETENTION_BYTES_CONFIG), "retention.bytes");
            //Assert2.notNullOf(defaultTopicProps.get(TopicConfig.DELETE_RETENTION_MS_CONFIG), "delete.retention.ms");
        }

        public CheckpointQoS getQos() {
            if (StringUtils.isNumeric(qosType)) {
                return CheckpointQoS.values()[Integer.parseInt(qosType)];
            }
            return CheckpointQoS.valueOf(qosType);
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
    public static class SubscribeEnginePipelineConfig {
        private String name;
        private @Builder.Default boolean enable = true;
        private String source;
        private String filter;
        private String sink;
        //
        // Parsed to transient properties.
        //
        private transient @NotNull ISubscribeSourceProvider parsedSourceProvider;
        private transient @NotBlank ISubscribeFilter parsedFilter;
        private transient @Null ISubscribeSink parsedSink;

        public void validate() {
            Assert2.hasTextOf(name, "name");
            Assert2.hasTextOf(source, "source");
            Assert2.hasTextOf(filter, "filter");
            Assert2.hasTextOf(sink, "sink");
        }
    }

    // ----- Coordinator configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class SubscribeCoordinatorConfig {
        private @Builder.Default String shardingStrategy = AverageShardingStrategy.TYPE;
        private @Builder.Default String bootstrapServers = "localhost:9092";
        private @Builder.Default KafkaCoordinatorConfigConfig configConfig = new KafkaCoordinatorConfigConfig();
        private @Builder.Default KafkaCoordinatorDiscoveryConfig discoveryConfig = new KafkaCoordinatorDiscoveryConfig();

        public void validate() {
            Assert2.hasTextOf(shardingStrategy, "shardingStrategy");
            Assert2.notNullOf(configConfig, "configConfig");
            Assert2.notNullOf(discoveryConfig, "discoveryConfig");
        }
    }

}
