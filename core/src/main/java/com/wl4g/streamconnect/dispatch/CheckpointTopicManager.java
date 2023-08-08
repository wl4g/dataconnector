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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.checkpoint.IProcessCheckpoint;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.PipelineConfig;
import com.wl4g.streamconnect.coordinator.CachingSubscriberRegistry;
import com.wl4g.streamconnect.custom.StreamConnectEngineCustomizer;
import com.wl4g.streamconnect.exception.TopicConfigurationException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

/**
 * The {@link CheckpointTopicManager}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@AllArgsConstructor
public class CheckpointTopicManager {
    private final StreamConnectConfiguration config;
    private final StreamConnectEngineCustomizer customizer;
    private final CachingSubscriberRegistry registry;

    /**
     * Create the all pipeline topics and configuration to the subscribers (if necessary)
     *
     * @param timeout The timeout period for calling the broker asynchronously at each step.
     */
    public void initPipelinesTopicIfNecessary(int timeout) throws TopicConfigurationException {
        log.info("Initializing all pipeline topics if necessary of {} ...", config.getPipelines().size());
        config.getPipelines().forEach(pipeline -> {
            try {
                addSubscribersTopicIfNecessary(pipeline, registry.getSubscribers(pipeline.getName()), timeout);
            } catch (Throwable ex) {
                throw new TopicConfigurationException(String.format("Failed to create topics of pipeline %s",
                        pipeline.getName()), ex);
            }
        });
    }

    /**
     * Create the topic and configuration to the subscribers (if necessary)
     *
     * @param pipelineConfig pipelineConfig config.
     * @param subscribers    subscribers.
     * @param timeout        The timeout period for calling the broker asynchronously at each step.
     */
    public void addSubscribersTopicIfNecessary(PipelineConfig pipelineConfig,
                                               Collection<SubscriberInfo> subscribers,
                                               int timeout) throws TopicConfigurationException {
        Assert2.notNullOf(pipelineConfig, "pipelineConfig");
        log.info("{} :: Creating topics if necessary of {} ...", pipelineConfig.getName(),
                config.getPipelines().size());

        final IProcessCheckpoint checkpoint = pipelineConfig.getCheckpoint();
        final String topicPrefix = checkpoint.getTopicPrefix();
        final int partitions = checkpoint.getTopicPartitions();
        final short replicationFactor = checkpoint.getReplicationFactor();

        // TODO may per subscriber a consumer properties(brokers)
        String brokerServers = checkpoint.getProducerProps().getProperty(BOOTSTRAP_SERVERS_CONFIG);
        brokerServers = isBlank(brokerServers) ? checkpoint.getProducerProps().getProperty(BOOTSTRAP_SERVERS_CONFIG) : brokerServers;

        try (AdminClient adminClient = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG, brokerServers))) {
            createOrUpdateBatchTopicsIfNecessary(adminClient, pipelineConfig.getName(), topicPrefix,
                    partitions, replicationFactor, brokerServers, customizer, subscribers)
                    .get(timeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException ex) {
            throw new TopicConfigurationException(ex);
        }
    }

    public static KafkaFuture<Object> createOrUpdateBatchTopicsIfNecessary(
            AdminClient adminClient,
            String pipelineName,
            String topicPrefix,
            int partitions,
            short replicationFactor,
            String brokerServers,
            StreamConnectEngineCustomizer customizer,
            Collection<SubscriberInfo> subscribers) {

        final List<AlterTopicConfig> topics = safeList(subscribers).stream()
                .map(subscriber -> {
                    // TODO add support more topic config props.
                    final Map<String, String> overrideItems = new HashMap<>();
                    overrideItems.put(RETENTION_MS_CONFIG, String.valueOf(subscriber.getRule().getLogRetentionTime().toMillis()));
                    overrideItems.put(RETENTION_BYTES_CONFIG, String.valueOf(subscriber.getRule().getLogRetentionBytes().toBytes()));
                    return new AlterTopicConfig(customizer.generateCheckpointTopic(pipelineName,
                            topicPrefix, subscriber.getId()), subscriber, overrideItems);
                })
                .collect(toList());

        return adminClient
                .listTopics() // Listing of all topics config.
                .names() // getting topic names.
                .thenApply(allTopicNames -> {
                    log.info("{} :: List to existing topics: {}", brokerServers, allTopicNames);

                    KafkaFuture<Void> createFuture = null;
                    KafkaFuture<Object> updateFuture = null;

                    // Create new topics.
                    log.info("{} :: Creating to topics: {}", brokerServers, topics);
                    final List<AlterTopicConfig> newTopics = safeList(topics).stream()
                            .filter(topic -> !allTopicNames.contains(topic.getTopic()))
                            .collect(Collectors.toList());

                    if (!newTopics.isEmpty()) {
                        final List<NewTopic> _newTopics = newTopics.stream()
                                .map(topic -> {
                                    final NewTopic newTopic = new NewTopic(topic.getTopic(), partitions, replicationFactor);
                                    newTopic.configs(safeMap(topic.getExpectedUpdateItems()));
                                    return newTopic;
                                })
                                .collect(toList());

                        createFuture = adminClient.createTopics(_newTopics).all();
                        log.info("{} :: Created new topics: {}", brokerServers, newTopics);
                    }

                    // Update new topics name.
                    final List<AlterTopicConfig> updateTopics = safeList(topics).stream()
                            .filter(topic -> allTopicNames.contains(topic.getTopic()))
                            .collect(toList());
                    try {
                        updateFuture = updateBatchTopicsIfNecessary(adminClient, brokerServers, updateTopics);
                    } catch (Throwable ex) {
                        throw new RuntimeException(ex);
                    }

                    return KafkaFuture.allOf(createFuture, updateFuture);
                });
    }

    public static KafkaFuture<Object> updateBatchTopicsIfNecessary(
            AdminClient adminClient,
            String brokerServers,
            List<AlterTopicConfig> updateTopics) {

        // Convert to topic->subscriber map.
        final Map<String, AlterTopicConfig> updateTopicMap = updateTopics.stream()
                .collect(Collectors.toMap(AlterTopicConfig::getTopic, e -> e));

        final Set<String> topicNames = updateTopicMap.keySet();
        log.info("{} :: Updating to topics: {}", brokerServers, topicNames);

        // Getting existing topics config.
        final List<ConfigResource> topicConfigResources = safeList(updateTopics)
                .stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic.getTopic()))
                .collect(Collectors.toList());

        return adminClient
                .describeConfigs(topicConfigResources)
                .all()
                .thenApply(existingConfigMap -> {
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: topics: {}, existing config items: {}", brokerServers, topicNames, existingConfigMap);
                    }

                    // Wrap per topic update alter config OP. (If necessary)
                    final Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = safeMap(existingConfigMap)
                            .entrySet()
                            .stream()
                            .filter(existing -> {
                                // If any config item is not the expected value, it needs to be update, otherwise not need update.
                                final Map<String, String> expectedUpdateItems = updateTopicMap.get(existing.getKey().name())
                                        .getExpectedUpdateItems();
                                return !expectedUpdateItems.entrySet().stream().allMatch(e1 ->
                                        existing.getValue().entries().stream().anyMatch(e2 -> StringUtils.equals(e1.getKey(), e2.name())
                                                && StringUtils.equals(e1.getValue(), e2.value())));
                            })
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    existing -> {
                                        final Map<String, String> expectedUpdateItems = updateTopicMap.get(existing.getKey().name())
                                                .getExpectedUpdateItems();
                                        final List<ConfigEntry> entries = new ArrayList<>(existing.getValue().entries());
                                        // Add the update topic config items.
                                        entries.addAll(safeMap(expectedUpdateItems)
                                                .entrySet()
                                                .stream()
                                                .map(e2 -> new ConfigEntry(e2.getKey(), e2.getValue())).collect(toList()));
                                        // Convert to alter config OP
                                        return entries.stream().map(entry -> new AlterConfigOp(entry,
                                                AlterConfigOp.OpType.SET)).collect(toList());
                                    }
                            ));
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: topics: {}, new config items: {}", brokerServers, topicNames, alterConfigs);
                    }

                    // Do batch alter topics config.
                    // If kafka broker >= 2.3.0
                    return adminClient
                            // Alter by incremental configs.
                            .incrementalAlterConfigs(alterConfigs)
                            .all()
                            .whenComplete((unused, ex) -> {
                                if (Objects.isNull(ex)) {
                                    log.info("{} :: Updated to topics: {}", brokerServers, topicNames);
                                    return;
                                }
                                final Throwable reason = ExceptionUtils.getRootCause(ex);
                                // for compatible, if kafka broker < 2.3.0
                                if (reason instanceof UnsupportedVersionException) {
                                    log.warn("{} :: broker unsupported incremental alter, and fallback full alter config items.",
                                            brokerServers);
                                    // Convert to older version full alter config.
                                    final Map<ConfigResource, Config> newConfigMapFull = alterConfigs
                                            .entrySet().stream()
                                            .collect(Collectors.toMap(
                                                    Map.Entry::getKey,
                                                    e -> new Config(safeList(e.getValue()).stream()
                                                            .map(AlterConfigOp::configEntry)
                                                            .collect(Collectors.toList()))));

                                    // Alter by full configs.
                                    adminClient
                                            .alterConfigs(newConfigMapFull)
                                            .all()
                                            .whenComplete((unused2, ex2) -> {
                                                log.info("{} :: Updated to topics: {}", brokerServers, topicNames);
                                                if (Objects.nonNull(ex2)) {
                                                    throw new TopicConfigurationException(String.format("%s :: Failed to full alter topics config of %s",
                                                            brokerServers, updateTopics), ex);
                                                }
                                            });
                                } else {
                                    throw new TopicConfigurationException(String.format("%s :: Failed to incremental alter topics config of %s",
                                            brokerServers, updateTopics), ex);
                                }
                            });
                });
    }

    @Getter
    @AllArgsConstructor
    static class AlterTopicConfig {
        private final String topic;
        private final SubscriberInfo subscriber;
        private final Map<String, String> expectedUpdateItems;
    }

}
