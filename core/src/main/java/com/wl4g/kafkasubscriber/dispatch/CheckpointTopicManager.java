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

package com.wl4g.kafkasubscriber.dispatch;

import com.wl4g.infra.common.lang.TypeConverts;
import com.wl4g.infra.common.lang.tuples.Tuple2;
import com.wl4g.kafkasubscriber.config.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.CheckpointConfig;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeEnginePipelineConfig;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import com.wl4g.kafkasubscriber.custom.SubscribeEngineCustomizer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
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
public class CheckpointTopicManager implements ApplicationRunner {
    private final KafkaSubscribeConfiguration config;
    private final SubscribeEngineCustomizer customizer;
    private final CachingSubscriberRegistry registry;

    @Override
    public void run(ApplicationArguments args) {
        // TODO when the add subscriber should to be dynamic add new checkpoint topic and config
        addTopicAllIfNecessary();
    }

    public void addTopicAllIfNecessary() {
        log.info("Creating topics if necessary of {} ...", config.getPipelines().size());
        config.getPipelines().forEach(pipeline -> {
            final CheckpointConfig checkpoint = pipeline.getParsedFilter().getFilterConfig().getCheckpoint();
            // TODO may per subscriber a consumer properties(brokers)
            final String brokerServers = checkpoint.getProducerProps().getProperty(BOOTSTRAP_SERVERS_CONFIG);
            try (AdminClient adminClient = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG,
                    brokerServers))) {
                doCreateOrUpdateTopicsIfNecessary(adminClient, pipeline, customizer, registry.getShardingAll(), 6000);
            } catch (Throwable ex) {
                log.error(String.format("Failed to create topics of %s", config.getPipelines().size()), ex);
            }
        });
    }

    public static void doCreateOrUpdateTopicsIfNecessary(AdminClient adminClient,
                                                         SubscribeEnginePipelineConfig pipeline,
                                                         SubscribeEngineCustomizer customizer,
                                                         Collection<SubscriberInfo> subscribers,
                                                         long timeout) throws ExecutionException, InterruptedException, TimeoutException {
        final String topicPrefix = pipeline.getParsedFilter().getFilterConfig().getTopicPrefix();
        final int partitions = pipeline.getParsedFilter().getFilterConfig().getTopicPartitions();
        final short replicationFactor = pipeline.getParsedFilter().getFilterConfig().getReplicationFactor();

        final List<Tuple2> topics = safeList(subscribers).stream()
                .map(subscriber -> new Tuple2(customizer.generateCheckpointTopic(pipeline.getName(),
                        topicPrefix, subscriber.getId()), subscriber.getSettings()))
                .collect(toList());

        // Listing of all topics.
        final ListTopicsResult allTopicsResult = adminClient.listTopics(new ListTopicsOptions()
                .timeoutMs(TypeConverts.safeLongToInt(timeout)));
        final Set<String> allTopicNames = allTopicsResult.names().get(timeout, TimeUnit.MILLISECONDS);
        log.info("Listing to existing topics: {}", allTopicNames);

        // Create new topics.
        final List<Tuple2> newTopics = safeList(topics).stream()
                .filter(topic -> !allTopicNames.contains((String) topic.getItem1()))
                .collect(Collectors.toList());
        if (!newTopics.isEmpty()) {
            log.info("Creating to topics: {}", topics);
            final List<NewTopic> _newTopics = newTopics.stream()
                    .map(topic -> {
                        final SubscriberInfo.SubscribeSettings settings = (SubscriberInfo.SubscribeSettings) topic.getItem2();
                        final NewTopic newTopic = new NewTopic(topic.getItem1(), partitions, replicationFactor);
                        newTopic.configs(singletonMap(RETENTION_MS_CONFIG, String.valueOf(settings.getLogRetentionTime().toMillis())));
                        newTopic.configs(singletonMap(RETENTION_BYTES_CONFIG, String.valueOf(settings.getLogRetentionBytes().toBytes())));
                        return newTopic;
                    })
                    .collect(toList());
            final CreateTopicsResult createResult = adminClient.createTopics(_newTopics);
            createResult.all().get(timeout, TimeUnit.MILLISECONDS);
            log.info("Created new topics: {}", newTopics);
        }

        // Update new topics name.
        final List<Tuple2> updateTopics = safeList(topics).stream()
                .filter(topic -> allTopicNames.contains((String) topic.getItem1()))
                .collect(toList());
        doUpdateTopicsIfNecessary(adminClient, updateTopics, timeout);
    }

    public static void doUpdateTopicsIfNecessary(AdminClient adminClient,
                                                 List<Tuple2> topics,
                                                 long timeout) throws ExecutionException, InterruptedException, TimeoutException {
        final Map<String, SubscriberInfo.SubscribeSettings> topicMap = topics.stream().collect(Collectors
                .toMap(topic -> (String) topic.getItem1(), topic -> (SubscriberInfo.SubscribeSettings) topic.getItem2()));
        final Set<String> topicNames = topicMap.keySet();
        log.info("Updating to topics: {}", topicNames);

        // Describe topics config.
        final List<ConfigResource> topicConfigResources = safeList(topics).stream().map(topic ->
                new ConfigResource(ConfigResource.Type.TOPIC, topic.getItem1())).collect(Collectors.toList());
        final Map<ConfigResource, Config> existingConfigMap = adminClient.describeConfigs(topicConfigResources)
                .all().get(timeout, TimeUnit.MILLISECONDS);

        // Update alter topics config.
        final Map<ConfigResource, Collection<AlterConfigOp>> newConfigMap = safeMap(existingConfigMap)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            final SubscriberInfo.SubscribeSettings settings = topicMap.get(e.getKey().name());
                            final List<ConfigEntry> entries = new ArrayList<>(e.getValue().entries());
                            // TODO add support more topic config props.
                            entries.add(new ConfigEntry(RETENTION_MS_CONFIG, String.valueOf(settings.getLogRetentionTime().toMillis())));
                            entries.add(new ConfigEntry(RETENTION_BYTES_CONFIG, String.valueOf(settings.getLogRetentionBytes().toBytes())));
                            return entries.stream().map(entry -> new AlterConfigOp(entry, AlterConfigOp.OpType.SET)).collect(toList());
                        }
                ));

        // Alter topics config.
        try {
            AlterConfigsResult alterResult = null;
            try {
                // If kafka broker >= 2.3.0
                alterResult = adminClient.incrementalAlterConfigs(newConfigMap);
                alterResult.all().get(timeout, TimeUnit.MILLISECONDS);
            } catch (ExecutionException ex) {
                final Throwable reason = ExceptionUtils.getRootCause(ex);
                if (reason instanceof UnsupportedVersionException) { // If kafka broker < 2.3.0
                    // Convert to older version full alter config.
                    final Map<ConfigResource, Config> newConfigMapFull = newConfigMap
                            .entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> new Config(safeList(e.getValue()).stream()
                                            .map(AlterConfigOp::configEntry)
                                            .collect(Collectors.toList()))));

                    alterResult = adminClient.alterConfigs(newConfigMapFull);
                    alterResult.all().get(timeout, TimeUnit.MILLISECONDS);
                } else {
                    throw ex;
                }
            }
            log.info("Updated to topics: {}", topicNames);
        } catch (Throwable ex) {
            log.error(String.format("Failed to update topics config of %s", topics), ex);
        }

    }

}
