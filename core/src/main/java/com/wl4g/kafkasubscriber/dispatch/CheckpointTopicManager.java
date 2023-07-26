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
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.coordinator.CachingSubscriberRegistry;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
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
    private final KafkaSubscriberProperties config;
    private final CachingSubscriberRegistry registry;

    @Override
    public void run(ApplicationArguments args) {
        addTopicAllIfNecessary();
    }

    public void addTopicAllIfNecessary() {
        log.info("Creating topics if necessary of {} ...", config.getPipelines().size());
        config.getPipelines().forEach(pipeline -> {
            final KafkaSubscriberProperties.CheckpointProperties checkpoint = pipeline.getInternalFilter().getCheckpoint();
            final String brokerServers = checkpoint.getDefaultProducerProps().getProperty(BOOTSTRAP_SERVERS_CONFIG);
            try (AdminClient adminClient = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG,
                    brokerServers))) {
                final String topicPrefix = pipeline.getInternalFilter().getTopicPrefix();
                final int partitions = pipeline.getInternalFilter().getTopicPartitions();
                final short replicationFactor = pipeline.getInternalFilter().getReplicationFactor();

                final List<NewTopic> topics = safeList(registry.getAll()).stream()
                        .map(subscriber -> String.format("%s-%s", topicPrefix, subscriber.getId()))
                        .map(topic -> {
                            final Map<String, String> topicConfigs = checkpoint.getDefaultTopicProps()
                                    .entrySet().stream().collect(toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));

                            topicConfigs.put(RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(1).toMillis()));
                            topicConfigs.put(RETENTION_BYTES_CONFIG, String.valueOf(DataSize.ofGigabytes(1).toBytes()));
                            return new NewTopic(topic, partitions, replicationFactor).configs(topicConfigs);
                        })
                        .collect(toList());

                // TODO if create checkpoint filtered topic failure, how to process??? forever retry???
                // e.g Timed out waiting for a node assignment. Call: listTopics
                doCreateTopicsIfNecessary(adminClient, topics, 6000);
            } catch (Throwable ex) {
                log.error(String.format("Failed to create topics of %s", config.getPipelines().size()), ex);
            }
        });
    }

    public static void doCreateTopicsIfNecessary(AdminClient adminClient,
                                                 List<NewTopic> topics,
                                                 long timeout) throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Creating topics: {}", topics);

        final ListTopicsResult listResult = adminClient.listTopics(new ListTopicsOptions().timeoutMs(TypeConverts.safeLongToInt(timeout)));
        final Set<String> existingTopics = listResult.names().get(timeout, TimeUnit.MILLISECONDS);
        log.info("Skip to create existing topics: {}", existingTopics);

        final List<NewTopic> newTopics = safeList(topics).stream()
                .filter(topic -> !existingTopics.contains(topic.name()))
                .collect(Collectors.toList());

        final CreateTopicsResult createResult = adminClient.createTopics(newTopics);

        createResult.all().get(timeout, TimeUnit.MILLISECONDS);
        log.info("Created new topics: {}", newTopics);
    }

}
