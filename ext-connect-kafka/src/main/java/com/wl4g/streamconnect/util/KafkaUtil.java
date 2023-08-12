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

package com.wl4g.streamconnect.util;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.exception.StreamConnectException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
import static org.springframework.util.ReflectionUtils.findField;
import static org.springframework.util.ReflectionUtils.getField;

/**
 * The {@link KafkaUtil}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public abstract class KafkaUtil {

    public static Map<String, String> toHeaderStringMap(Headers headers) {
        return safeMap(toHeaderBytesMap(headers))
                .entrySet()
                .stream()
                .collect(toMap(Entry::getKey,
                        e -> nonNull(e.getValue()) ? new String(e.getValue(), UTF_8) : null));
    }

    public static Map<String, byte[]> toHeaderBytesMap(Headers headers) {
        final Map<String, byte[]> _headers = new HashMap<>();
        for (Header header : headers) {
            if (isNotBlank(header.key())) {
                _headers.put(header.key(), header.value());
            }
        }
        return _headers;
    }

    public static Boolean getFirstValueAsBoolean(Headers headers, String key) {
        return parseBoolean(getFirstValueAsString(headers, key));
    }

    public static String getFirstValueAsString(Headers headers, String key) {
        final byte[] value = getFirstValue(headers, key);
        return nonNull(value) ? new String(value, UTF_8) : null;
    }

    public static byte[] getFirstValue(Headers headers, String key) {
        if (Objects.nonNull(headers)) {
            final Iterator<Header> it = headers.headers(key).iterator();
            if (it.hasNext()) {
                Header first = it.next(); // first only
                if (Objects.nonNull(first)) {
                    return first.value();
                }
            }
        }
        return null;
    }

    public static AdminClient createAdminClient(@NotNull Map<String, Object> config) {
        requireNonNull(config, "config must not be null");

        final Map<String, Object> mergedConfig = new HashMap<>();
        mergedConfig.put(CLIENT_ID_CONFIG, "bootstrapServers-".concat((String) config.get(BOOTSTRAP_SERVERS_CONFIG)));
        mergedConfig.put(DEFAULT_API_TIMEOUT_MS_CONFIG, 60_000);
        mergedConfig.put(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10_000);
        mergedConfig.putAll(config);

        return AdminClient.create(mergedConfig);
    }

    public static Collection<MemberDescription> getGroupConsumers(
            @NotBlank String bootstrapServers,
            @NotBlank String groupId,
            long timeout)
            throws ExecutionException, InterruptedException, TimeoutException {
        Assert2.hasTextOf(bootstrapServers, "bootstrapServers");
        Assert2.hasTextOf(groupId, "groupId");

        final Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = createAdminClient(config)) {
            return getGroupConsumers(adminClient, groupId, timeout);
        }
    }

    public static Collection<MemberDescription> getGroupConsumers(
            @NotNull AdminClient adminClient,
            @NotBlank String groupId,
            long timeout)
            throws ExecutionException, InterruptedException, TimeoutException {
        Assert2.notNullOf(adminClient, "adminClient");
        Assert2.hasTextOf(groupId, "groupId");

        DescribeConsumerGroupsOptions describeOptions = new DescribeConsumerGroupsOptions()
                .includeAuthorizedOperations(false);

        DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
                Collections.singletonList(groupId), describeOptions);

        Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap =
                describeResult.all().get(timeout, TimeUnit.MILLISECONDS);

        if (consumerGroupDescriptionMap.containsKey(groupId)) {
            ConsumerGroupDescription consumerGroupDescription =
                    consumerGroupDescriptionMap.get(groupId);
            return consumerGroupDescription.members();
        }

        return Collections.emptyList();
    }


    public static KafkaFuture<Object> createOrAlterTopicsIfNecessary(
            @NotNull AdminClient adminClient,
            @NotEmpty List<TopicDesc> topics) {
        requireNonNull(adminClient, "adminClient must not be null");
        Assert2.notEmptyOf(topics, "topics");

        final String clientId = getAdminClientClientId(adminClient);
        return adminClient
                .listTopics() // Listing of all alterTopicConfigs config.
                .names() // getting topic names.
                .thenApply(allTopicNames -> {
                    if (log.isInfoEnabled()) {
                        log.info("{} :: List to existing topics: {}", clientId, allTopicNames);
                    }
                    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
                    KafkaFuture<Object> updateFuture = KafkaFuture.completedFuture(null);

                    // Create new alterTopicConfigs.
                    if (log.isInfoEnabled()) {
                        log.info("{} :: Creating to topics: {}", clientId, topics
                                .stream()
                                .map(TopicDesc::getTopicName)
                                .collect(toList()));
                    }
                    final List<TopicDesc> newTopics = safeList(topics).stream()
                            .filter(topic -> !allTopicNames.contains(topic.getTopicName()))
                            .collect(Collectors.toList());

                    if (!newTopics.isEmpty()) {
                        final List<NewTopic> _newTopics = newTopics.stream()
                                .map(topic -> {
                                    final NewTopic newTopic = new NewTopic(topic.getTopicName(),
                                            topic.getPartitions(),
                                            topic.getReplicationFactor());
                                    newTopic.configs(safeMap(topic.getConfigEntries()));
                                    return newTopic;
                                })
                                .collect(toList());

                        createFuture = adminClient.createTopics(_newTopics).all();
                        if (log.isInfoEnabled()) {
                            log.info("{} :: Created new alterTopicConfigs: {}", clientId, newTopics);
                        }
                    } else {
                        // Update new alterTopicConfigs name.
                        final List<TopicDesc> updateTopics = safeList(topics).stream()
                                .filter(topic -> allTopicNames.contains(topic.getTopicName()))
                                .collect(toList());
                        try {
                            updateFuture = alterTopicsIfNecessary(adminClient, updateTopics);
                        } catch (Throwable ex) {
                            throw new StreamConnectException(ex);
                        }
                    }
                    return KafkaFuture.allOf(createFuture, updateFuture);
                });
    }

    @SuppressWarnings("deprecation")
    public static KafkaFuture<Object> alterTopicsIfNecessary(
            @NotNull AdminClient adminClient,
            @NotEmpty List<TopicDesc> topics) {
        requireNonNull(adminClient, "adminClient must not be null");
        Assert2.notEmptyOf(topics, "topics");

        final String clientId = getAdminClientClientId(adminClient);

        // Convert to topic->channel map.
        final Map<String, TopicDesc> updateTopicMap = safeList(topics)
                .stream()
                .collect(Collectors.toMap(TopicDesc::getTopicName, e -> e));

        final Set<String> topicNames = updateTopicMap.keySet();
        if (log.isInfoEnabled()) {
            log.info("{} :: Updating to topics: {}", clientId, topicNames);
        }

        // Getting existing topics config.
        final List<ConfigResource> topicConfigResources = safeList(topics)
                .stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic.getTopicName()))
                .collect(Collectors.toList());

        return adminClient
                .describeConfigs(topicConfigResources)
                .all()
                .thenApply(existingConfigMap -> {
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: topics: {}, existing config items: {}", clientId, topicNames, existingConfigMap);
                    }

                    // Wrap per topic update alter config OP. (If necessary)
                    final Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = safeMap(existingConfigMap)
                            .entrySet()
                            .stream()
                            .filter(existing -> {
                                // If any config item is not the expected value, it needs to be update, otherwise not need update.
                                final Map<String, String> expectedUpdateItems = updateTopicMap.get(existing.getKey().name())
                                        .getConfigEntries();
                                return !expectedUpdateItems.entrySet().stream().allMatch(e1 ->
                                        existing.getValue().entries().stream().anyMatch(e2 -> StringUtils.equals(e1.getKey(), e2.name())
                                                && StringUtils.equals(e1.getValue(), e2.value())));
                            })
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    existing -> {
                                        final Map<String, String> expectedUpdateItems = updateTopicMap.get(existing.getKey().name())
                                                .getConfigEntries();
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
                        log.debug("{} :: topics: {}, new config items: {}", clientId, topicNames, alterConfigs);
                    }

                    // Do batch alter topics config.
                    // If kafka broker >= 2.3.0
                    return adminClient
                            // Alter by incremental configs.
                            .incrementalAlterConfigs(alterConfigs)
                            .all()
                            .whenComplete((unused, ex) -> {
                                if (Objects.isNull(ex)) {
                                    if (log.isInfoEnabled()) {
                                        log.info("{} :: Updated to topics: {}", clientId, topicNames);
                                    }
                                    return;
                                }
                                final Throwable reason = ExceptionUtils.getRootCause(ex);
                                // for compatible, if kafka broker < 2.3.0
                                if (reason instanceof UnsupportedVersionException) {
                                    log.warn("{} :: broker unsupported incremental alter, and fallback full alter config items.",
                                            clientId);
                                    // Convert to older version full alter config.
                                    final Map<ConfigResource, Config> newConfigMapFull = alterConfigs
                                            .entrySet().stream()
                                            .collect(Collectors.toMap(
                                                    Map.Entry::getKey,
                                                    e -> new Config(safeList(e.getValue()).stream()
                                                            .map(AlterConfigOp::configEntry)
                                                            .collect(Collectors.toList()))));

                                    // Alter by full configs.
                                    adminClient.alterConfigs(newConfigMapFull)
                                            .all()
                                            .whenComplete((unused2, ex2) -> {
                                                if (log.isInfoEnabled()) {
                                                    log.info("{} :: Updated to topics: {}", clientId, topicNames);
                                                }
                                                if (Objects.nonNull(ex2)) {
                                                    throw new StreamConnectException(String.format("%s :: Failed to full alter topics config of %s",
                                                            clientId, topics), ex);
                                                }
                                            });
                                } else {
                                    throw new StreamConnectException(String.format("%s :: Failed to incremental alter topics config of %s",
                                            clientId, topics), ex);
                                }
                            });
                });
    }

    private static String getAdminClientClientId(@NotNull AdminClient adminClient) {
        requireNonNull(adminClient, "adminClient must not be null");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) adminClient;
        final Field field = findField(kafkaAdminClient.getClass(), "clientId", String.class);
        assert field != null : String.format("Could not find field clientId of %s", KafkaAdminClient.class.getName());
        field.setAccessible(true);
        return (String) getField(field, kafkaAdminClient);
    }

    @Getter
    public static class TopicDesc {
        private final @NotBlank String topicName;
        private final @Min(1) int partitions;
        private final @Min(1) short replicationFactor;
        private final @Null Map<String, String> configEntries;

        public TopicDesc(String topicName,
                         int partitions,
                         short replicationFactor,
                         Map<String, String> configEntries) {
            Assert2.hasTextOf(topicName, "topicName");
            Assert2.isTrueOf(1 <= partitions, "partitions >= 1");
            Assert2.isTrueOf(1 <= replicationFactor, "replicationFactor >= 1");
            this.topicName = topicName;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.configEntries = configEntries;
        }
    }

}
