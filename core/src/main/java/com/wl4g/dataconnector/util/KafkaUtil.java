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

package com.wl4g.dataconnector.util;

import com.wl4g.infra.common.lang.Assert2;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.lang.Assert2.notEmptyOf;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.apache.kafka.clients.admin.AdminClientConfig.*;
import static org.springframework.util.ReflectionUtils.findField;
import static org.springframework.util.ReflectionUtils.getField;

/**
 * The {@link KafkaUtil}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@SuppressWarnings("unused")
public abstract class KafkaUtil {

    public static Map<String, String> toHeaderStringMap(Headers headers) {
        return safeMap(toHeaderBytesMap(headers))
                .entrySet()
                .stream()
                .filter(e -> nonNull(e.getValue()))
                .collect(toMap(Entry::getKey, e -> new String(e.getValue(), UTF_8)));
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

    public static CompletableFuture<Void> alterOrCreateTopicsIfNeed(
            @NotNull AdminClient adminClient,
            @NotEmpty List<TopicSpec> topics) throws ExecutionException {
        requireNonNull(adminClient, "adminClient must not be null");
        notEmptyOf(topics, "topics");

        final String clientId = getAdminClientId(adminClient);
        try {
            return adminClient
                    .listTopics() // Listing of all alterTopicConfigs config.
                    .names() // getting topic names.
                    .thenApply(allTopicNames -> {
                        if (log.isDebugEnabled()) {
                            log.debug("{} :: List to existing topics: {}", clientId, allTopicNames);
                        }
                        CompletableFuture<Void> createFuture = new CompletableFuture<>();
                        CompletableFuture<Void> alterFuture;

                        if (log.isDebugEnabled()) {
                            log.debug("{} :: Creating to topics: {}", clientId, topics
                                    .stream()
                                    .map(TopicSpec::getTopicName)
                                    .collect(toList()));
                        }
                        // Should be create topics.
                        final List<TopicSpec> createTopicSpecs = safeList(topics).stream()
                                .filter(topic -> !allTopicNames.contains(topic.getTopicName()))
                                .collect(Collectors.toList());
                        // Should be update topics.
                        final List<TopicSpec> alterTopicSpecs = safeList(topics).stream()
                                .filter(topic -> allTopicNames.contains(topic.getTopicName()))
                                .collect(toList());

                        // Create the topics.
                        final List<NewTopic> newTopics = createTopicSpecs.stream()
                                .map(topic -> {
                                    final NewTopic newTopic = new NewTopic(topic.getTopicName(),
                                            topic.getPartitions(),
                                            topic.getReplicationFactor());
                                    newTopic.configs(safeMap(topic.getConfigEntries()));
                                    return newTopic;
                                })
                                .collect(toList());
                        // If there is no topic that needs to be creation, then complete it directly.
                        if (newTopics.isEmpty()) {
                            createFuture.complete(null);
                        } else {
                            adminClient.createTopics(newTopics).all().whenComplete((unused, ex) -> {
                                if (nonNull(ex)) {
                                    final Throwable reason = getRootCause(ex);
                                    if (reason instanceof TopicExistsException) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("{} :: Skip to create existing topics: {}", clientId, newTopics);
                                        }
                                    } else {
                                        createFuture.completeExceptionally(ex);
                                        if (log.isDebugEnabled()) {
                                            log.debug("{} :: Failed to create topics: {}", clientId, newTopics);
                                        }
                                    }
                                } else {
                                    createFuture.complete(null);
                                    if (log.isDebugEnabled()) {
                                        log.debug("{} :: Created topics: {}", clientId, newTopics);
                                    }
                                }
                            });
                        }

                        // If there is no topic that needs to be alter, then complete it directly
                        if (alterTopicSpecs.isEmpty()) {
                            alterFuture = CompletableFuture.completedFuture(null);
                        } else {
                            alterFuture = alterTopicsIfNeed(adminClient, alterTopicSpecs);
                        }

                        return CompletableFuture.allOf(createFuture, alterFuture);
                    }).get();
        } catch (InterruptedException ex) {
            throw new ExecutionException(ex);
        }
    }

    @SuppressWarnings("deprecation")
    public static CompletableFuture<Void> alterTopicsIfNeed(
            @NotNull AdminClient adminClient,
            @NotEmpty List<TopicSpec> alterTopics) {
        requireNonNull(adminClient, "adminClient must not be null");
        notEmptyOf(alterTopics, "alterTopics");

        final String clientId = getAdminClientId(adminClient);

        // Convert to topic -> config entries.
        final Map<String, TopicSpec> alterTopicMap = safeList(alterTopics)
                .stream()
                .collect(toMap(TopicSpec::getTopicName, e -> e));

        final Set<String> topicNames = alterTopicMap.keySet();
        if (log.isDebugEnabled()) {
            log.debug("{} :: Altering to topics: {}", clientId, topicNames);
        }

        // Getting existing all topics config.
        final List<ConfigResource> topicsConfigResources = safeList(alterTopics)
                .stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic.getTopicName()))
                .collect(toList());

        final CompletableFuture<Map<ConfigResource, Config>> describeTopicsConfigsFuture = new CompletableFuture<>();
        adminClient.describeConfigs(topicsConfigResources).all().whenComplete((oldTopicsConfigMap, ex) -> {
            if (nonNull(ex)) {
                describeTopicsConfigsFuture.completeExceptionally(ex);
            } else {
                describeTopicsConfigsFuture.complete(oldTopicsConfigMap);
            }
        });

        return describeTopicsConfigsFuture.thenCompose(existingTopicsConfigMap -> {
            if (log.isDebugEnabled()) {
                log.debug("{} :: topics: {}, existing config items: {}", clientId, topicNames, existingTopicsConfigMap);
            }
            // Wrap per topic update alter config OP. (if necessary)
            final Map<ConfigResource, Collection<AlterConfigOp>> incrementTopicsAlterConfigs = safeMap(existingTopicsConfigMap)
                    .entrySet()
                    .stream()
                    .filter(existingTopicConfig -> topicNames.contains(existingTopicConfig.getKey().name()))
                    .collect(toMap(Map.Entry::getKey,
                            existingTopicConfig -> {
                                final Map<String, String> alterTopicConfigEntries = safeMap(alterTopicMap
                                        .get(existingTopicConfig.getKey().name())
                                        .getConfigEntries());
                                return safeList(existingTopicConfig.getValue().entries())
                                        .stream()
                                        .map(configEntry -> {
                                            final String alterValue = alterTopicConfigEntries.get(configEntry.name());
                                            return !isBlank(alterValue) ? new ConfigEntry(configEntry.name(), alterValue) : null;
                                        })
                                        .filter(Objects::nonNull)
                                        .map(entry -> new AlterConfigOp(entry, OpType.SET))
                                        .collect(toList());
                            }
                    ));
            if (log.isDebugEnabled()) {
                log.debug("{} :: Altering for topics: {}, config items: {}", clientId, topicNames, incrementTopicsAlterConfigs);
            }

            final CompletableFuture<Void> alterConfigsFuture = new CompletableFuture<>();
            adminClient.incrementalAlterConfigs(incrementTopicsAlterConfigs).all().whenComplete((unused, ex) -> {
                if (isNull(ex)) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: Updated with incremental to topics: {}", clientId, topicNames);
                    }
                    alterConfigsFuture.complete(null);
                    return;
                }
                final Throwable reason = getRootCause(ex);
                // for compatible, if kafka broker < 2.3.0
                if (reason instanceof UnsupportedVersionException) {
                    log.warn("{} :: broker unsupported incremental alter, and fallback full alter config items.",
                            clientId);
                    // Convert to older version full alter config.
                    final Map<ConfigResource, Config> fullAlterTopicsConfigs = existingTopicsConfigMap.entrySet()
                            .stream()
                            .collect(toMap(Entry::getKey,
                                    e -> {
                                        final Set<ConfigEntry> mergeEntries = new HashSet<>(e.getValue().entries());
                                        final TopicSpec alterTopicSpec = alterTopicMap.get(e.getKey().name());
                                        if (nonNull(alterTopicSpec)) {
                                            safeMap(alterTopicSpec.getConfigEntries())
                                                    .forEach((name, value) -> mergeEntries.add(new ConfigEntry(name, value)));
                                        }
                                        return new Config(mergeEntries);
                                    }));

                    // Alter by full configs.
                    adminClient.alterConfigs(fullAlterTopicsConfigs)
                            .all()
                            .whenComplete((unused2, ex2) -> {
                                if (isNull(ex2)) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("{} :: Updated with full to topics: {}", clientId, topicNames);
                                    }
                                    alterConfigsFuture.complete(null);
                                } else {
                                    alterConfigsFuture.completeExceptionally(new IllegalStateException(String.format("%s :: Failed to full alter topics config of %s",
                                            clientId, alterTopics), ex));
                                }
                            });
                } else {
                    alterConfigsFuture.completeExceptionally(new IllegalStateException(String.format("%s :: Failed to full alter topics config of %s",
                            clientId, alterTopics), ex));
                }
            });

            return alterConfigsFuture;
        });
    }

    private static String getAdminClientId(@NotNull AdminClient adminClient) {
        requireNonNull(adminClient, "adminClient must not be null");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) adminClient;
        final Field field = findField(kafkaAdminClient.getClass(), "clientId", String.class);
        assert field != null : String.format("Could not find field clientId of %s", KafkaAdminClient.class.getName());
        field.setAccessible(true);
        return (String) getField(field, kafkaAdminClient);
    }

    @Getter
    public static class TopicSpec {
        private final @NotBlank String topicName;
        private final @Min(1) int partitions;
        private final @Min(1) short replicationFactor;
        private final @Nullable Map<String, String> configEntries;

        public TopicSpec(String topicName,
                         int partitions,
                         short replicationFactor,
                         @Nullable Map<String, String> configEntries) {
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