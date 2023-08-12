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

package com.wl4g.streamconnect.checkpoint.kafka;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.checkpoint.ICheckpoint.CheckpointConfig;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.ChannelInfo.CheckpointSpec;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.util.KafkaUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.TopicConfig;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.streamconnect.util.KafkaUtil.TopicDesc;
import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * The {@link KafkaTopicHelper}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@AllArgsConstructor
public class KafkaTopicHelper {
    private final StreamConnectConfiguration config;
    private final CachingChannelRegistry registry;

    /**
     * Create or update the all connectors topics by the channels (if necessary)
     */
    public void initConnectorsTopicIfNecessary() throws StreamConnectException {
        safeMap(config.getConnectorMap()).values().forEach(connectorConfig -> {
            try {
                if (log.isInfoEnabled()) {
                    log.info("Initializing topics if necessary of connector: {} ...",
                            connectorConfig.getName());
                }
                final CheckpointConfig checkpointConfig = connectorConfig
                        .getCheckpoint().getCheckpointConfig();
                if (checkpointConfig instanceof KafkaCheckpoint.KafkaCheckpointConfig) {
                    initChannelsTopicIfNecessary(connectorConfig,
                            (KafkaCheckpoint.KafkaCheckpointConfig) checkpointConfig,
                            registry.getAssignedChannels(connectorConfig.getName()));
                }
            } catch (Throwable ex) {
                throw new StreamConnectException(String.format("Could not create topics of connector : %s",
                        connectorConfig.getName()), ex);
            }
        });
    }

    /**
     * Create or update the topic by channels (if necessary)
     *
     * @param connectorConfig connector config.
     * @param channels        channels(subscribers) information.
     */
    public void initChannelsTopicIfNecessary(@NotNull ConnectorConfig connectorConfig,
                                             @NotNull KafkaCheckpoint.KafkaCheckpointConfig checkpointConfig,
                                             @NotEmpty Collection<ChannelInfo> channels) {
        requireNonNull(connectorConfig, "connectorConfig");
        requireNonNull(checkpointConfig, "checkpointConfig");
        Assert2.notEmptyOf(channels, "channels");

        if (log.isInfoEnabled()) {
            log.info("{} :: Creating topics if necessary of {} ...",
                    connectorConfig.getName(), config.getConnectorMap().size());
        }
        for (ChannelInfo channel : safeList(channels)) {
            final String checkpointServers = KafkaCheckpoint.getCheckpointBootstrapServers(
                    checkpointConfig, channel);

            final Map<String, String> props = new HashMap<>();
            final CheckpointSpec ckpSpec = channel.getSettingsSpec().getCheckpointSpec();
            props.put(TopicConfig.RETENTION_BYTES_CONFIG, valueOf(ckpSpec.getRetentionBytes()));
            props.put(TopicConfig.RETENTION_MS_CONFIG, valueOf(ckpSpec.getRetentionTime()));

            final TopicDesc topic = new TopicDesc(checkpointConfig.generateDlqTopic(channel.getId()),
                    checkpointConfig.getTopicPartitions(),
                    checkpointConfig.getReplicationFactor(),
                    props);

            try (AdminClient adminClient = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG,
                    checkpointServers))) {
                KafkaUtil.createOrAlterTopicsIfNecessary(adminClient, singletonList(topic))
                        .get(checkpointConfig.getInitTopicTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException | InterruptedException | TimeoutException ex) {
                throw new StreamConnectException(ex);
            }
        }
    }

}
