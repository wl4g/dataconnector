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

package com.wl4g.streamconnect.coordinator;

import com.wl4g.infra.common.lang.SystemUtils2;
import com.wl4g.streamconnect.config.StreamConnectProperties;
import com.wl4g.streamconnect.coordinator.StreamConnectEventPublisher.SubscribeEvent;
import com.wl4g.streamconnect.custom.StreamConnectEngineCustomizer;
import com.wl4g.streamconnect.util.KafkaUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.serialize.JacksonUtils.parseJSON;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomUtils.nextInt;

/**
 * The {@link KafkaStreamConnectCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class KafkaStreamConnectCoordinator extends AbstractStreamConnectCoordinator {

    public static final String SUBSCRIBE_COORDINATOR_DISCOVERY_CLIENT_ID = String.format(
            "subscribe-coordinator-discovery-clientId-%s", nextInt(1_000_000, 99_999_999)); // duplicate rate: 0.01%

    private KafkaConsumer<String, String> configConsumer;
    private KafkaConsumer<String, String> discoveryConsumer;
    private AdminClient discoveryAdminClient;

    public KafkaStreamConnectCoordinator(Environment environment,
                                         StreamConnectProperties config,
                                         StreamConnectEngineCustomizer customizer,
                                         CachingSubscriberRegistry registry) {
        super(environment, config, customizer, registry);
    }

    @Override
    protected void init() {
        initCoordinatorConfig();
        initCoordinatorDiscovery();
    }

    protected void initCoordinatorConfig() {
        log.info("Initializing subscribe coordinator config consumer ...");
        final KafkaCoordinatorBusConfig configConfig = config.getCoordinator().getConfigConfig();

        final Properties props = new Properties();
        props.putAll(configConfig.getConsumerProps());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getCoordinator().getBootstrapServers());
        // The one groupId per consumer.(broadcast)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, generateBroadcastGroupId());
        this.configConsumer = new KafkaConsumer<>(props);
        this.configConsumer.assign(singleton(new TopicPartition(configConfig.getTopic(), 0)));

        // TODO custom re-balancing listener?
        this.configConsumer.subscribe(singleton(configConfig.getTopic()), new NoOpConsumerRebalanceListener());
    }

    protected void initCoordinatorDiscovery() {
        log.info("Initializing subscribe coordinator discovery consumer ...");
        final KafkaCoordinatorBusConfig configConfig = config.getCoordinator().getConfigConfig();
        final KafkaCoordinatorDiscoveryConfig discoveryConfig = config.getCoordinator().getDiscoveryConfig();

        final Properties props = new Properties();
        props.putAll(discoveryConfig.getConsumerProps());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getCoordinator().getBootstrapServers());
        this.discoveryConsumer = new KafkaConsumer<>(props);
        this.discoveryConsumer.assign(singleton(new TopicPartition(configConfig.getTopic(), 0)));

        this.discoveryConsumer.subscribe(singleton(configConfig.getTopic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                doReBalanceForDiscovery();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                doReBalanceForDiscovery();
            }
        });

        log.info("Initializing subscribe coordinator admin client ...");

        final Properties adminProps = new Properties();
        adminProps.putAll(discoveryConfig.getAdminProps());
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getCoordinator().getBootstrapServers());
        this.discoveryAdminClient = KafkaUtil.createAdminClient(adminProps);
    }

    private String generateBroadcastGroupId() {
        return String.format("subscribe-coordinator-config-%s-%s",
                SystemUtils2.getHostName(), environment.getRequiredProperty("server.port"));
    }

    @Override
    public void doRun() {
        while (getRunning().get()) {
            final ConsumerRecords<String, String> records = configConsumer.poll(Duration.ofMillis(800L));
            log.debug("Received subscribe event records: {}", records);

            records.forEach(record -> super.onBusEvent(parseJSON(record.value(), SubscribeEvent.class)));

            // TODO using async or callback?
            this.configConsumer.commitSync();
        }
    }

    private void doReBalanceForDiscovery() {
        try {
            final KafkaCoordinatorDiscoveryConfig discoveryConfig = config.getCoordinator().getDiscoveryConfig();

            final List<MemberDescription> members = safeList(KafkaUtil.getGroupConsumers(discoveryAdminClient,
                    discoveryConfig.getGroupId(), 6000L)); // TODO timeout

            final List<ServiceInstance> instances = members
                    .stream()
                    .map(member -> ServiceInstance.builder()
                            .instanceId(member.clientId())
                            .selfInstance(StringUtils.equals(member.clientId(), SUBSCRIBE_COORDINATOR_DISCOVERY_CLIENT_ID))
                            .host(member.host()).build())
                    .collect(toList());

            onDiscovery(instances);
        } catch (Throwable th) {
            log.error("Failed to do on re-balancing discovery", th);
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaCoordinatorBusConfig {

        public static final String DEFAULT_CONFIG_PRODUCER_TX_ID = "subscribe_coordinator_config_tx_id";

        private @Builder.Default String topic = "subscribe_coordinator_config_topic";

        private @Builder.Default Properties producerProps = new Properties() {
            {
                //setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                //setProperty(ProducerConfig.CLIENT_ID_CONFIG, "");
                setProperty(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "10000");
                setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
                setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "128");
                setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // snappy|gzip|lz4|zstd|none
                setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(16 * 1024));
                setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(32 * 1024 * 1024));
                setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
                setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                setProperty(ProducerConfig.ACKS_CONFIG, "all");
                setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
                setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, DEFAULT_CONFIG_PRODUCER_TX_ID);
                setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000");
                setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            }
        };

        private @Builder.Default Properties consumerProps = new Properties() {
            {
                //setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                setProperty(ConsumerConfig.CLIENT_ID_CONFIG, SUBSCRIBE_COORDINATOR_DISCOVERY_CLIENT_ID);
                setProperty(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "10000");
                setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
                setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, "128");
                setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
                setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                // 显示指定 Range 分配器确保同一时刻一个 partition 只分配给一个 consumer
                setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
                // This consumer is used for consumption dynamic configuration similar to spring cloud config bus kafka,
                // so read isolation should be used to ensure consistency.
                setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());
            }
        };
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaCoordinatorDiscoveryConfig {
        private @Builder.Default String groupId = "subscribe_coordinator_discovery_group";

        private @Builder.Default Properties consumerProps = new Properties() {
            {
                //setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                setProperty(ConsumerConfig.CLIENT_ID_CONFIG, SUBSCRIBE_COORDINATOR_DISCOVERY_CLIENT_ID);
                setProperty(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "10000");
                setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
                setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, "128");
                setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
                setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                // 显示指定 Range 分配器确保同一时刻一个 partition 只分配给一个 consumer
                setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
                // This consumer is used for consumption dynamic configuration similar to spring cloud config bus kafka,
                // so read isolation should be used to ensure consistency.
                setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());
            }
        };

        private @Builder.Default Properties adminProps = new Properties() {
            {
                //setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
                setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
                setProperty(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "10000");
            }
        };
    }

}
