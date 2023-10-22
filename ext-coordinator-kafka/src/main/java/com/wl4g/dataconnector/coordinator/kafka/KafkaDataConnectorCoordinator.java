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

package com.wl4g.dataconnector.coordinator.kafka;

import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.config.configurator.IDataConnectorConfigurator;
import com.wl4g.dataconnector.coordinator.AbstractDataConnectorCoordinator;
import com.wl4g.dataconnector.coordinator.CachingChannelRegistry;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import com.wl4g.dataconnector.util.Crc32Util;
import com.wl4g.dataconnector.util.KafkaUtil;
import com.wl4g.infra.common.lang.Assert2;
import lombok.Builder.Default;
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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.lang.SystemUtils2.LOCAL_PROCESS_ID;
import static com.wl4g.infra.common.serialize.JacksonUtils.parseJSON;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getenv;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * The {@link KafkaDataConnectorCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class KafkaDataConnectorCoordinator extends AbstractDataConnectorCoordinator {

    private final KafkaCoordinatorConfig coordinatorConfig;
    private final String localDiscoveryClientId;
    private final KafkaBusPublisher busPublisher;

    private final CountDownLatch readyLatch = new CountDownLatch(COORDINATOR_READY_LATCH_COUNT);
    private KafkaConsumer<String, String> busConsumer;
    private KafkaConsumer<String, String> discoveryConsumer;
    private AdminClient discoveryAdminClient;

    protected KafkaDataConnectorCoordinator(@NotNull Environment environment,
                                            @NotNull DataConnectorConfiguration config,
                                            @NotNull IDataConnectorConfigurator configurator,
                                            @NotNull CachingChannelRegistry registry,
                                            @NotNull DataConnectorMeter meter,
                                            @NotNull KafkaCoordinatorConfig coordinatorConfig) {
        super(environment, config, configurator, registry, meter);
        this.coordinatorConfig = requireNonNull(coordinatorConfig, "coordinatorConfig must not be null");
        this.busPublisher = new KafkaBusPublisher(config);
        this.localDiscoveryClientId = generateLocalDiscoveryClientId();
    }

    protected String generateLocalDiscoveryClientId() {
        try {
            final String localHostName = InetAddress.getLocalHost().getHostName();
            return String.format("coordinator-%s-%s", localHostName,
                    Crc32Util.compute(localHostName
                            .concat(LOCAL_PROCESS_ID)
                            .concat(valueOf(currentTimeMillis()))));
        } catch (Exception ex) {
            throw new DataConnectorException("Failed to build kafka discovery local clientId.", ex);
        }
    }

    protected String generateBroadcastGroupId() {
        try {
            final String localHostName = InetAddress.getLocalHost().getHostName();
            return String.format(COORDINATOR_BUS_GROUP_ID_TPL,
                    localHostName, getEnvironment().getRequiredProperty("server.port"));
        } catch (Exception ex) {
            throw new DataConnectorException("Failed to generate broadcast group id.", ex);
        }
    }

    protected void initCoordinatorBus() {
        if (log.isInfoEnabled()) {
            log.info("Initializing coordinator config consumer on '{}' ...",
                    coordinatorConfig.getBootstrapServers());
        }
        final KafkaBusConfig busConfig = coordinatorConfig.getBusConfig();

        // Create config bus topic(if necessary).
        final Map<String, Object> aConfig = new HashMap<>();
        aConfig.putAll(busConfig.getAdminProps());
        aConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, coordinatorConfig.getBootstrapServers());
        try (AdminClient adminClient = KafkaUtil.createAdminClient(aConfig)) {
            adminClient.createTopics(singletonList(new NewTopic(busConfig.getTopic(),
                    1, (short) 1))).all().get();
            if (log.isInfoEnabled()) {
                log.info("Created config bus topic '{}' on '{}'", busConfig.getTopic(),
                        coordinatorConfig.getBootstrapServers());
            }
        } catch (Exception ex) {
            throw new DataConnectorException(String.format("Failed to create config bus topic '%s' on '%s'",
                    busConfig.getTopic(), coordinatorConfig.getBootstrapServers()), ex);
        }

        // Startup config bus consumer.
        final Map<String, Object> cConfig = new HashMap<>();
        cConfig.putAll(busConfig.getConsumerProps());
        cConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, localDiscoveryClientId);
        cConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, coordinatorConfig.getBootstrapServers());
        cConfig.put(ConsumerConfig.GROUP_ID_CONFIG, generateBroadcastGroupId());
        this.busConsumer = new KafkaConsumer<>(cConfig);
        //this.busConsumer.assign(singleton(new TopicPartition(busConfig.getTopic(), 0)));
        // TODO custom re-balancing listener?
        this.busConsumer.subscribe(singleton(busConfig.getTopic()), new NoOpConsumerRebalanceListener());

        // If the bus config consumer subscription is initialized successfully, it is considered ready.
        if (readyLatch.getCount() < COORDINATOR_READY_LATCH_COUNT) {
            readyLatch.countDown();
        }

        if (log.isInfoEnabled()) {
            log.info("Initialized coordinator bus config consumer client on '{}/{}'.",
                    busConfig.getTopic(), coordinatorConfig.getBootstrapServers());
        }
    }

    protected void initCoordinatorDiscovery() {
        final KafkaBusConfig busConfig = coordinatorConfig.getBusConfig();
        final KafkaDiscoveryConfig discoveryConfig = coordinatorConfig.getDiscoveryConfig();

        if (log.isInfoEnabled()) {
            log.info("Initializing coordinator admin client on '{}' ...", coordinatorConfig.getBootstrapServers());
        }
        final Map<String, Object> aConfig = new HashMap<>();
        aConfig.putAll(discoveryConfig.getAdminProps());
        aConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, coordinatorConfig.getBootstrapServers());
        this.discoveryAdminClient = KafkaUtil.createAdminClient(aConfig);

        if (log.isInfoEnabled()) {
            log.info("Initializing coordinator discovery consumer on '{}' ...",
                    coordinatorConfig.getBootstrapServers());
        }
        final Map<String, Object> cConfig = new HashMap<>();
        cConfig.putAll(discoveryConfig.getConsumerProps());
        cConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, localDiscoveryClientId);
        cConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, coordinatorConfig.getBootstrapServers());
        cConfig.put(ConsumerConfig.GROUP_ID_CONFIG, COORDINATOR_DISCOVERY_GROUP_ID);
        this.discoveryConsumer = new KafkaConsumer<>(cConfig);
        //this.discoveryConsumer.assign(singleton(new TopicPartition(busConfig.getTopic(), 0)));
        this.discoveryConsumer.subscribe(singleton(busConfig.getTopic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                doReBalanceForDiscovery();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                doReBalanceForDiscovery();
            }
        });
        if (log.isInfoEnabled()) {
            log.info("Initialized coordinator discovery client on '{}'.",
                    coordinatorConfig.getBootstrapServers());
        }
    }

    @Override
    public void doRun() {
        initCoordinatorBus();
        initCoordinatorDiscovery();

        while (getRunning().get()) {
            final ConsumerRecords<String, String> records = busConsumer.poll(Duration.ofMillis(500L));
            if (records.isEmpty()) {
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug("Received event channel records: {}", records);
            }
            records.forEach(record -> onBusEvent(parseJSON(record.value(), BusEvent.class)));
            this.busConsumer.commitSync();

            if (log.isInfoEnabled()) {
                final List<String> values = StreamSupport.stream(records.spliterator(), false)
                        .map(ConsumerRecord::value)
                        .collect(toList());
                log.info("Processed to bus config records: {}", values);
            }
        }
    }

    private void doReBalanceForDiscovery() {
        try {
            final KafkaDiscoveryConfig discoveryConfig = coordinatorConfig.getDiscoveryConfig();

            final List<MemberDescription> members = safeList(KafkaUtil.getGroupConsumers(discoveryAdminClient,
                    discoveryConfig.getGroupId(), coordinatorConfig.getDiscoveryConfig()
                            .getDiscoveryTimeoutMs()));

            final List<ServerInstance> instances = members
                    .stream()
                    .map(member -> ServerInstance.builder()
                            .instanceId(member.clientId())
                            .selfInstance(StringUtils.equals(member.clientId(), localDiscoveryClientId))
                            .host(member.host()).build())
                    .collect(toList());

            onDiscovery(instances);

            // When service discovery is performed successfully for the first time, it is considered ready.
            if (readyLatch.getCount() < COORDINATOR_READY_LATCH_COUNT) {
                readyLatch.countDown();
            }
        } catch (Exception th) {
            log.error("Failed to do on re-balancing discovery", th);
        }
    }

    @Override
    public void waitForReady() throws TimeoutException, InterruptedException {
        if (readyLatch.await(coordinatorConfig.getWaitReadyTimeoutMs(), MILLISECONDS)) {
            throw new TimeoutException(String.format("Failed to wait for coordinator ready in %sms.",
                    coordinatorConfig.getWaitReadyTimeoutMs()));
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaCoordinatorConfig extends CoordinatorConfig {
        private @Default String bootstrapServers = "localhost:9092";
        private @Default KafkaBusConfig busConfig = new KafkaBusConfig();
        private @Default KafkaDiscoveryConfig discoveryConfig = new KafkaDiscoveryConfig();

        @Override
        public void validate() {
            super.validate();

            Assert2.hasText(bootstrapServers, "bootstrapServers");
            requireNonNull(busConfig, "busConfig is null");
            requireNonNull(discoveryConfig, "discoveryConfig is null");

            busConfig.validate();
            discoveryConfig.validate();
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaBusConfig {
        private @Default String topic = "data-connector-coordinator-bus-topic";
        private @Default Map<String, Object> adminProps = new HashMap<>();
        private @Default Map<String, Object> consumerProps = new HashMap<>();
        private @Default Map<String, Object> producerProps = new HashMap<>();

        public void validate() {
            // Apply to default properties.
            DEFAULT_ADMIN_PROPS.forEach((key, value) -> adminProps.putIfAbsent(key, value));
            DEFAULT_CONSUMER_PROPS.forEach((key, value) -> consumerProps.putIfAbsent(key, value));
            DEFAULT_PRODUCER_PROPS.forEach((key, value) -> producerProps.putIfAbsent(key, value));

            requireNonNull(topic, "topic is null");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaDiscoveryConfig {
        private @Default String groupId = "data-connector-coordinator-discovery-group";
        private @Default Map<String, Object> consumerProps = new HashMap<>();
        private @Default Map<String, Object> adminProps = new HashMap<>();
        private @Default long discoveryTimeoutMs = 30_000L;

        public void validate() {
            // Apply to default properties.
            DEFAULT_CONSUMER_PROPS.forEach((key, value) -> consumerProps.putIfAbsent(key, value));
            DEFAULT_ADMIN_PROPS.forEach((key, value) -> adminProps.putIfAbsent(key, value));

            requireNonNull(groupId, "groupId is null");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class KafkaCoordinatorProvider extends CoordinatorProvider {
        public static final String TYPE_NAME = "KAFKA_COORDINATOR";
        private KafkaDataConnectorCoordinator instance;

        private @Default KafkaCoordinatorConfig coordinatorConfig = new KafkaCoordinatorConfig();

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public void validate() {
            requireNonNull(coordinatorConfig, "coordinatorConfig is null");
            coordinatorConfig.validate();
        }

        @Override
        public IDataConnectorCoordinator obtain(
                Environment environment,
                DataConnectorConfiguration config,
                IDataConnectorConfigurator configurator,
                CachingChannelRegistry registry,
                DataConnectorMeter meter) {
            if (isNull(instance)) {
                synchronized (this) {
                    if (isNull(instance)) {
                        instance = new KafkaDataConnectorCoordinator(
                                environment, config, configurator, registry, meter, coordinatorConfig);
                    }
                }
            }
            return instance;
        }
    }

    public static final String COORDINATOR_BUS_PRODUCER_TXID = "coordinator-bus-txid";
    public static final String COORDINATOR_BUS_GROUP_ID_TPL = "coordinator-bus-group-%s-%s";
    public static final String COORDINATOR_DISCOVERY_GROUP_ID = getenv().getOrDefault("COORDINATOR_DISCOVERY_GROUP_ID",
            "coordinator-discovery-group");
    public static final int COORDINATOR_READY_LATCH_COUNT = 2;
    static final Map<String, Object> DEFAULT_CONSUMER_PROPS = new HashMap<String, Object>() {
        {
            //put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
            //put(ConsumerConfig.CLIENT_ID_CONFIG, "");
            put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10_000);
            put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
            put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
            put(ConsumerConfig.SEND_BUFFER_CONFIG, 128);
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45_000);
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // 显示指定 Range 分配器确保同一时刻一个 partition 只分配给一个 consumer
            put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
            // This consumer is used for consumption dynamic configuration similar to spring cloud config bus kafka,
            // so read isolation should be used to ensure consistency.
            put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());
        }
    };
    static final Map<String, Object> DEFAULT_PRODUCER_PROPS = new HashMap<String, Object>() {
        {
            //put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
            //put(ProducerConfig.CLIENT_ID_CONFIG, "");
            put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10_000);
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
            put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
            put(ProducerConfig.SEND_BUFFER_CONFIG, 128);
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // snappy|gzip|lz4|zstd|none
            put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
            put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
            put(ProducerConfig.LINGER_MS_CONFIG, 0);
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            put(ProducerConfig.ACKS_CONFIG, "all");
            put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, COORDINATOR_BUS_PRODUCER_TXID);
            put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
    };
    static final Map<String, Object> DEFAULT_ADMIN_PROPS = new HashMap<String, Object>() {
        {
            //put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
            put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60_000);
            put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10_000);
        }
    };

}
