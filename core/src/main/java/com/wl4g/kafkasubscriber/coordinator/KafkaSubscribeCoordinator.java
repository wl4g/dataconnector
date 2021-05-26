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

package com.wl4g.kafkasubscriber.coordinator;

import com.wl4g.kafkasubscriber.facade.SubscribeEventPublisher.AddSubscribeEvent;
import com.wl4g.kafkasubscriber.facade.SubscribeEventPublisher.RemoveSubscribeEvent;
import com.wl4g.kafkasubscriber.facade.SubscribeEventPublisher.SubscribeEvent;
import com.wl4g.kafkasubscriber.facade.SubscribeEventPublisher.UpdateSubscribeEvent;
import com.wl4g.kafkasubscriber.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.serialize.JacksonUtils.parseJSON;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

/**
 * The {@link KafkaSubscribeCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class KafkaSubscribeCoordinator extends AbstractSubscribeCoordinator {

    public static final String SUBSCRIBE_COORDINATOR_TOPIC = "subscribe-coordinator-topic";
    public static final String SUBSCRIBE_COORDINATOR_GROUP = "subscribe-coordinator-group";
    public static final String SUBSCRIBE_COORDINATOR_CLIENT_ID = String.format("subscribe-coordinator-client-id-%s", RandomUtils.nextInt(1000000, 9999999));

    private KafkaConsumer<String, String> consumer;
    private AdminClient adminClient;

    @Override
    protected void init() {
        final Properties props = new Properties();
        // TODO config!!!
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, SUBSCRIBE_COORDINATOR_CLIENT_ID);
        props.put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // TODO 此分配器还是不能保证按照不同 Pod 的多个 consumers 隔离分配不同的 partitions，
        // TODO 因此无法严格实现保序 ??? (还是有概率被 brokers 将某个 partition 分配给了 2 个 Pod 的 consumer)
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        // This consumer is used for consumption dynamic configuration similar to spring cloud config bus kafka,
        // so read isolation should be used to ensure consistency.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.assign(singleton(new TopicPartition(SUBSCRIBE_COORDINATOR_TOPIC, 0)));

        this.consumer.subscribe(singleton(SUBSCRIBE_COORDINATOR_TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                doOnReBalancing();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                doOnReBalancing();
            }
        });

        // TODO using configuration
        this.adminClient = KafkaUtil.createAdminClient("localhost:9092");
    }

    @Override
    public void doRun() {
        while (getRunning().get()) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(800L));
            log.debug("Received subscribe event records: {}", records);

            records.forEach(record -> {
                //final String key = record.key();
                final SubscribeEvent event = parseJSON(record.value(), SubscribeEvent.class);

                if (event instanceof AddSubscribeEvent) {
                    log.info("Adding subscribe event: {}", event);
                    getRegistry().putAll(event.getPipelineName(), safeList(((AddSubscribeEvent) event).getSubscribers()));
                } else if (event instanceof UpdateSubscribeEvent) {
                    log.info("Updating subscribe event: {}", event);
                    getRegistry().putAll(event.getPipelineName(), safeList(((UpdateSubscribeEvent) event).getSubscribers()));
                } else if (event instanceof RemoveSubscribeEvent) {
                    log.info("Removing subscribe event: {}", event);
                    safeList(((RemoveSubscribeEvent) event).getSubscriberIds()).forEach(subscriberId -> {
                        getRegistry().remove(event.getPipelineName(), subscriberId);
                    });
                } else {
                    log.warn("Unsupported subscribe event type of: {}", event);
                }
            });

            // TODO using sync or callback?
            this.consumer.commitAsync();
        }
    }

    private void doOnReBalancing() {
        try {
            final List<MemberDescription> members = safeList(KafkaUtil.getGroupConsumers(adminClient,
                    SUBSCRIBE_COORDINATOR_GROUP, 6000L));

            final List<ServiceInstance> instances = members
                    .stream()
                    .map(member -> ServiceInstance.builder()
                            .instanceId(member.clientId())
                            .selfInstance(StringUtils.equals(member.clientId(), SUBSCRIBE_COORDINATOR_CLIENT_ID))
                            .host(member.host()).build())
                    .collect(toList());

            onDiscovery(instances);
        } catch (Throwable th) {
            log.error("Failed to do on re-balancing discovery", th);
        }
    }

}
