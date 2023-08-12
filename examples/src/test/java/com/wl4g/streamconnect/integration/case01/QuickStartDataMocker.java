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

package com.wl4g.streamconnect.integration.case01;

import com.wl4g.streamconnect.integration.base.mock.AbstractDataMocker;
import com.wl4g.streamconnect.stream.AbstractStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.wl4g.infra.common.serialize.JacksonUtils.toJSONString;
import static com.wl4g.streamconnect.integration.base.GenericITContainerManager.IT_DATA_MOCKERS_TIMEOUT;
import static java.lang.System.out;
import static java.util.Collections.singleton;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

/**
 * The {@link QuickStartDataMocker}
 *
 * @author James Wong
 * @since v1.0
 **/
public class QuickStartDataMocker extends AbstractDataMocker {

    private static final Random random = new Random();
    Supplier<CountDownLatch> finishedLatchSupplier;
    String kafkaServers;
    String kafkaSourceTopic;
    String tenantId;
    AtomicInteger statisticsTotal = new AtomicInteger(0);
    AtomicInteger statisticsDebugTrueTotal = new AtomicInteger(0);
    AtomicInteger statisticsDebugFalseTotal = new AtomicInteger(0);
    AtomicInteger statisticsConnectedTrueTotal = new AtomicInteger(0);
    AtomicInteger statisticsConnectedFalseTotal = new AtomicInteger(0);

    public QuickStartDataMocker(Supplier<CountDownLatch> finishedLatchSupplier,
                                String kafkaServers,
                                String kafkaSourceTopic,
                                String tenantId) {
        this.finishedLatchSupplier = requireNonNull(finishedLatchSupplier);
        this.kafkaServers = requireNonNull(kafkaServers);
        this.kafkaSourceTopic = requireNonNull(kafkaSourceTopic);
        this.tenantId = tenantId;
    }

    @Override
    public void run() {
        recreateKafkaSourceTopics();
        runConcurrencyKafkaProducing();
        finishedLatchSupplier.get().countDown();
    }

    @Override
    public void printStatistics() {
        final StringBuilder sb = new StringBuilder(1024 * 2);
        sb.append(String.format("\n--- %s :: Print(start) :: %s - %s :: ---\n", getClass().getSimpleName(), tenantId, kafkaServers));
        sb.append("\n       debug true Total : ").append(statisticsDebugTrueTotal);
        sb.append("\n      debug false Total : ").append(statisticsDebugFalseTotal);
        sb.append("\n   connected true Total : ").append(statisticsConnectedTrueTotal);
        sb.append("\n  connected false Total : ").append(statisticsConnectedFalseTotal);
        sb.append("\n          Summary Total : ").append(statisticsTotal);
        sb.append(String.format("\n--- %s :: Print(end  ) :: %s - %s :: ---\n", getClass().getSimpleName(), tenantId, kafkaServers));
        out.println(sb);
    }

    private void recreateKafkaSourceTopics() {
        System.out.printf("Recreating to Kafka source topic: %s on %s%n", kafkaSourceTopic, kafkaServers);
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        //props.put(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG, MockCustomHostResolver.class.getName());
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                DeleteTopicsResult result = adminClient.deleteTopics(singleton(kafkaSourceTopic));
                result.all().get(2L, TimeUnit.MINUTES);
                System.out.println("Deleted Kafka source topic: " + kafkaSourceTopic);
            } catch (Throwable ex) {
                Throwable reason = ExceptionUtils.getRootCause(ex);
                if (reason instanceof UnknownTopicOrPartitionException) {
                    System.out.println("Not exists Kafka source topic: " + kafkaSourceTopic);
                } else {
                    throw new IllegalStateException(String.format("Could not delete Kafka source topic: %s",
                            kafkaSourceTopic), ex);
                }
            }
            try {
                System.out.println("Creating RocketMQ input topic: " + kafkaSourceTopic);
                CreateTopicsResult result = adminClient.createTopics(singleton(new NewTopic(kafkaSourceTopic, 10, (short) 1)));
                result.all().get(2L, TimeUnit.MINUTES);
                System.out.println("Created Kafka source topic: " + kafkaSourceTopic);
            } catch (Throwable ex) {
                Throwable reason = ExceptionUtils.getRootCause(ex);
                if (reason instanceof TopicExistsException) {
                    System.out.println("Already exists Kafka source topic: " + kafkaSourceTopic);
                } else {
                    throw new IllegalStateException(String.format("Could not create Kafka source topic: %s, " +
                            "please check sets to auto.create.topics.enable=true ?", kafkaSourceTopic), ex);
                }
            }
        }
    }

    private void runConcurrencyKafkaProducing() {
        final int total = 100, sharding = 10;
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(total / sharding);
        final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        try {
            for (int i = 0, shardingItems = total / sharding; i < sharding; i++) {
                final int index = i;
                executor.execute(() -> {
                    final Map<String, Object> props = new HashMap<>();
                    //props.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, MockCustomHostResolver.class.getName());
                    props.put(ProducerConfig.ACKS_CONFIG, "all");
                    props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
                    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32");
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    Producer<String, Object> producer = new KafkaProducer<>(props);
                    try {
                        for (int j = 0; j < shardingItems; j++) {
                            statisticsTotal.incrementAndGet();

                            final String key = String.format("%s_%s", tenantId, counter.incrementAndGet());
                            final Properties value = new Properties();
                            value.put("msgId", key);
                            value.put("cts", System.currentTimeMillis());
                            value.put(AbstractStream.KEY_TENANT, tenantId);

                            if (random.nextBoolean()) {
                                value.put("debug", true);
                                statisticsDebugTrueTotal.incrementAndGet();
                            } else {
                                value.put("debug", false);
                                statisticsDebugFalseTotal.incrementAndGet();
                            }

                            final Properties properties = new Properties();
                            final Properties online = new Properties();
                            if (random.nextBoolean()) {
                                online.put("connected", true);
                                statisticsConnectedTrueTotal.incrementAndGet();
                            } else {
                                online.put("connected", false);
                                statisticsConnectedFalseTotal.incrementAndGet();
                            }
                            online.put("directlyLinked", "true");
                            properties.put("__online__", online);
                            value.put("__properties__", properties);

                            log.info("Sending to Kafka source topic: {}, key: {}, value: {}", kafkaSourceTopic, key, value);
                            ProducerRecord<String, Object> pr = new ProducerRecord<>(kafkaSourceTopic, key, toJSONString(value));
                            final Future<RecordMetadata> future = producer.send(pr);
                            log.info("Sent to Kafka source topic: {}, key: {} of result: {}", kafkaSourceTopic, key, future.get());
                        }

                        log.info("Flushing of {}/{}", tenantId, index);
                        producer.flush();
                        log.info("Flushed of {}/{}", tenantId, index);
                    } catch (Throwable ex) {
                        try {
                            if (nonNull(producer)) {
                                producer.close();
                            }
                            producer = new KafkaProducer<>(props);
                        } catch (Throwable ex2) {
                            log.error("Recreate producer error", ex2);
                            if (nonNull(producer)) {
                                producer.close();
                            }
                        }
                        //Throwable reason = ExceptionUtils.getRootCause(ex);
                        //if (reason instanceof NotLeaderOrFollowerException) {
                        //    log.warn(">>> Re-trying mock producing to kafka '{}', caused by: {}", kafkaServers, reason.getMessage());
                        //} else {
                        //    throw new IllegalStateException(String.format("Could not sent mock data to kafka of %s", kafkaServers), ex);
                        //}
                    } finally {
                        latch.countDown();
                    }
                });
            }
            try {
                log.info("Waiting for all mock executor tasks completion ...");
                if (!latch.await(IT_DATA_MOCKERS_TIMEOUT, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(String.format("Timeout for all mock tasks completed. on %s/%s", kafkaServers, kafkaSourceTopic));
                } else {
                    log.info("Completed for all mock tasks!");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } finally {
            executor.shutdown();
        }
    }

}
