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

package com.wl4g.kafkasubscriber;

import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link IntegrationTestTopicInitializer}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class IntegrationTestTopicInitializer {

    static boolean testKafkaEmbeddedEnable = Boolean.parseBoolean(System.getenv().getOrDefault("IT_KAFKA_EMBEDDED_ENABLE", "true"));
    static String testInputTopic = System.getenv().getOrDefault("IT_KAFKA_INPUT_TOPIC", "test-input");
    private String testBrokerServers = System.getenv().getOrDefault("IT_KAFKA_SERVERS", "localhost:9092");
    //private String testZkServers = System.getenv().getOrDefault("IT_KAFKA_ZK_SERVERS", "localhost:2181");
    private KafkaContainer embeddedKafkaContainer = null;

    public void start() {
        initKafkaBroker();
        recreateTestTopic();
        startProduceTestTopic();
    }

    private void initKafkaBroker() {
        if (testKafkaEmbeddedEnable) {
            System.out.println("-------------------- Integration Test embedded kafka broker initializing ... ---------------------------");
            embeddedKafkaContainer = new KafkaContainer(DockerImageName.parse("registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_kafka:2.2.0")
                    .asCompatibleSubstituteFor("confluentinc/cp-kafka")).withEmbeddedZookeeper();
            System.out.println("Starting embedded kafka broker ... ");
            embeddedKafkaContainer.start();
            testBrokerServers = embeddedKafkaContainer.getBootstrapServers();
            System.out.printf("-------------------- Started embedded kafka broker: %s --------------------", testBrokerServers);
        } else {
            System.out.println("-------------------- Integration Test embedded kafka broker disabled. --------------------");
        }
    }

    private void recreateTestTopic() {
        System.out.println("Recreating test topic: " + testInputTopic);
        try (AdminClient adminClient = AdminClient.create(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBrokerServers));) {
            try {
                DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(testInputTopic));
                result.all().get();
                System.out.println("Deleted test topic: " + testInputTopic);
            } catch (Throwable ex) {
                Throwable reason = ExceptionUtils.getRootCause(ex);
                if (reason instanceof UnknownTopicOrPartitionException) {
                    System.out.println("Not found test topic: " + testInputTopic);
                } else {
                    ex.printStackTrace();
                }
            }
            try {
                System.out.println("Creating test topic: " + testInputTopic);
                CreateTopicsResult result2 = adminClient.createTopics(Collections.singleton(new NewTopic(testInputTopic, 10, (short) 1)));
                result2.all().get();
                System.out.println("Created test topic: " + testInputTopic);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
        }
    }

    private void startProduceTestTopic() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBrokerServers);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, "3");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "100");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducerBuilder(configProps).buildKafkaProducer();

        AtomicLong counter = new AtomicLong(0);
        int total = 100, recordsPerExecutor = 10;
        CountDownLatch latch = new CountDownLatch(total / recordsPerExecutor);
        Executor executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < total / recordsPerExecutor; i++) {
            final int _i = i;
            executor.execute(() -> {
                try {
                    int j = 0;
                    while (++j <= recordsPerExecutor) {
                        String key = String.format("test-key-%s-%s", _i, counter.incrementAndGet());
                        String value = String.format("{\"cts\":%s,\"__properties__\":{\"__online__\":{\"connected\":true,\"directlyLinked\":true,\"seq\":\"%s-%s\"}},\"$subscriberId\":54321}",
                                System.currentTimeMillis(), _i, counter.get());
                        System.out.printf("Send to topic: %s, key: %s, value: %s%n", testInputTopic, key, value);
                        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("test-input", key, value));
                        //System.out.println(future.get());
                    }
                    System.out.println("flushing of " + _i);
                    producer.flush();
                    System.out.println("flushed of " + _i);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            System.out.println("---------------- waiting for all tasks completed");
            latch.await();
            System.out.println("---------------- all tasks completed");
            producer.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
