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

package com.wl4g.kafkasubscriber.it;

import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;
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
 * The {@link FilterBatchMessageDispatcherIT}
 *
 * @author James Wong
 * @since v1.0
 **/
public class FilterBatchMessageDispatcherIT {

    static boolean testKafkaEmbeddedEnable = Boolean.parseBoolean(System.getenv().getOrDefault("IT_KAFKA_EMBEDDED_ENABLE", "true"));
    static String testBrokerServers = System.getenv().getOrDefault("IT_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    static String testInputTopic = System.getenv().getOrDefault("IT_KAFKA_INPUT_TOPIC", "test-input");
    static int testInputTopicPartitions = Integer.parseInt(System.getenv().getOrDefault("IT_KAFKA_INPUT_TOPIC_PARTITION", "10"));
    static KafkaContainer embeddedKafkaContainer = null;

    @BeforeClass
    public static void init() {
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

    @Test
    public void start() {
        recreateTestTopic();
        startProduceTestTopic();
    }

    static void recreateTestTopic() {
        try (AdminClient adminClient = AdminClient.create(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBrokerServers));) {
            System.out.println("Recreating test topic: " + testInputTopic);
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(testInputTopic));
            result.all().get();
            System.out.println("Deleted test topic: " + testInputTopic);
            System.out.println("Creating test topic: " + testInputTopic);
            CreateTopicsResult result2 = adminClient.createTopics(Collections.singleton(new NewTopic(testInputTopic, testInputTopicPartitions, (short) 1)));
            result2.all().get();
            System.out.println("Created test topic: " + testInputTopic);
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    static void startProduceTestTopic() {
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
        CountDownLatch latch = new CountDownLatch(10);
        Executor executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 10; i++) {
            final int _i = i;
            executor.execute(() -> {
                int j = 0;
                while (++j < 200) {
                    try {
                        String key = String.format("test-key-%s-%s", _i, counter.incrementAndGet());
                        String value = String.format("{\"cts\":%s,\"__properties__\":{\"__online__\":{\"connected\":true,\"directlyLinked\":true,\"sequence\":\"%s-%s\"}}}",
                                System.currentTimeMillis(), _i, counter.get());
                        System.out.printf("Send to topic: %s, key: %s, value: %s%n", testInputTopic, key, value);
                        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("test-input", key, value));
                        //System.out.println(future.get());
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
                System.out.println("flushing of " + _i);
                producer.flush();
                System.out.println("flushed of " + _i);
            });
        }
        try {
            System.out.println("---------------- waiting for all tasks completed");
            latch.await();
            System.out.println("---------------- all tasks completed");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
