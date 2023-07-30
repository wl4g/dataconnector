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
import com.wl4g.kafkasubscriber.config.KafkaSubscriberAutoConfiguration;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;

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
//@EmbeddedKafka(ports = {59092}, count = 1, zookeeperPort = 52181)
//@RunWith(SpringRunner.class)
//@SpringBootTest(classes = {
//        KafkaSubscriberAutoConfiguration.class, FilterBatchMessageDispatcherIT.class
//})
//@SpringBootApplication(exclude = {JdbcTemplateAutoConfiguration.class, DataSourceAutoConfiguration.class})
//@SpringBootConfiguration
//@AutoConfigureMockMvc
//@ComponentScan(basePackages = {"com.wl4g", "org.springframework"})
public class FilterBatchMessageDispatcherIT {

    static boolean testKafkaEmbeddedEnable = Boolean.parseBoolean(System.getenv().getOrDefault("IT_KAFKA_EMBEDDED_ENABLE", "true"));
    static String testBrokerServers = System.getenv().getOrDefault("IT_KAFKA_SERVERS", "localhost:9092");
    static String testZkServers = System.getenv().getOrDefault("IT_KAFKA_ZK_SERVERS", "localhost:2181");
    static String testInputTopic = System.getenv().getOrDefault("IT_KAFKA_INPUT_TOPIC", "test-input");

    @Autowired(required = false)
    EmbeddedKafkaBroker embeddedKafkaBroker;

    //@Before
    //public void init() {
    //    if (testKafkaEmbeddedEnable) {
    //        System.out.println("-------------------- Integration Test embedded kafka broker initializing ... ---------------------------");
    //        testBrokerServers = embeddedKafkaBroker.getBrokersAsString();
    //        testZkServers = embeddedKafkaBroker.getZookeeperConnectionString();
    //        System.out.printf("-------------------- Started embedded kafka broker: %s --------------------", testBrokerServers);
    //    } else {
    //        System.out.println("-------------------- Integration Test embedded kafka broker disabled. --------------------");
    //    }
    //}

    @Test
    public void start() {
        recreateTestTopic();
        startProduceTestTopic();
        validateFilteredTopicRecords();
    }

    static void recreateTestTopic() {
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
        CountDownLatch latch = new CountDownLatch(4);
        Executor executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 2; i++) {
            final int _i = i;
            executor.execute(() -> {
                try {
                    int j = 0;
                    while (++j <= 2) {
                        String key = String.format("test-key-%s-%s", _i, counter.incrementAndGet());
                        String value = String.format("{\"cts\":%s,\"__properties__\":{\"__online__\":{\"connected\":true,\"directlyLinked\":true,\"sequence\":\"%s-%s\"}}}",
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static void validateFilteredTopicRecords() {
        // TODO

    }

}
