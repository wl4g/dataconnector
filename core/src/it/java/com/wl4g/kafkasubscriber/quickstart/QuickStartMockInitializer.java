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

package com.wl4g.kafkasubscriber.quickstart;

import com.wl4g.infra.common.serialize.JacksonUtils;
import com.wl4g.kafkasubscriber.base.BasedMockInitializer;
import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import com.wl4g.kafkasubscriber.dispatch.FilterBatchMessageDispatcher;
import lombok.Getter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link QuickStartMockInitializer}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class QuickStartMockInitializer extends BasedMockInitializer {

    static final Random random = new Random();
    boolean enableEmbeddedBroker;
    String inputTopic;
    int tenantIdMinSuffixInclusive;
    int tenantIdMaxSuffixExclusive;
    String brokerServers;
    KafkaContainer embeddedKafkaContainer;

    AtomicLong mockStatisticsDebugTrueTotal = new AtomicLong(0);
    AtomicLong mockStatisticsDebugFalseTotal = new AtomicLong(0);
    AtomicLong mockStatisticsConnectedTrueTotal = new AtomicLong(0);
    AtomicLong mockStatisticsConnectedFalseTotal = new AtomicLong(0);
    AtomicLong mockStatisticsSubscriber01Total = new AtomicLong(0);
    AtomicLong mockStatisticsSubscriber02Total = new AtomicLong(0);
    AtomicLong mockStatisticsSubscriber03Total = new AtomicLong(0);

    public QuickStartMockInitializer(boolean enableEmbeddedBroker,
                                     String brokerServers,
                                     String inputTopic,
                                     int tenantIdMinSuffixInclusive,
                                     int tenantIdMaxSuffixExclusive) {
        this.enableEmbeddedBroker = enableEmbeddedBroker;
        this.brokerServers = Objects.requireNonNull(brokerServers);
        this.inputTopic = Objects.requireNonNull(inputTopic);
        this.tenantIdMinSuffixInclusive = tenantIdMinSuffixInclusive;
        this.tenantIdMaxSuffixExclusive = tenantIdMaxSuffixExclusive;
    }

    @Override
    public void run() {
        System.out.printf("Integration mock [%s] initializing ...", brokerServers);
        initKafkaBroker();
        recreateTestTopic();
        startProduceTestTopic();
    }

    @Override
    public void printStatistics() {
        System.out.printf("\n---------- %s :: PRINT(START) :: %s :: ----------%n",
                QuickStartMockInitializer.class.getSimpleName(), brokerServers);
        System.out.println("      debug true: " + mockStatisticsDebugTrueTotal);
        System.out.println("     debug false: " + mockStatisticsDebugFalseTotal);
        System.out.println("  connected true: " + mockStatisticsConnectedTrueTotal);
        System.out.println(" connected false: " + mockStatisticsConnectedFalseTotal);
        System.out.println("   subscriber 01: " + mockStatisticsSubscriber01Total);
        System.out.println("   subscriber 02: " + mockStatisticsSubscriber02Total);
        System.out.println("   subscriber 03: " + mockStatisticsSubscriber03Total);
        System.out.printf("\n---------- %s :: PRINT( END ) :: %s :: ----------%n",
                QuickStartMockInitializer.class.getSimpleName(), brokerServers);
    }

    private void initKafkaBroker() {
        if (enableEmbeddedBroker) {
            System.out.println("-------------------- Integration Test embedded kafka broker initializing ... ---------------------------");
            embeddedKafkaContainer = new KafkaContainer(DockerImageName.parse("registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/bitnami_kafka:2.2.0")
                    .asCompatibleSubstituteFor("confluentinc/cp-kafka")).withEmbeddedZookeeper();
            System.out.println("Starting embedded kafka broker ... ");
            embeddedKafkaContainer.start();
            brokerServers = embeddedKafkaContainer.getBootstrapServers();
            System.out.printf("-------------------- Started embedded kafka broker: %s --------------------", brokerServers);
        } else {
            System.out.println("-------------------- Integration Test embedded kafka broker disabled. --------------------");
        }
    }

    private void recreateTestTopic() {
        System.out.println("Recreating input topic: " + inputTopic);
        try (AdminClient adminClient = AdminClient.create(Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers));) {
            try {
                DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(inputTopic));
                result.all().get(2L, TimeUnit.MINUTES);
                System.out.println("Deleted input topic: " + inputTopic);
            } catch (Throwable ex) {
                Throwable reason = ExceptionUtils.getRootCause(ex);
                if (reason instanceof UnknownTopicOrPartitionException) {
                    System.out.println("Not found input topic: " + inputTopic);
                } else {
                    throw new IllegalStateException(ex);
                }
            }
            try {
                System.out.println("Creating input topic: " + inputTopic);
                CreateTopicsResult result2 = adminClient.createTopics(Collections.singleton(new NewTopic(inputTopic, 10, (short) 1)));
                result2.all().get(2L, TimeUnit.MINUTES);
                System.out.println("Created input topic: " + inputTopic);
            } catch (Throwable ex) {
                Throwable reason = ExceptionUtils.getRootCause(ex);
                if (reason instanceof TopicExistsException) {
                    System.out.println("Already exists input topic: " + inputTopic);
                } else {
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    private void startProduceTestTopic() {
        final Properties configProps = new Properties();
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, "3");
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "10");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final int mockTotal = 100, mockPerExecutorTotal = 10;
        final AtomicLong counter = new AtomicLong(0);
        final CountDownLatch latch = new CountDownLatch(mockTotal / mockPerExecutorTotal);
        final Executor executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < mockTotal / mockPerExecutorTotal; i++) {
            final int _i = i;
            executor.execute(() -> {
                try (Producer<String, String> producer = new KafkaProducerBuilder(configProps).buildProducer()) {
                    int j = 0;
                    while (++j <= mockPerExecutorTotal) {
                        String key = String.format("test-key-%s-%s", _i, counter.incrementAndGet());

                        Properties value = new Properties();
                        value.put("cts", System.currentTimeMillis());

                        int suffix = RandomUtils.nextInt(tenantIdMinSuffixInclusive, tenantIdMaxSuffixExclusive);
                        value.put(FilterBatchMessageDispatcher.KEY_TENANT, "t100" + suffix);
                        switch (suffix) {
                            case 1:
                                mockStatisticsSubscriber01Total.incrementAndGet();
                                break;
                            case 2:
                                mockStatisticsSubscriber02Total.incrementAndGet();
                                break;
                            case 3:
                                mockStatisticsSubscriber03Total.incrementAndGet();
                                break;
                        }

                        value.put("id", String.format("%s0000%s", _i, counter.get()));
                        if (random.nextBoolean()) {
                            value.put("debug", true);
                            mockStatisticsDebugTrueTotal.incrementAndGet();
                        } else {
                            value.put("debug", false);
                            mockStatisticsDebugFalseTotal.incrementAndGet();
                        }

                        Properties __properties__ = new Properties();
                        Properties __online__ = new Properties();
                        if (random.nextBoolean()) {
                            __online__.put("connected", true);
                            mockStatisticsConnectedTrueTotal.incrementAndGet();
                        } else {
                            __online__.put("connected", false);
                            mockStatisticsConnectedFalseTotal.incrementAndGet();
                        }
                        __online__.put("directlyLinked", "true");
                        __properties__.put("__online__", __online__);
                        value.put("__properties__", __properties__);

                        System.out.printf("Send to topic: %s, key: %s, value: %s%n", inputTopic, key, value);
                        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(inputTopic,
                                key, JacksonUtils.toJSONString(value)));
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

}
