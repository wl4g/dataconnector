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

package com.wl4g.streamconnect.quickstart;

import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.streamconnect.base.AbstractDataMocker;
import com.wl4g.streamconnect.base.AssertionApplicationIT;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static com.wl4g.streamconnect.stream.AbstractStream.KEY_TENANT;

/**
 * The middlewares usage matrix for this integration test is as follows:
 *
 * <pre>
 * ------------------------------------------------------------------------------------------------------------------------------
 * | Middlewares/Usages  | Source Streams | Checkpoint    | Sink Streams | Coordinator | Channel Policies | Mock data of Tenant |
 * ------------------------------------------------------------------------------------------------------------------------------
 * |    Kafka Cluster 01 | channel(1,2)   | checkpoint(1) | channel(1)   |      √      | channel(1,2)     | tenant(1,2)         |
 * ------------------------------------------------------------------------------------------------------------------------------
 * |    Kafka Cluster 02 | channel(3)     |       -       | channel(2)   |      -      | channel(3)       | tenant(3)           |
 * ------------------------------------------------------------------------------------------------------------------------------
 * | RocketMQ Cluster 01 |        -       |       -       | channel(3)   |      -      |        -         |          -          |
 * ------------------------------------------------------------------------------------------------------------------------------
 * </pre>
 *
 * @author James Wong
 * @since v1.0
 **/
//
// Notice: That using @RunWith/@SpringBootTest to start the application cannot control the startup
// sequence (e.g after the kafka container is started), so it can only be controlled by manual startup.
//
//@SpringJUnitConfig
//@RunWith(SpringRunner.class)
//@SpringBootTest(classes = StreamConnectExamples.class,
//        properties = {
//                "spring.profiles.active=it-quickstart",
//                "logging.level.root=INFO",
//        })
public class QuickStartApplicationIT extends AssertionApplicationIT {
    public static final String KAFKA_CLUSTER_01 = "kafka01";
    public static final String KAFKA_CLUSTER_02 = "kafka02";
    public static final String ROCKETMQ_CLUSTER_01 = "rocketmq01";

    public static final String KAFKA_MOCKER_T1001 = "kafkaMocker01";
    public static final String KAFKA_MOCKER_T1002 = "kafkaMocker02";
    public static final String KAFKA_MOCKER_T1003 = "kafkaMocker03";

    //
    // Notice: That using @RunWith/@SpringBootTest to start the application cannot control the startup
    // sequence (e.g after the kafka container is started), so it can only be controlled by manual startup.
    //
    //static {
    //    extraEnvSupplier.put("IT_SOURCE_KAFKA_SERVERS_01", () -> getKafkaClusterServers(KAFKA_CLUSTER_01));
    //    extraEnvSupplier.put("IT_SOURCE_KAFKA_SERVERS_02", () -> getKafkaClusterServers(KAFKA_CLUSTER_02));
    //    extraEnvSupplier.put("IT_CHECKPOINT_KAFKA_SERVERS_01", () -> getKafkaClusterServers(KAFKA_CLUSTER_01));
    //    extraEnvSupplier.put("IT_SINK_KAFKA_SERVERS_01", () -> getKafkaClusterServers(KAFKA_CLUSTER_01));
    //    extraEnvSupplier.put("IT_SINK_KAFKA_SERVERS_02", () -> getKafkaClusterServers(KAFKA_CLUSTER_02));
    //    extraEnvSupplier.put("IT_SINK_ROCKETMQ_SERVERS_01", () -> getRocketMQClusterServers(ROCKETMQ_CLUSTER_01));
    //    extraEnvSupplier.put("IT_COORDINATOR_KAFKA_SERVERS_01", () -> getKafkaClusterServers(KAFKA_CLUSTER_01));
    //}

    @Override
    protected void initMiddlewareContainers(@NotNull Supplier<CountDownLatch> startedLatchSupplier,
                                            Map<String, ITGenericContainerWrapper> mwContainers) {
        mwContainers.put(KAFKA_CLUSTER_01, buildBitnamiKafkaContainer(startedLatchSupplier, 59092, 9092));
        mwContainers.put(KAFKA_CLUSTER_02, buildBitnamiKafkaContainer(startedLatchSupplier, 59093, 9092));
        mwContainers.put(ROCKETMQ_CLUSTER_01, buildApacheRocketMQContainer(startedLatchSupplier, 59876, 9876));
    }

    @Override
    protected void initDataMockers(@NotNull Supplier<CountDownLatch> finishedLatchSupplier,
                                   @NotNull Map<String, AbstractDataMocker> dataMockers) {
        log.info("Starting for kafka data mockers ...");

        dataMockers.put(KAFKA_MOCKER_T1001, new QuickStartDataMocker(finishedLatchSupplier,
                getKafkaClusterServers(KAFKA_CLUSTER_01),
                "it-quickstart-source-topic-shared",
                "t1001"));

        dataMockers.put(KAFKA_MOCKER_T1002, new QuickStartDataMocker(finishedLatchSupplier,
                getKafkaClusterServers(KAFKA_CLUSTER_01),
                "it-quickstart-source-topic-shared",
                "t1002"));

        dataMockers.put(KAFKA_MOCKER_T1003, new QuickStartDataMocker(finishedLatchSupplier,
                getKafkaClusterServers(KAFKA_CLUSTER_02),
                "it-quickstart-source-topic-t1003",
                "t1003"));
    }

    @Override
    protected void registerApplicationEnvironment() {
        System.setProperty("IT_SOURCE_KAFKA_SERVERS_01", getKafkaClusterServers(KAFKA_CLUSTER_01));
        System.setProperty("IT_SOURCE_KAFKA_SERVERS_02", getKafkaClusterServers(KAFKA_CLUSTER_02));
        System.setProperty("IT_CHECKPOINT_KAFKA_SERVERS_01", getKafkaClusterServers(KAFKA_CLUSTER_01));
        System.setProperty("IT_SINK_KAFKA_SERVERS_01", getKafkaClusterServers(KAFKA_CLUSTER_01));
        System.setProperty("IT_SINK_KAFKA_SERVERS_02", getKafkaClusterServers(KAFKA_CLUSTER_02));
        System.setProperty("IT_SINK_ROCKETMQ_SERVERS_01", getRocketMQClusterServers(ROCKETMQ_CLUSTER_01));
        System.setProperty("IT_COORDINATOR_KAFKA_SERVERS_01", getKafkaClusterServers(KAFKA_CLUSTER_01));
    }

    @Override
    protected String[] applicationLaunchArgs() {
        String[] launchArgs = {
                "--logging.level.root=INFO",
                "--spring.profiles.active=it-quickstart",
        };
        return launchArgs;
    }

    @Override
    protected int shouldRegisterAssertionTasksTotal() {
        return 3;
    }

    @Override
    protected List<Runnable> doRegisterAssertionTasks(CountDownLatch assertionLatch) {
        final List<Runnable> tasks = new ArrayList<>((int) assertionLatch.getCount());
        final int perTaskTimeoutSec = IT_ASSERTION_TIMEOUT;

        final QuickStartDataMocker dataMocker01 = getRequiredDataMocker(KAFKA_MOCKER_T1001);
        final int assertNum1 = dataMocker01.statisticsConnectedTrueTotal.get();
        tasks.add(buildKafkaConsumingAssertionRunner(assertionLatch, getKafkaClusterServers(KAFKA_CLUSTER_01),
                "it-quickstart-sink-topic-c1001", "it-assertion-sink-group-c1001",
                perTaskTimeoutSec, value -> {
                    final String tenantId = value.at("/".concat(KEY_TENANT)).asText();
                    return StringUtils.equalsIgnoreCase(tenantId, "t1001");
                }, assertNum1, result -> {
                    // Record filter result assertion.
                    Assertions.assertEquals(assertNum1, result.size(), "assertion failed, because assertion count is not equals");
                }));

        final QuickStartDataMocker dataMocker02 = getRequiredDataMocker(KAFKA_MOCKER_T1002);
        final int assertNum2 = dataMocker02.statisticsConnectedFalseTotal.get() + dataMocker02.statisticsConnectedTrueTotal.get();
        tasks.add(buildKafkaConsumingAssertionRunner(assertionLatch, getKafkaClusterServers(KAFKA_CLUSTER_02),
                "it-quickstart-sink-topic-c1002", "it-assertion-sink-group-c1002",
                perTaskTimeoutSec, value -> {
                    final String tenantId = value.at("/".concat(KEY_TENANT)).asText();
                    return StringUtils.equalsIgnoreCase(tenantId, "t1002");
                }, assertNum2, result -> {
                    // Record filter result assertion.
                    Assertions.assertEquals(assertNum2, result.size(), "assertion failed, because assertion count is not equals");

                    // Field filter result assertion.
                    result.forEach((key, value) -> {
                        final JsonNode connected = value.at("/__properties__/__online__/connected");
                        Assertions.assertNull(connected, "assertion failed, because '/__properties__/__online__/connected' is not deleted by mapper");
                    });
                }));

        final QuickStartDataMocker dataMocker03 = getRequiredDataMocker(KAFKA_MOCKER_T1003);
        final int assertNum3 = dataMocker03.statisticsConnectedTrueTotal.get();
        tasks.add(buildRocketMQConsumingAssertionRunner(assertionLatch, getRocketMQClusterServers(ROCKETMQ_CLUSTER_01),
                "it-quickstart-sink-topic-c1003", "it-assertion-sink-group-c1003",
                perTaskTimeoutSec, value -> {
                    final String tenantId = value.at("/".concat(KEY_TENANT)).asText();
                    return StringUtils.equalsIgnoreCase(tenantId, "t1003");
                }, assertNum3, result -> {
                    // Record filter result assertion.
                    Assertions.assertEquals(assertNum3, result.size(), "assertion failed, because assertion count is not equals");
                }));

        return tasks;
    }

}
