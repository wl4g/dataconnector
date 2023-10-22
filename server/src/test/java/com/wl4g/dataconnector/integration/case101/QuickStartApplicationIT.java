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

package com.wl4g.dataconnector.integration.case101;

import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.DataConnectorApplication;
import com.wl4g.infra.common.tests.integration.AssertionSpringApplicationIT;
import com.wl4g.infra.common.tests.integration.manager.AssertionITContainerManager;
import com.wl4g.infra.common.tests.integration.mock.IDataMocker;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static com.wl4g.dataconnector.integration.case101.QuickStartDataMocker.KEY_TENANT;
import static java.lang.Integer.parseInt;
import static java.lang.System.getenv;

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
public class QuickStartApplicationIT extends AssertionSpringApplicationIT {
    public static final String KAFKA_CLUSTER_01 = "kafka01";
    public static final String KAFKA_CLUSTER_02 = "kafka02";
    public static final String ROCKETMQ_CLUSTER_01 = "rocketmq01";
    private static final int DEFAULT_KAFKA_PARTITION = parseInt(getenv().getOrDefault("IT_101_KAFKA_PARTITION", "3"));

    public static final String KAFKA_MOCKER_T1001 = "kafkaMocker01";
    public static final String KAFKA_MOCKER_T1002 = "kafkaMocker02";
    public static final String KAFKA_MOCKER_T1003 = "kafkaMocker03";

    // Runtime of container and consumer definitions.
    private static final AssertionITContainerManager containerManager = new AssertionITContainerManager(QuickStartApplicationIT.class) {
        @Override
        protected void initMwContainers(@NotNull Supplier<CountDownLatch> startedLatchSupplier,
                                        Map<String, ITGenericContainer> mwContainers) {
            final Map<String, String> kafkaEnv = new HashMap<>();
            // broker default partition config.
            // see:https://kafka.apache.org/35/documentation.html#brokerconfigs_num.partitions
            kafkaEnv.put("KAFKA_CFG_NUM_PARTITIONS", String.valueOf(DEFAULT_KAFKA_PARTITION));
            mwContainers.put(KAFKA_CLUSTER_01, buildBitnamiKafka35xContainer(startedLatchSupplier, 59092, 59997, kafkaEnv));
            mwContainers.put(KAFKA_CLUSTER_02, buildBitnamiKafka35xContainer(startedLatchSupplier, 59093, 59998, kafkaEnv));
            mwContainers.put(ROCKETMQ_CLUSTER_01, buildApacheRocketMQ49xContainer(startedLatchSupplier, 59876));
        }

        @Override
        protected void initDataMockers(@NotNull Supplier<CountDownLatch> finishedLatchSupplier,
                                       @NotNull Map<String, IDataMocker> dataMockers) {
            log.info("Starting for data mockers ...");

            dataMockers.put(KAFKA_MOCKER_T1001, new QuickStartDataMocker(finishedLatchSupplier,
                    getServersConnectString(KAFKA_CLUSTER_01, 59092),
                    "it-quickstart-source-topic-shared",
                    "t1001"));

            dataMockers.put(KAFKA_MOCKER_T1002, new QuickStartDataMocker(finishedLatchSupplier,
                    getServersConnectString(KAFKA_CLUSTER_01, 59092),
                    "it-quickstart-source-topic-shared",
                    "t1002"));

            dataMockers.put(KAFKA_MOCKER_T1003, new QuickStartDataMocker(finishedLatchSupplier,
                    getServersConnectString(KAFKA_CLUSTER_02, 59093),
                    "it-quickstart-source-topic-t1003",
                    "t1003"));
        }
    };

    public QuickStartApplicationIT() {
        super(DataConnectorApplication.class);
    }

    @BeforeEach
    public void setup() throws Exception {
        containerManager.start();
    }

    @AfterEach
    public void shutdown() throws IOException {
        containerManager.close();
    }

    @Override
    protected void registerApplicationEnvironment() {
        System.setProperty("IT_SOURCE_KAFKA_SERVERS_01", getKafkaCluster01());
        System.setProperty("IT_SOURCE_KAFKA_SERVERS_02", getKafkaCluster02());
        System.setProperty("IT_CHECKPOINT_KAFKA_SERVERS_01", getKafkaCluster01());
        System.setProperty("IT_SINK_KAFKA_SERVERS_01", getKafkaCluster01());
        System.setProperty("IT_SINK_KAFKA_SERVERS_02", getKafkaCluster02());
        System.setProperty("IT_SINK_ROCKETMQ_SERVERS_01", getRocketMQCluster01());
        System.setProperty("IT_COORDINATOR_KAFKA_SERVERS_01", getKafkaCluster01());
    }

    @Override
    protected String[] applicationLaunchArgs() {
        return new String[] {
                "--logging.level.root=INFO",
                "--spring.profiles.active=it-quickstart",
        };
    }

    @Override
    protected int shouldRegisterAssertionTasksTotal() {
        return 3;
    }

    @Override
    protected List<Runnable> doRegisterAssertionTasks(CountDownLatch assertionLatch) {
        final List<Runnable> tasks = new ArrayList<>((int) assertionLatch.getCount());
        final int perTaskTimeoutSec = IT_ASSERTION_TIMEOUT;

        final QuickStartDataMocker dataMocker01 = containerManager.getRequiredDataMocker(KAFKA_MOCKER_T1001);
        final int assertNum1 = dataMocker01.statisticsConnectedTrueTotal.get();
        tasks.add(containerManager.buildKafkaConsumingAssertionRunner(assertionLatch, getKafkaCluster01(),
                "it-quickstart-sink-topic-c1001",
                "it-assertion-sink-group-c1001",
                null,
                perTaskTimeoutSec,
                value -> {
                    final String tenantId = value.at("/".concat(KEY_TENANT)).asText();
                    return StringUtils.equalsIgnoreCase(tenantId, "t1001");
                },
                assertNum1,
                result -> {
                    // Record filter result assertion.
                    Assertions.assertEquals(assertNum1, result.size(), "assertion failed, because assertion count is not equals");
                }));

        final QuickStartDataMocker dataMocker02 = containerManager.getRequiredDataMocker(KAFKA_MOCKER_T1002);
        final int assertNum2 = dataMocker02.statisticsConnectedFalseTotal.get() + dataMocker02.statisticsConnectedTrueTotal.get();
        tasks.add(containerManager.buildKafkaConsumingAssertionRunner(assertionLatch, getKafkaCluster02(),
                "it-quickstart-sink-topic-c1002",
                "it-assertion-sink-group-c1002",
                null,
                perTaskTimeoutSec,
                value -> {
                    final String tenantId = value.at("/".concat(KEY_TENANT)).asText();
                    return StringUtils.equalsIgnoreCase(tenantId, "t1002");
                },
                assertNum2,
                result -> {
                    // Record filter result assertion.
                    Assertions.assertEquals(assertNum2, result.size(), "assertion failed, because assertion count is not equals");

                    // Field filter result assertion.
                    result.forEach((key, value) -> {
                        final JsonNode connected = value.at("/__properties__/__online__/connected");
                        Assertions.assertNull(connected, "assertion failed, because '/__properties__/__online__/connected' is not deleted by mapper");
                    });
                }));

        final QuickStartDataMocker dataMocker03 = containerManager.getRequiredDataMocker(KAFKA_MOCKER_T1003);
        final int assertNum3 = dataMocker03.statisticsConnectedTrueTotal.get();
        tasks.add(containerManager.buildRocketMQConsumingAssertionRunner(assertionLatch, getRocketMQCluster01(),
                "it-quickstart-sink-topic-c1003",
                "it-assertion-sink-group-c1003",
                perTaskTimeoutSec,
                value -> {
                    final String tenantId = value.at("/".concat(KEY_TENANT)).asText();
                    return StringUtils.equalsIgnoreCase(tenantId, "t1003");
                },
                assertNum3,
                result -> {
                    // Record filter result assertion.
                    Assertions.assertEquals(assertNum3, result.size(), "assertion failed, because assertion count is not equals");
                }));

        return tasks;
    }

    private static String getKafkaCluster01() {
        return containerManager.getServersConnectString(KAFKA_CLUSTER_01, 59092);
    }

    private static String getKafkaCluster02() {
        return containerManager.getServersConnectString(KAFKA_CLUSTER_02, 59093);
    }

    private static String getRocketMQCluster01() {
        return containerManager.getServersConnectString(ROCKETMQ_CLUSTER_01, 59876);
    }

}
