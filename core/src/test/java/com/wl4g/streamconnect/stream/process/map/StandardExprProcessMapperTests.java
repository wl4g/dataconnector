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

package com.wl4g.streamconnect.stream.process.map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.ChannelInfo.CheckpointSpec;
import com.wl4g.streamconnect.config.ChannelInfo.PolicySpec;
import com.wl4g.streamconnect.config.ChannelInfo.RuleSpec;
import com.wl4g.streamconnect.config.ChannelInfo.SettingsSpec;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import com.wl4g.streamconnect.stream.process.map.StandardExprProcessMapper.JQConfig;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * The {@link StandardExprProcessMapperTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class StandardExprProcessMapperTests {

    @Test
    public void testSimpleDoMap() {
        final PolicySpec mockPolicySpec = PolicySpec.builder()
                .sequence(true)
                .rules(singletonList(RuleSpec.builder()
                        .tenantId("t1001")
                        .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"t > 1690345000000 && props.online == true\"}}")
                        .fieldFilter("del(.address.city)")
                        .build()))
                .build();

        final CheckpointSpec mockCheckpointSpec = CheckpointSpec.builder()
                .servers("localhost:9092")
                .retentionTime(60 * 60 * 1000L)
                .retentionBytes(1024 * 1024 * 1024L)
                .build();

        final SinkStream.SinkStreamConfig mockSinkSpec = new SinkStream.SinkStreamConfig() {
            @Override
            public String getType() {
                return "KAFKA_SINK";
            }
        };
        mockSinkSpec.setQos("qos_1");

        final SettingsSpec mockSettingsSpec = SettingsSpec.builder()
                .policySpec(mockPolicySpec)
                .checkpointSpec(mockCheckpointSpec)
                .sinkSpec(mockSinkSpec)
                .build();

        final ChannelInfo mockChannel = ChannelInfo.builder()
                .id("c1001")
                .name("channel_1001")
                .tenantId("t1001")
                .settingsSpec(mockSettingsSpec)
                .build();
        mockChannel.validate();

        final StandardExprProcessMapper mockMapper = new StandardExprProcessMapper();

        final JQConfig jqConfig = new JQConfig();
        jqConfig.setMaxCacheSize(8);
        jqConfig.setRegisterScopes(singletonMap("del", "net.thisptr.jackson.jq.internal.functions.DelFunction"));
        mockMapper.setJqConfig(jqConfig);

        mockMapper.updateMergeConditions(singletonList(mockChannel));

        final MessageRecord<String, Object> mockRecord = new MessageRecord<String, Object>() {
            @Override
            public String getKey() {
                return "myKey";
            }

            @Override
            public ObjectNode getValue() {
                return (ObjectNode) parseToNode("{\"name\":\"John\",\"age\":30,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}");
            }

            @Override
            public long getTimestamp() {
                return 0;
            }
        };

        final MessageRecord<String, Object> mockResult = mockMapper.doMap(mockChannel, mockRecord);

        Assertions.assertEquals("{\"name\":\"John\",\"age\":30,\"address\":{\"zipcode\":\"12345\"}}",
                mockResult.getValue().toString());
    }

}
