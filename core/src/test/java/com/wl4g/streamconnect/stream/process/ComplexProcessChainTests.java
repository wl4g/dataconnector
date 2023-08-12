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

package com.wl4g.streamconnect.stream.process;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.ChannelInfo.PolicySpec;
import com.wl4g.streamconnect.config.ChannelInfo.RuleSpec;
import com.wl4g.streamconnect.config.ChannelInfo.SettingsSpec;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import com.wl4g.streamconnect.stream.process.ComplexProcessChain.ComplexProcessResult;
import com.wl4g.streamconnect.stream.process.filter.StandardExprProcessFilter;
import com.wl4g.streamconnect.stream.process.map.NoOpProcessMapper;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;
import static java.util.Collections.singletonList;

/**
 * The {@link ComplexProcessChainTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ComplexProcessChainTests {

    @Test
    public void testComplexProcessChainEnd() {
        // ---------------------- Filters -------------------------

        final List<Integer> filterTracing = new ArrayList<>();

        final StandardExprProcessFilter filter1 = new StandardExprProcessFilter() {
            @Override
            public boolean doFilter(ChannelInfo channelInfo, MessageRecord<String, Object> record) {
                filterTracing.add(1);
                return super.doFilter(channelInfo, record);
            }
        };
        filter1.setName("filter1");
        filter1.updateMergeConditions(singletonList(ChannelInfo
                .builder()
                .settingsSpec(SettingsSpec.builder().policySpec(PolicySpec.builder()
                                .rules(singletonList(RuleSpec
                                        .builder()
                                        .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"age == 18\"}}")
                                        .build()))
                                .build())
                        .checkpointSpec(null)
                        .sinkSpec(null)
                        .build())
                .build()));

        final StandardExprProcessFilter filter2 = new StandardExprProcessFilter() {
            @Override
            public boolean doFilter(ChannelInfo channelInfo, MessageRecord<String, Object> record) {
                filterTracing.add(2);
                return super.doFilter(channelInfo, record);
            }
        };
        filter2.setName("filter2");
        filter2.updateMergeConditions(singletonList(ChannelInfo
                .builder()
                .settingsSpec(SettingsSpec.builder().policySpec(PolicySpec.builder()
                                .rules(singletonList(RuleSpec
                                        .builder()
                                        .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"age > 18\"}}") // The end
                                        .build()))
                                .build())
                        .checkpointSpec(null)
                        .sinkSpec(null)
                        .build())
                .build()));

        final StandardExprProcessFilter filter3 = new StandardExprProcessFilter() {
            @Override
            public boolean doFilter(ChannelInfo channelInfo, MessageRecord<String, Object> record) {
                // never called
                filterTracing.add(3);
                return super.doFilter(channelInfo, record);
            }
        };
        filter3.setName("filter3");
        filter3.updateMergeConditions(singletonList(ChannelInfo
                .builder()
                .settingsSpec(SettingsSpec.builder().policySpec(PolicySpec.builder()
                                .rules(singletonList(RuleSpec
                                        .builder()
                                        .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"age < 20\"}}") // never called
                                        .build()))
                                .build())
                        .checkpointSpec(null)
                        .sinkSpec(null)
                        .build())
                .build()));

        // ---------------------- Mappers -------------------------

        final List<Integer> mapperTracing = new ArrayList<>();

        final NoOpProcessMapper mapper1 = new NoOpProcessMapper() {
            @Override
            public MessageRecord<String, Object> doMap(ChannelInfo channel,
                                                       MessageRecord<String, Object> record) {
                mapperTracing.add(1);
                return super.doMap(channel, record);
            }
        };
        mapper1.setName("mapper1");

        // never called
        NoOpProcessMapper mapper2 = new NoOpProcessMapper() {
            @Override
            public MessageRecord<String, Object> doMap(ChannelInfo channel,
                                                       MessageRecord<String, Object> record) {
                mapperTracing.add(2);
                return record; // the end
            }
        };
        mapper2.setName("mapper2");

        // never called
        NoOpProcessMapper mapper3 = new NoOpProcessMapper() {
            @Override
            public MessageRecord<String, Object> doMap(ChannelInfo channel,
                                                       MessageRecord<String, Object> record) {
                mapperTracing.add(3);
                return super.doMap(channel, record);
            }
        };
        mapper3.setName("mapper3");

        // ---------------------- Chain -------------------------

        final ComplexProcessChain mockChain = new ComplexProcessChain(new ComplexProcessHandler[] {filter1, mapper1, filter2, mapper2, filter3, mapper3});
        final ObjectNode mockValue = (ObjectNode) parseToNode("{\"name\":\"Mary\",\"age\":18,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}");
        final MessageRecord<String, Object> record = new MessageRecord<String, Object>() {

            @Override
            public String getKey() {
                return null;
            }

            @Override
            public Object getValue() {
                return mockValue;
            }

            @Override
            public long getTimestamp() {
                return 0;
            }
        };
        final ComplexProcessResult mockResult = mockChain.process(ChannelInfo.builder().build(), record);

        // ---------------------- Assertion ---------------------

        Assertions.assertFalse(mockResult.isMatched());
        Assertions.assertEquals(2, filterTracing.size());
        Assertions.assertTrue(filterTracing.contains(1));
        Assertions.assertTrue(filterTracing.contains(2));
        Assertions.assertFalse(filterTracing.contains(3));
        Assertions.assertTrue(mapperTracing.contains(1));
        Assertions.assertFalse(mapperTracing.contains(2));
        Assertions.assertFalse(mapperTracing.contains(3));
    }

}
