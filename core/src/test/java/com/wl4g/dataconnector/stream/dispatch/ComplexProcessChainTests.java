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

package com.wl4g.dataconnector.stream.dispatch;

import com.wl4g.dataconnector.DataConnectorMockerSetup;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.ComplexProcessChain.ComplexProcessResult;
import com.wl4g.dataconnector.stream.dispatch.filter.IProcessFilter;
import com.wl4g.dataconnector.stream.dispatch.filter.StandardExprProcessFilterTests;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper;
import com.wl4g.dataconnector.stream.dispatch.map.NoOpProcessMapperTests;
import com.wl4g.dataconnector.stream.dispatch.map.StandardExprProcessMapperTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;

/**
 * The {@link ComplexProcessChainTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ComplexProcessChainTests {

    @Test
    public void testComplexProcessChain() {
        // ---------------------- Filters -------------------------

        final List<Integer> filterTracing = new ArrayList<>();

        final ChannelInfo channel1 = DataConnectorMockerSetup.buildDefaultMockChannelInfo("c1001", "connector_1",
                "age == 18");
        final ChannelInfo channel2 = DataConnectorMockerSetup.buildDefaultMockChannelInfo("c1002", "connector_1",
                "age > 18");
        final ChannelInfo channel3 = DataConnectorMockerSetup.buildDefaultMockChannelInfo("c1003", "connector_1",
                "age < 20"); // never called

        final IProcessFilter filter1 = record -> {
            filterTracing.add(1);
            return StandardExprProcessFilterTests.buildDefaultMockStandardExprProcessFilter(channel1, null).doFilter(record);
        };
        final IProcessFilter filter2 = record -> {
            filterTracing.add(2);
            return StandardExprProcessFilterTests.buildDefaultMockStandardExprProcessFilter(channel2, null).doFilter(record);
        };
        final IProcessFilter filter3 = record -> {
            filterTracing.add(3);
            return StandardExprProcessFilterTests.buildDefaultMockStandardExprProcessFilter(channel3, null).doFilter(record);
        };

        // ---------------------- Mappers -------------------------

        final List<Integer> mapperTracing = new ArrayList<>();

        final IProcessMapper mapper1 = record -> {
            mapperTracing.add(1);
            return StandardExprProcessMapperTests.buildDefaultMockStandardExprProcessMapper(channel1, null).doMap(record);
        };
        // never called
        final IProcessMapper mapper2 = record -> {
            mapperTracing.add(2);
            return NoOpProcessMapperTests.buildDefaultMockNoOpProcessMapper(channel2).doMap(record);
        };
        // never called
        final IProcessMapper mapper3 = record -> {
            mapperTracing.add(3);
            return NoOpProcessMapperTests.buildDefaultMockNoOpProcessMapper(channel3).doMap(record);
        };

        // ---------------------- Chain -------------------------

        final ComplexProcessChain mockChain = new ComplexProcessChain(new ComplexProcessHandler[]{filter1, mapper1, filter2, mapper2, filter3, mapper3});
        final MessageRecord<String, Object> record = new MessageRecord<String, Object>() {
            @Override
            public String getKey() {
                return null;
            }

            @Override
            public Object getValue() {
                return parseToNode("{\"name\":\"Mary\",\"age\":18,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}");
            }

            @Override
            public long getTimestamp() {
                return 0;
            }
        };
        final ComplexProcessResult mockResult = mockChain.process(record);

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
