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

package com.wl4g.streamconnect.process;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.bean.SubscriberInfo.SubscribeGrantedPolicy;
import com.wl4g.streamconnect.bean.SubscriberInfo.SubscribeRule;
import com.wl4g.streamconnect.process.ComplexProcessChain.ComplexProcessedResult;
import com.wl4g.streamconnect.process.filter.StandardExprProcessFilter;
import com.wl4g.streamconnect.process.map.NoOpProcessMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;
import static java.util.Collections.singletonList;

/**
 * The {@link ProcessComplexChainTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ProcessComplexChainTests {

    @Test
    public void testComplexProcessChainEnd() {
        // ---------------------- Filters -------------------------

        final List<Integer> filterTracing = new ArrayList<>();

        StandardExprProcessFilter filter1 = new StandardExprProcessFilter() {
            @Override
            public boolean doFilter(SubscriberInfo subscriberInfo, ConsumerRecord<String, ObjectNode> record) {
                filterTracing.add(1);
                return super.doFilter(subscriberInfo, record);
            }
        };
        filter1.setName("filter1");
        filter1.updateMergeSubscribeConditions(singletonList(SubscriberInfo
                .builder()
                .rule(SubscribeRule.builder()
                        .policies(singletonList(SubscribeGrantedPolicy
                                .builder()
                                .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"age == 18\"}}")
                                .build()))
                        .build())
                .build()));

        StandardExprProcessFilter filter2 = new StandardExprProcessFilter() {
            @Override
            public boolean doFilter(SubscriberInfo subscriberInfo, ConsumerRecord<String, ObjectNode> record) {
                filterTracing.add(2);
                return super.doFilter(subscriberInfo, record);
            }
        };
        filter2.setName("filter2");
        filter2.updateMergeSubscribeConditions(singletonList(SubscriberInfo
                .builder()
                .rule(SubscribeRule.builder()
                        .policies(singletonList(SubscribeGrantedPolicy
                                .builder()
                                .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"age > 18\"}}") // The end
                                .build()))
                        .build())
                .build()));

        StandardExprProcessFilter filter3 = new StandardExprProcessFilter() {
            @Override
            public boolean doFilter(SubscriberInfo subscriberInfo, ConsumerRecord<String, ObjectNode> record) {
                // never called
                filterTracing.add(3);
                return super.doFilter(subscriberInfo, record);
            }
        };
        filter3.setName("filter3");
        filter3.updateMergeSubscribeConditions(singletonList(SubscriberInfo
                .builder()
                .rule(SubscribeRule.builder()
                        .policies(singletonList(SubscribeGrantedPolicy
                                .builder()
                                .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"age < 20\"}}") // never called
                                .build()))
                        .build())
                .build()));

        // ---------------------- Mappers -------------------------

        final List<Integer> mapperTracing = new ArrayList<>();

        NoOpProcessMapper mapper1 = new NoOpProcessMapper() {
            @Override
            public ConsumerRecord<String, ObjectNode> doMap(SubscriberInfo subscriber,
                                                            ConsumerRecord<String, ObjectNode> record) {
                mapperTracing.add(1);
                return super.doMap(subscriber, record);
            }
        };
        mapper1.setName("mapper1");

        // never called
        NoOpProcessMapper mapper2 = new NoOpProcessMapper() {
            @Override
            public ConsumerRecord<String, ObjectNode> doMap(SubscriberInfo subscriber,
                                                            ConsumerRecord<String, ObjectNode> record) {
                mapperTracing.add(2);
                return record; // the end
            }
        };
        mapper2.setName("mapper2");

        // never called
        NoOpProcessMapper mapper3 = new NoOpProcessMapper() {
            @Override
            public ConsumerRecord<String, ObjectNode> doMap(SubscriberInfo subscriber,
                                                            ConsumerRecord<String, ObjectNode> record) {
                mapperTracing.add(3);
                return super.doMap(subscriber, record);
            }
        };
        mapper3.setName("mapper3");

        // ---------------------- Chain -------------------------

        ComplexProcessChain complexChain = new ComplexProcessChain(new ComplexProcessHandler[]{filter1, mapper1, filter2, mapper2, filter3, mapper3});
        ObjectNode value = (ObjectNode) parseToNode("{\"name\":\"Mary\",\"age\":18,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}");
        ConsumerRecord<String, ObjectNode> record = new ConsumerRecord<>("myTopic", 1, 1, "myKey", value);
        ComplexProcessedResult result = complexChain.doProcess(SubscriberInfo.builder().build(), record);

        // ---------------------- Assertion ---------------------

        Assertions.assertFalse(result.isMatched());
        Assertions.assertEquals(2, filterTracing.size());
        Assertions.assertTrue(filterTracing.contains(1));
        Assertions.assertTrue(filterTracing.contains(2));
        Assertions.assertFalse(filterTracing.contains(3));
        Assertions.assertTrue(mapperTracing.contains(1));
        Assertions.assertFalse(mapperTracing.contains(2));
        Assertions.assertFalse(mapperTracing.contains(3));
    }

}
