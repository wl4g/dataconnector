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

package com.wl4g.streamconnect.filter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.bean.SubscriberInfo.SubscribeGrantedPolicy;
import com.wl4g.streamconnect.bean.SubscriberInfo.SubscribeRule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;
import static java.util.Collections.singletonList;

/**
 * The {@link ProcessFilterChainTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ProcessFilterChainTests {

    @Test
    public void testFilterChainEnd() {
        final List<Integer> tracing = new ArrayList<>();

        StandardProcessFilter filter1 = new StandardProcessFilter() {
            @Override
            public boolean doFilter(SubscriberInfo subscriberInfo, ConsumerRecord<String, ObjectNode> record) {
                tracing.add(1);
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

        StandardProcessFilter filter2 = new StandardProcessFilter() {
            @Override
            public boolean doFilter(SubscriberInfo subscriberInfo, ConsumerRecord<String, ObjectNode> record) {
                tracing.add(2);
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

        StandardProcessFilter filter3 = new StandardProcessFilter() {
            @Override
            public boolean doFilter(SubscriberInfo subscriberInfo, ConsumerRecord<String, ObjectNode> record) {
                // never called
                tracing.add(3);
                return super.doFilter(subscriberInfo, record);
            }
        };
        filter3.setName("filter3");
        filter3.updateMergeSubscribeConditions(singletonList(SubscriberInfo
                .builder()
                .rule(SubscribeRule.builder()
                        .policies(singletonList(SubscribeGrantedPolicy
                                .builder()
                                .recordFilter("{\"type\":\"RELATION\",\"name\":\"testCondition1\",\"fn\":{\"expression\":\"age > 18\"}}")
                                .build()))
                        .build())
                .build()));

        ProcessFilterChain chain = new ProcessFilterChain(new IProcessFilter[]{filter1, filter2, filter3});
        ObjectNode value = (ObjectNode) parseToNode("{\"name\":\"Mary\",\"age\":18,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}");
        ConsumerRecord<String, ObjectNode> record = new ConsumerRecord<>("myTopic", 1, 1, "myKey", value);
        boolean result = chain.doFilter(SubscriberInfo.builder().build(), record);

        Assertions.assertFalse(result);
        Assertions.assertEquals(2, tracing.size());
        Assertions.assertTrue(tracing.contains(1));
        Assertions.assertTrue(tracing.contains(2));
        Assertions.assertFalse(tracing.contains(3));
    }

}
