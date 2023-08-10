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

package com.wl4g.streamconnect.process.map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;

/**
 * The {@link ProcessMapperChainTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ProcessMapperChainTests {

    @Test
    public void testMapperChainEnd() {
        final List<Integer> tracing = new ArrayList<>();

        NoOpProcessMapper mapper1 = new NoOpProcessMapper() {
            @Override
            public ConsumerRecord<String, ObjectNode> doMap(ProcessMapperChain chain,
                                                            SubscriberInfo subscriber,
                                                            ConsumerRecord<String, ObjectNode> record) {
                tracing.add(1);
                return super.doMap(chain, subscriber, record);
            }
        };
        mapper1.setName("mapper1");

        NoOpProcessMapper mapper2 = new NoOpProcessMapper() {
            @Override
            public ConsumerRecord<String, ObjectNode> doMap(ProcessMapperChain chain,
                                                            SubscriberInfo subscriber,
                                                            ConsumerRecord<String, ObjectNode> record) {
                tracing.add(2);
                return record; // the end
            }
        };
        mapper2.setName("mapper2");

        NoOpProcessMapper mapper3 = new NoOpProcessMapper() {
            @Override
            public ConsumerRecord<String, ObjectNode> doMap(ProcessMapperChain chain,
                                                            SubscriberInfo subscriber,
                                                            ConsumerRecord<String, ObjectNode> record) {
                // never called
                tracing.add(3);
                return super.doMap(chain, subscriber, record);
            }
        };
        mapper3.setName("mapper3");

        ProcessMapperChain chain = new ProcessMapperChain(new IProcessMapper[]{mapper1, mapper2, mapper3});
        ObjectNode value = (ObjectNode) parseToNode("{\"name\":\"Mary\",\"age\":18,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}");
        ConsumerRecord<String, ObjectNode> record = new ConsumerRecord<>("myTopic", 1, 1, "myKey", value);
        chain.doMap(SubscriberInfo.builder().build(), record);

        Assertions.assertEquals(2, tracing.size());
        Assertions.assertTrue(tracing.contains(1));
        Assertions.assertTrue(tracing.contains(2));
        Assertions.assertFalse(tracing.contains(3));
    }

}
