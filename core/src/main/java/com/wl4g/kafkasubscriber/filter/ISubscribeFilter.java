/*
 * Copyright 2017 ~ 2025 the original authors James Wong.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g.kafkasubscriber.filter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeFilterConfig;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * The custom subscribe processing filtering, support {@link #doMatch(SubscriberInfo, ConsumerRecord)}
 * by record row match filtering, and {@link #doMap(SubscriberInfo, ConsumerRecord)} by record column filtering.
 *
 * @author James Wong
 * @since v1.0
 **/
public interface ISubscribeFilter {

    String getName();

    String getType();

    SubscribeFilterConfig getFilterConfig();

    void validate();

    boolean doMatch(SubscriberInfo subscriber,
                    ConsumerRecord<String, ObjectNode> record);

    default ConsumerRecord<String, ObjectNode> doMap(SubscriberInfo subscriber,
                                                     ConsumerRecord<String, ObjectNode> record) {
        return record;
    }

    void updateConfigWithMergeSubscribers(List<SubscriberInfo> subscribers, long delayTime);

}
