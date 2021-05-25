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

package com.wl4g.kafkasubscriber.facade;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.validation.constraints.Null;
import java.util.List;
import java.util.Objects;

/**
 * The {@link SubscribeFacade}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface SubscribeFacade {

    List<SubscriberInfo> findSubscribers(@Null SubscriberInfo query);

    default boolean matchSubscriberRecord(@Null SubscriberInfo subscriber,
                                          @Null ConsumerRecord<String, ObjectNode> record) {
        // Notice: By default, the $subscriberId field of the source message match, which should be customized
        // to match the subscriber relationship corresponding to each record in the source consume topic.
        if (Objects.nonNull(subscriber) && Objects.nonNull(record.value())) {
            return record.value().at("/$subscriberId").asLong(-1L) == subscriber.getId();
        }
        return false;
    }

    default String generateFilteredTopic(@Null KafkaSubscriberProperties.FilterProperties filterConfig,
                                         @Null SubscriberInfo subscriber) {
        return generateFilteredTopic(filterConfig, subscriber.getId());
    }

    default String generateFilteredTopic(@Null KafkaSubscriberProperties.FilterProperties filterConfig,
                                         @Null Long subscriberId) {
        String topicPrefix = filterConfig.getTopicPrefix();
        if (!StringUtils.endsWithAny(topicPrefix, "-", "_")) {
            topicPrefix += "_";
        }
        return topicPrefix.concat(String.valueOf(subscriberId));
    }

    default String generateSinkGroupId(@Null KafkaSubscriberProperties.SinkProperties sinkConfig,
                                       @Null Long subscriberId) {
        String groupIdPrefix = sinkConfig.getGroupIdPrefix();
        if (!StringUtils.endsWithAny(groupIdPrefix, "-", "_")) {
            groupIdPrefix += "_";
        }
        return groupIdPrefix.concat(String.valueOf(subscriberId));
    }

}
