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

package com.wl4g.kafkasubscriber.custom;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.config.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.dispatch.FilterBatchMessageDispatcher;
import com.wl4g.kafkasubscriber.util.KafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.wl4g.kafkasubscriber.dispatch.AbstractBatchMessageDispatcher.KEY_SUBSCRIBER_ID;

/**
 * The {@link SubscribeEngineCustomizer}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface SubscribeEngineCustomizer {

    List<SubscriberInfo> loadSubscribers(@NotBlank String pipelineName,
                                         @Null SubscriberInfo query);

    default boolean matchSubscriberRecord(@NotBlank String pipelineName,
                                          @NotNull SubscriberInfo subscriber,
                                          @NotNull ConsumerRecord<String, ObjectNode> record) {
        Assert2.notNullOf(subscriber, "subscriber");
        Assert2.notNullOf(record, "record");
        // Notice: By default, the $subscriberId field of the source message match, which should be customized
        // to match the subscriber relationship corresponding to each record in the source consume topic.
        if (Objects.nonNull(record.value())) {
            // Priority match with message header.
            final Iterator<Header> it = record.headers().headers("$$subscriber").iterator();
            if (it.hasNext()) {
                // match first only
                final String first = KafkaUtil.getFirstValueAsString(record.headers(),
                        FilterBatchMessageDispatcher.KEY_SUBSCRIBER_ID);
                return StringUtils.equals(first, subscriber.getId());
            }
            // Fallback match with message value.
            return StringUtils.equals(record.value().remove(KEY_SUBSCRIBER_ID)
                    .asText(""), subscriber.getId());
        }
        return false;
    }

    default String generateCheckpointTopic(@NotBlank String pipelineName,
                                           @NotBlank String topicPrefix,
                                           @NotBlank String subscriberId) {
        Assert2.hasTextOf(topicPrefix, "topicPrefix");
        Assert2.hasTextOf(subscriberId, "subscriberId");
        if (!StringUtils.endsWithAny(topicPrefix, "-", "_")) {
            topicPrefix += "_";
        }
        return topicPrefix.concat(String.valueOf(subscriberId));
    }

    default String generateSinkGroupId(@NotBlank String pipelineName,
                                       @Null KafkaSubscribeConfiguration.SubscribeSinkConfig subscribeSinkConfig,
                                       @Null String subscriberId) {
        String groupIdPrefix = subscribeSinkConfig.getGroupIdPrefix();
        if (!StringUtils.endsWithAny(groupIdPrefix, "-", "_")) {
            groupIdPrefix += "_";
        }
        return groupIdPrefix.concat(String.valueOf(subscriberId));
    }

}
