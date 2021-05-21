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
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.validation.constraints.Null;
import java.util.List;

/**
 * The {@link SubscribeFacade}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface SubscribeFacade {

    List<SubscriberInfo> findSubscribers(@Null SubscriberInfo query);

    boolean matchSubscriberRecord(@Null SubscriberInfo subscriber,
                                  @Null ConsumerRecord<String, ObjectNode> record);

    default String generateFilteredTopic(@Null KafkaSubscriberProperties.SubscribePipelineProperties config,
                                         @Null SubscriberInfo subscriber) {
        return generateFilteredTopic(config, subscriber.getId());
    }

    default String generateFilteredTopic(@Null KafkaSubscriberProperties.SubscribePipelineProperties config,
                                         @Null Long subscriberId) {
        return config.getFilter().getTopicPrefix().concat("-").concat(String.valueOf(subscriberId));
    }

}
