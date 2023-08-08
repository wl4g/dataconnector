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

package com.wl4g.streamconnect.custom;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.bean.SubscriberInfo.SubscribeGrantedPolicy;
import com.wl4g.streamconnect.config.StreamConnectProperties;
import com.wl4g.streamconnect.config.StreamConnectProperties.SubscribeSourceProperties;
import com.wl4g.streamconnect.dispatch.ProcessBatchMessageDispatcher;
import com.wl4g.streamconnect.util.KafkaUtil;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.List;
import java.util.Objects;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.streamconnect.dispatch.AbstractBatchMessageDispatcher.KEY_TENANT;

/**
 * The {@link StreamConnectEngineCustomizer}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface StreamConnectEngineCustomizer {

    List<SubscriberInfo> loadSubscribers(@NotBlank String pipelineName,
                                         @Null IStreamConnectCoordinator.ShardingInfo sharding);

    SubscribeSourceProperties loadSourceByTenant(@NotBlank String pipelineName,
                                                 @NotBlank String tenantId);

    default boolean matchSubscriberRecord(@NotBlank String pipelineName,
                                          @NotNull SubscriberInfo subscriber,
                                          @NotNull ConsumerRecord<String, ObjectNode> record) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        Assert2.notNullOf(subscriber, "subscriber");
        Assert2.notNullOf(record, "record");

        // Notice: By default, the $$tenant field of the source message match, which should be customized
        // to match the subscriber relationship corresponding to each record in the source consume topic.
        if (Objects.nonNull(record.value())) {
            // Priority match with message header.
            final String tenantId1 = KafkaUtil.getFirstValueAsString(record.headers(), // match first only
                    ProcessBatchMessageDispatcher.KEY_TENANT);
            if (!StringUtils.isBlank(tenantId1)) {
                return safeList(subscriber.getRule().getPolicies())
                        .stream()
                        .map(SubscribeGrantedPolicy::getTenantId)
                        .anyMatch(tid -> StringUtils.equals(tenantId1, tid));
            }
            // Fallback match with message value.
            final String tenantId2 = record.value().remove(KEY_TENANT).textValue();
            if (!StringUtils.isBlank(tenantId2)) {
                return safeList(subscriber.getRule().getPolicies())
                        .stream()
                        .map(SubscribeGrantedPolicy::getTenantId)
                        .anyMatch(tid -> StringUtils.equals(tenantId2, tid));
            }
        }
        return false;
    }

    default String generateCheckpointTopic(@NotBlank String pipelineName,
                                           @NotBlank String topicPrefix,
                                           @NotBlank String subscriberId) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        Assert2.hasTextOf(topicPrefix, "topicPrefix");
        Assert2.hasTextOf(subscriberId, "subscriberId");

        if (!StringUtils.endsWithAny(topicPrefix, "-", "_")) {
            topicPrefix += "_";
        }
        return topicPrefix.concat(String.valueOf(subscriberId));
    }

    default String generateSinkGroupId(@NotBlank String pipelineName,
                                       @Null StreamConnectProperties.SubscribeSinkProperties subscribeSinkConfig,
                                       @Null String subscriberId) {
        Assert2.hasTextOf(pipelineName, "pipelineName");

        String groupIdPrefix = subscribeSinkConfig.getGroupIdPrefix();
        if (!StringUtils.endsWithAny(groupIdPrefix, "-", "_")) {
            groupIdPrefix += "_";
        }
        return groupIdPrefix.concat(String.valueOf(subscriberId));
    }

}
