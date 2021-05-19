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

package com.wl4g.kafkasubscriber.sink;

import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link ShardingSubscriberCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public abstract class ShardingSubscriberCoordinator {

    private @Autowired SubscriberRegistry registry;

    protected void update(ShardingInfo sharding) {
        log.info("Re-balancing of sharding: {} ...", sharding);
        final List<SubscriberInfo> rebalancedSubscribers = registry.getAll().stream().filter(s -> sharding.getShardingTotalCount() % s.getId() == sharding.getShardingItem()).collect(Collectors.toList());
        log.info("Re-balanced of sharding: {}, {}, subscribers: {}", sharding, rebalancedSubscribers.size(), rebalancedSubscribers);
        registry.putAll(rebalancedSubscribers.stream().collect(Collectors.toMap(SubscriberInfo::getId, s -> s)));
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString(callSuper = true)
    public static class ShardingInfo {
        private int shardingTotalCount;
        private int shardingItem;
    }

}
