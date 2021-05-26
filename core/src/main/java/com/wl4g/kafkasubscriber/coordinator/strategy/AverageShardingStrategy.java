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

package com.wl4g.kafkasubscriber.coordinator.strategy;

import com.wl4g.kafkasubscriber.coordinator.ISubscribeCoordinator.ServiceInstance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link AverageShardingStrategy}
 *
 * @author James Wong
 * @since v1.0
 **/
public class AverageShardingStrategy implements IShardingStrategy {

    @Override
    public String getType() {
        return "AVG_SHARDING";
    }

    @Override
    public Map<ServiceInstance, List<Integer>> getShardingItem(final int shardingTotalCount,
                                                               final Collection<ServiceInstance> instances) {
        if (instances.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<ServiceInstance, List<Integer>> result = shardingAliquot(instances, shardingTotalCount);
        addAliquant(instances, shardingTotalCount, result);
        return result;
    }

    private Map<ServiceInstance, List<Integer>> shardingAliquot(final Collection<ServiceInstance> shardingUnits,
                                                                final int shardingTotalCount) {
        Map<ServiceInstance, List<Integer>> result = new LinkedHashMap<>(shardingUnits.size(), 1);
        int itemCountPerSharding = shardingTotalCount / shardingUnits.size();
        int count = 0;
        for (ServiceInstance each : shardingUnits) {
            List<Integer> shardingItems = new ArrayList<>(itemCountPerSharding + 1);
            for (int i = count * itemCountPerSharding; i < (count + 1) * itemCountPerSharding; i++) {
                shardingItems.add(i);
            }
            result.put(each, shardingItems);
            count++;
        }
        return result;
    }

    private void addAliquant(final Collection<ServiceInstance> shardingUnits,
                             final int shardingTotalCount,
                             final Map<ServiceInstance, List<Integer>> shardingResults) {
        int aliquant = shardingTotalCount % shardingUnits.size();
        int count = 0;
        for (Map.Entry<ServiceInstance, List<Integer>> entry : shardingResults.entrySet()) {
            if (count < aliquant) {
                entry.getValue().add(shardingTotalCount / shardingUnits.size() * shardingUnits.size() + count);
            }
            count++;
        }
    }

}
