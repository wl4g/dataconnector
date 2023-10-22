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

package com.wl4g.dataconnector.coordinator.strategy;

import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link RepeatShardingStrategy}
 *
 * @author James Wong
 * @since v1.0
 **/
public class RepeatShardingStrategy extends AbstractShardingStrategy {

    public static final String TYPE = "REPEAT_SHARDING";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Map<IDataConnectorCoordinator.ServerInstance, List<Integer>> getShardingItem(
            final int shardingTotalCount,
            final Collection<IDataConnectorCoordinator.ServerInstance> instances) {
        return safeList(instances)
                .stream()
                .collect(toMap(e -> e,
                        // Each instance owns all shards.
                        e -> IntStream.range(0, shardingTotalCount)
                                .boxed()
                                .collect(toList())));
    }

}
