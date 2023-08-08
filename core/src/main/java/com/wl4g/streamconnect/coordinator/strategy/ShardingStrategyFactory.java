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

package com.wl4g.streamconnect.coordinator.strategy;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

/**
 * The {@link ShardingStrategyFactory}
 *
 * @author James Wong
 * @since v1.0
 **/
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ShardingStrategyFactory {

    private final static Map<String, IShardingStrategy> CACHED;

    static {
        CACHED = new ConcurrentHashMap<>(2);
        StreamSupport
                .stream(ServiceLoader.load(IShardingStrategy.class).spliterator(), false)
                .forEach(strategy -> CACHED.putIfAbsent(strategy.getType(), strategy));
    }

    /**
     * Get sharding strategy.
     *
     * @param type job sharding strategy type
     * @return job sharding strategy
     */
    public static IShardingStrategy getStrategy(final String type) {
        if (Strings.isNullOrEmpty(type)) {
            return new AverageShardingStrategy();
        }
        return Objects.requireNonNull(CACHED.get(type),
                String.format("Not found sharding strategy type %s", type));
    }

}
