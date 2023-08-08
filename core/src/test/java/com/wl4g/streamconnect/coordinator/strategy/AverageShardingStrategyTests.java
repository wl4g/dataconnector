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

import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator.ServiceInstance;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@link AverageShardingStrategyTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class AverageShardingStrategyTests {

    @Test
    public void testShardingItems() {
        List<ServiceInstance> instances = new ArrayList<>();
        instances.add(ServiceInstance.builder().instanceId("i101001").build());
        instances.add(ServiceInstance.builder().instanceId("i101002").build());
        instances.add(ServiceInstance.builder().instanceId("i101003").build());

        IShardingStrategy strategy = new AverageShardingStrategy();
        Map<ServiceInstance, List<Integer>> sharding = strategy.getShardingItem(30, instances);
        //System.out.println(sharding);

        Assertions.assertEquals(3, sharding.size());
        Assertions.assertEquals(10, sharding.get(instances.get(0)).size());
        Assertions.assertEquals(10, sharding.get(instances.get(1)).size());
        Assertions.assertEquals(10, sharding.get(instances.get(2)).size());
    }

}
