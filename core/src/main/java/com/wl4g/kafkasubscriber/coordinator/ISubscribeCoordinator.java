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

package com.wl4g.kafkasubscriber.coordinator;

import com.wl4g.kafkasubscriber.coordinator.SubscribeEventPublisher.SubscribeEvent;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.util.List;
import java.util.Properties;

/**
 * The {@link AbstractSubscribeCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface ISubscribeCoordinator extends ApplicationRunner, Runnable, AutoCloseable {

    void start();

    void stop();

    @Override
    default void run(ApplicationArguments args) {
        start();
    }

    @Override
    default void close() {
        stop();
    }

    void onDiscovery(List<ServiceInstance> instances);

    void onEvent(SubscribeEvent event);

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode(of = "instanceId")
    class ServiceInstance implements Comparable<ServiceInstance> {
        private String instanceId;
        private Boolean selfInstance;
        private String host;
        private Properties metadata;

        @Override
        public int compareTo(ServiceInstance other) {
            return getInstanceId().compareTo(other.getInstanceId());
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    class ShardingInfo {
        private int total;
        private List<Integer> items;
    }
}
