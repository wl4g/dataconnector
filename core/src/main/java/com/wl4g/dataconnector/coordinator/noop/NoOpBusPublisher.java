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

package com.wl4g.dataconnector.coordinator.noop;

import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.IBusPublisher;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;

/**
 * The {@link NoOpBusPublisher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class NoOpBusPublisher implements IBusPublisher {
    @Override
    public void publishSync(List<IDataConnectorCoordinator.BusEvent> events,
                            Duration timeout) throws InterruptedException, TimeoutException {
        log.warn("NoOp to bus publish sync for {}", events);
    }

    @Override
    public List<Future<?>> publishAsync(List<IDataConnectorCoordinator.BusEvent> events) {
        log.warn("NoOp to bus publish async for {}", events);
        return emptyList();
    }
}
