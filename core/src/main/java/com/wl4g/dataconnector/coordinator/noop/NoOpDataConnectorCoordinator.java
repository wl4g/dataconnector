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

import com.wl4g.dataconnector.config.configurator.IDataConnectorConfigurator;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import com.wl4g.dataconnector.coordinator.AbstractDataConnectorCoordinator;
import com.wl4g.dataconnector.coordinator.CachingChannelRegistry;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * The {@link NoOpDataConnectorCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class NoOpDataConnectorCoordinator extends AbstractDataConnectorCoordinator {
    private final NoOpCoordinatorConfig coordinatorConfig;
    private final NoOpBusPublisher busPublisher;

    protected NoOpDataConnectorCoordinator(@NotNull Environment environment,
                                           @NotNull DataConnectorConfiguration config,
                                           @NotNull IDataConnectorConfigurator configurator,
                                           @NotNull CachingChannelRegistry registry,
                                           @NotNull DataConnectorMeter meter,
                                           @NotNull NoOpCoordinatorConfig coordinatorConfig) {
        super(environment, config, configurator, registry, meter);
        this.coordinatorConfig = requireNonNull(coordinatorConfig, "coordinatorConfig must not be null");
        this.busPublisher = new NoOpBusPublisher();
    }

    @Override
    public void start() {
        if (log.isInfoEnabled()) {
            log.info("Initializing all channels synchronous ...");
        }
        for (ConnectorConfig connector : safeMap(getConfig().getConnectorMap()).values()) {
            final List<ChannelInfo> channels = getConfigurator().loadChannels(connector.getName(), null);
            if (log.isInfoEnabled()) {
                log.info("Loaded channels for connector: {} => {}", connector.getName(), channels);
            }
            final AddChannelEvent event = AddChannelEvent.builder()
                    .type(EventType.ADD)
                    .connectorName(connector.getName())
                    .channels(channels)
                    .build();
            super.doUpdateBusEvent(event);
        }
        if (log.isInfoEnabled()) {
            log.info("Initialized all channels synchronously.");
        }
    }

    @Override
    public void waitForReady() throws TimeoutException, InterruptedException {
        // Ignore
    }

    @Override
    protected void doRun() {
        throw new UnsupportedOperationException();
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class NoOpCoordinatorConfig extends CoordinatorConfig {
        @Override
        public void validate() {
            // Ignore, No configuration properties are required.
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class NoOpCoordinatorProvider extends CoordinatorProvider {
        public static final String TYPE_NAME = "NOOP_COORDINATOR";
        private volatile NoOpDataConnectorCoordinator SINGLETON;

        private @Builder.Default NoOpCoordinatorConfig coordinatorConfig = new NoOpCoordinatorConfig();

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public void validate() {
            requireNonNull(coordinatorConfig, "coordinatorConfig is null");
            coordinatorConfig.validate();
        }

        @Override
        public synchronized IDataConnectorCoordinator obtain(
                Environment environment,
                DataConnectorConfiguration config,
                IDataConnectorConfigurator configurator,
                CachingChannelRegistry registry,
                DataConnectorMeter meter) {
            return isNull(SINGLETON) ? (SINGLETON = new NoOpDataConnectorCoordinator(
                    environment, config, configurator, registry, meter, coordinatorConfig))
                    : SINGLETON;
        }
    }

}
